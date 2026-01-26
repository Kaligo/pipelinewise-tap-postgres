"""
Fast Sync RDS Strategy for tap-postgres

This module implements a fast and efficient data syncing strategy designed for
AWS RDS PostgreSQL as data source and Redshift as the target. It leverages
aws_s3.query_export_to_s3 to export data directly from RDS PostgreSQL to S3,
bypassing the Singer Specification for optimized performance.
"""

import base64
import copy
import datetime
import time
import uuid
from functools import partial
from typing import Dict, List, Optional, Tuple
import re

import psycopg2
import psycopg2.extras
import singer
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pyarrow.csv as pacsv
import connectorx as cx

from singer import utils
from singer import metrics

import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common

LOGGER = singer.get_logger("tap_postgres")

# It's important to use lowercase to match the default column order.
METADATA_COLUMNS = {"_sdc_batched_at", "_sdc_deleted_at", "_sdc_extracted_at"}


class FastSyncRdsStrategy:
    """
    Fast RDS sync strategy that exports data directly from RDS PostgreSQL to S3
    using aws_s3.query_export_to_s3 function.
    """

    def __init__(
        self, conn_config: Dict, s3_bucket: str, s3_prefix: str, s3_region: str
    ):
        """
        Initialize FastSyncRdsStrategy

        Args:
            conn_config: Database connection configuration
            s3_bucket: S3 bucket name for exports
            s3_prefix: S3 prefix/path for exports
            s3_region: AWS region where the S3 bucket is located
        """
        self.conn_config = conn_config
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_region = s3_region

    def _clean_name_for_s3(self, name: str) -> str:
        """
        Clean name for use in S3 path
        """
        return name.replace('"', "").replace("/", "_")

    def _generate_s3_path(self, schema_name: str, table_name: str) -> str:
        sync_id = str(uuid.uuid4())[:8]
        # Generate human-readable timestamp in YYYY-mm-DD-HHMMSS format
        timestamp = time.strftime("%Y-%m-%d-%H%M%S", time.gmtime())
        # Clean schema and table names to ensure they're safe for S3 paths
        # (removes quotes and replaces '/' with '_')
        clean_schema = self._clean_name_for_s3(schema_name)
        clean_table = self._clean_name_for_s3(table_name)

        path = (
            f"{self.s3_prefix}/{clean_schema}-{clean_table}/{timestamp}_{sync_id}.csv"
        )
        # Remove leading slash if prefix is empty to avoid double slashes
        return path.lstrip("/")

    def _postgres_type_to_pyarrow(self, postgres_type: str) -> pa.DataType:
        """
        Convert PostgreSQL type string to a PyArrow DataType.
        Keeps mapping intentionally small and defaults to string for unknowns.
        """
        postgres_type_lower = postgres_type.lower()

        if "timestamp" in postgres_type_lower:
            return pa.timestamp("us")

        if "character varying" in postgres_type_lower or "varchar" in postgres_type_lower:
            return pa.string()
        if "char" in postgres_type_lower and "varying" not in postgres_type_lower:
            return pa.string()
        if "text" in postgres_type_lower:
            return pa.string()

        if "integer" in postgres_type_lower or "int" in postgres_type_lower:
            if "bigint" in postgres_type_lower or "int8" in postgres_type_lower:
                return pa.int64()
            if "smallint" in postgres_type_lower or "int2" in postgres_type_lower:
                return pa.int16()
            return pa.int32()

        if "numeric" in postgres_type_lower or "decimal" in postgres_type_lower:
            match = re.search(r"\((\d+)(?:,\s*(\d+))?\)", postgres_type_lower)
            if match:
                precision = int(match.group(1))
                scale = int(match.group(2)) if match.group(2) else 0
                return pa.decimal128(precision, scale)
            return pa.decimal128(38, 0)

        if "real" in postgres_type_lower or "float4" in postgres_type_lower:
            return pa.float32()
        if "double precision" in postgres_type_lower or "float8" in postgres_type_lower:
            return pa.float64()

        if "boolean" in postgres_type_lower or "bool" in postgres_type_lower:
            return pa.bool_()

        if "date" in postgres_type_lower:
            # Use timestamp for compatibility with Spectrum expectations on date columns
            return pa.timestamp("us")

        if "json" in postgres_type_lower:
            return pa.string()

        if "uuid" in postgres_type_lower:
            return pa.string()

        # Default fallback
        return pa.string()

    def _get_export_column_names(self, desired_columns: List[str]) -> List[str]:
        """
        Return the ordered column names exactly as exported to CSV.
        """
        column_names: List[str] = []
        if self.conn_config.get("fast_sync_rds_add_metadata_columns", True):
            column_names.extend(
                ["_SDC_BATCHED_AT", "_SDC_DELETED_AT", "_SDC_EXTRACTED_AT"]
            )
        column_names.extend(desired_columns)
        return column_names

    def _build_pyarrow_schema(self, desired_columns: List[str], md_map: Dict) -> pa.Schema:
        """
        Build a PyArrow schema matching the exported CSV order.
        """
        fields: List[pa.Field] = []
        include_metadata = self.conn_config.get("fast_sync_rds_add_metadata_columns", True)
        if include_metadata:
            fields.append(pa.field("_SDC_BATCHED_AT", pa.timestamp("us"), nullable=True))
            fields.append(pa.field("_SDC_DELETED_AT", pa.timestamp("us"), nullable=True))
            fields.append(pa.field("_SDC_EXTRACTED_AT", pa.timestamp("us"), nullable=True))

        for col_name in desired_columns:
            sql_datatype = (
                md_map.get(("properties", col_name), {}).get("sql-datatype", "text")
            )
            pa_type = self._postgres_type_to_pyarrow(sql_datatype)
            fields.append(pa.field(col_name, pa_type, nullable=True))

        return pa.schema(fields)

    def _get_metadata_column_names(self) -> List[str]:
        if self.conn_config.get("fast_sync_rds_add_metadata_columns", True):
            return METADATA_COLUMNS
        return []

    def _get_metadata_column_sql(self, column_name: str) -> str:
        """Get SQL expression for a metadata column."""
        # Handle both lowercase and uppercase column names
        column_name_lower = column_name.lower()

        metadata_sql_map = {
            "_sdc_batched_at": "current_timestamp at time zone 'UTC' as _sdc_batched_at",
            "_sdc_deleted_at": "null as _sdc_deleted_at",
            "_sdc_extracted_at": "current_timestamp at time zone 'UTC' as _sdc_extracted_at",
        }

        if column_name_lower in metadata_sql_map:
            return metadata_sql_map[column_name_lower]

        raise ValueError(f"Unknown metadata column: {column_name}")

    def _build_sorted_column_expressions(
        self, desired_columns: List[str], md_map: Dict
    ) -> List[str]:
        """
        Build SQL expressions for all columns (metadata + desired) in sorted order.

        Columns are sorted alphabetically to match target's schema order.
        This ensures the exported data column order matches the table column
        order exactly.

        Args:
            desired_columns: List of desired column names from the source table
            md_map: Metadata map for column transformations

        Returns:
            List of SQL expressions for columns in sorted order
        """
        metadata_column_names = self._get_metadata_column_names()
        all_column_names = [*metadata_column_names, *desired_columns]
        # Sort columns to ensure the output CSV headers match target's schema order.
        all_column_names.sort()

        return [
            self._get_metadata_column_sql(name) if name in metadata_column_names
            else post_db.prepare_columns_for_select_sql(name, md_map=md_map)
            for name in all_column_names
        ]

    def _build_select_query(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        schema_name: str,
        table_name: str,
        desired_columns: List[str],
        md_map: Dict,
        replication_key: Optional[str] = None,
        replication_key_value: Optional[str] = None,
        replication_key_sql_datatype: Optional[str] = None,
    ) -> str:
        columns = self._build_sorted_column_expressions(desired_columns, md_map)

        return sync_common.get_query_for_replication_data(
            {
                "escaped_columns": columns,
                "replication_key": replication_key,
                "replication_key_sql_datatype": replication_key_sql_datatype,
                "replication_key_value": replication_key_value,
                "schema_name": schema_name,
                "table_name": table_name,
                "skip_last_n_seconds": self.conn_config.get(
                    "skip_last_n_seconds", None
                ),
                "look_back_n_seconds": self.conn_config.get(
                    "look_back_n_seconds", None
                ),
                "recover_mappings": self.conn_config.get("recover_mappings", {}),
                "skip_order": True,
            }
        )

    def _build_export_query(self, query: str, s3_path: str) -> str:
        """
        Build the export query for aws_s3.query_export_to_s3

        Note: aws_s3.query_export_to_s3 automatically splits large exports into
        multiple files (~6GB each) with naming pattern: path, path_part2, path_part3, etc.
        CSV files are exported without headers.
        """
        # Escape all values to prevent SQL injection and syntax errors.
        escaped_query = sync_common.escape_sql_string(query)
        escaped_bucket = sync_common.escape_sql_string(self.s3_bucket)
        escaped_path = sync_common.escape_sql_string(
            s3_path
        )  # Required: paths can contain quotes
        escaped_region = sync_common.escape_sql_string(self.s3_region)

        return f"""
        SELECT * FROM aws_s3.query_export_to_s3(
            '{escaped_query}',
            aws_commons.create_s3_uri(
                '{escaped_bucket}',
                '{escaped_path}',
                '{escaped_region}'
            ),
            'format csv'
        )
        """

    def _export_to_s3(self, conn, query: str, s3_path: str) -> Dict:
        """
        Export query results to S3 using aws_s3.query_export_to_s3

        Args:
            conn: Database connection
            query: SQL query to execute
            s3_path: S3 path where data will be exported

        Returns:
            Dictionary with export results (rows_uploaded, files_uploaded, bytes_uploaded)
        """
        export_query = self._build_export_query(query, s3_path)
        LOGGER.info("Exporting data to S3: s3://%s/%s", self.s3_bucket, s3_path)

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(export_query)
            result = cur.fetchone()

            if not result:
                raise RuntimeError("Export to S3 failed: No result returned")

            return {
                "rows_uploaded": result["rows_uploaded"],
                "files_uploaded": result["files_uploaded"],
                "bytes_uploaded": result["bytes_uploaded"],
            }

    def _export_query_to_parquet(
        self, conn, query: str, s3_path: str, desired_columns: List[str], md_map: Dict
    ) -> Dict:
        """
        Stream query results directly to Parquet on S3 using PyArrow.
        """
        parquet_path = (
            s3_path.rsplit(".", 1)[0] + ".parquet" if "." in s3_path else f"{s3_path}.parquet"
        )
        arrow_schema = self._build_pyarrow_schema(desired_columns, md_map)
        column_names = self._get_export_column_names(desired_columns)

        proxy_options = {
            "scheme": "http",  # proxy is likely plain HTTP even if upstream is HTTPS
            "host": "proxy.int.kaligo.com",
            "port": 3128,
        }
        filesystem = pafs.S3FileSystem(region=self.s3_region, proxy_options=proxy_options)

        writer = None
        batch_size = 50000
        rows_written = 0
        try:
            with conn.cursor(name="tap_pg_fast_sync_parquet") as cur:
                cur.itersize = batch_size
                cur.execute(query)
                while True:
                    rows = cur.fetchmany(batch_size)
                    if not rows:
                        break
                    arrays = []
                    for idx, field in enumerate(arrow_schema):
                        col_values = [row[idx] for row in rows]
                        # Coerce date objects into datetime for timestamp fields to satisfy Arrow
                        if pa.types.is_timestamp(field.type):
                            col_values = [
                                datetime.datetime.combine(v, datetime.time())
                                if isinstance(v, datetime.date) and not isinstance(v, datetime.datetime)
                                else v
                                for v in col_values
                            ]
                        arrays.append(pa.array(col_values, type=field.type))
                    batch = pa.RecordBatch.from_arrays(arrays, names=column_names)
                    if writer is None:
                        writer = pq.ParquetWriter(
                            f"{self.s3_bucket}/{parquet_path}",
                            arrow_schema,
                            filesystem=filesystem,
                            compression="zstd",
                        )
                    writer.write_batch(batch)
                    rows_written += len(rows)
            if writer is None:
                # Write empty file with schema to ensure target file exists
                empty_table = pa.Table.from_pydict(
                    {field.name: pa.array([], type=field.type) for field in arrow_schema},
                    schema=arrow_schema,
                )
                pq.write_table(
                    empty_table,
                    f"{self.s3_bucket}/{parquet_path}",
                    filesystem=filesystem,
                    compression="zstd",
                )
        finally:
            if writer is not None:
                writer.close()

        file_info = filesystem.get_file_info(f"{self.s3_bucket}/{parquet_path}")
        bytes_uploaded = file_info.size if file_info.size is not None else 0

        LOGGER.info(
            "Experted query to Parquet via PyArrow at s3://%s/%s",
            self.s3_bucket,
            parquet_path,
        )

        return {
            "rows_uploaded": rows_written,
            "files_uploaded": 1,
            "bytes_uploaded": bytes_uploaded,
            "s3_path": parquet_path,
            "pyarrow_schema": arrow_schema,
        }

    def _build_postgres_conn_string(self) -> str:
        """
        Build PostgreSQL connection string for connectorx from conn_config.
        """
        host = self.conn_config.get("secondary_host") if self.conn_config.get("use_secondary") else self.conn_config["host"]
        port = self.conn_config.get("secondary_port") if self.conn_config.get("use_secondary") else self.conn_config["port"]
        user = self.conn_config["user"]
        password = self.conn_config["password"]
        dbname = self.conn_config["dbname"]

        # Build connection string
        conn_str = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"

        # Add SSL mode if specified
        if self.conn_config.get("sslmode"):
            conn_str += f"?sslmode={self.conn_config['sslmode']}"

        return conn_str

    def _export_query_to_parquet_with_connectorx(
        self, query: str, s3_path: str
    ) -> Dict:
        """
        Read data from PostgreSQL using connectorx and write directly to Parquet on S3.
        Uses arrow_stream to process data in batches without loading everything into memory.
        Connectorx handles type conversion automatically, so we don't need explicit schema.
        """
        parquet_path = (
            s3_path.rsplit(".", 1)[0] + ".parquet" if "." in s3_path else f"{s3_path}.parquet"
        )

        # Build PostgreSQL connection string
        conn_str = self._build_postgres_conn_string()

        # Configure S3 filesystem with proxy using PyArrow
        proxy_options = {
            "scheme": "http",
            "host": "proxy.int.kaligo.com",
            "port": 3128,
        }
        filesystem = pafs.S3FileSystem(region=self.s3_region, proxy_options=proxy_options)

        # Stream data from PostgreSQL using connectorx arrow_stream
        # This processes data in batches without loading everything into memory
        batch_size = 500000  # Process 500k rows per batch
        LOGGER.info("Reading data from PostgreSQL using connectorx (streaming mode)")

        arrow_stream = cx.read_sql(
            conn_str,
            query,
            return_type="arrow_stream",
            batch_size=batch_size
        )

        # Write to S3 as Parquet in streaming fashion
        s3_parquet_path = f"{self.s3_bucket}/{parquet_path}"
        LOGGER.info("Writing Parquet file to s3://%s", s3_parquet_path)

        writer = None
        rows_uploaded = 0
        batch_count = 0
        schema = None

        try:
            for batch in arrow_stream:
                if writer is None:
                    # Initialize writer with schema from first batch
                    # Handle both RecordBatch and Table from connectorx
                    if isinstance(batch, pa.RecordBatch):
                        schema = batch.schema
                    elif isinstance(batch, pa.Table):
                        schema = batch.schema
                    else:
                        raise ValueError(f"Unexpected batch type: {type(batch)}")

                    writer = pq.ParquetWriter(
                        s3_parquet_path,
                        schema,
                        filesystem=filesystem,
                        compression="zstd",
                    )
                    LOGGER.debug("Initialized Parquet writer with schema from first batch")

                # Write batch to Parquet (handle both RecordBatch and Table)
                if isinstance(batch, pa.RecordBatch):
                    writer.write_batch(batch)
                    rows_uploaded += len(batch)
                elif isinstance(batch, pa.Table):
                    # Convert Table to batches if needed, or write directly
                    writer.write_table(batch)
                    rows_uploaded += len(batch)
                else:
                    raise ValueError(f"Unexpected batch type: {type(batch)}")

                batch_count += 1

                # Log progress every 10 batches (every ~1M rows)
                if batch_count % 10 == 0:
                    LOGGER.info("Processed %s batches, %s rows written to Parquet", batch_count, rows_uploaded)

            if writer is None:
                # No data was returned
                LOGGER.warning("No data returned from query")
                # Create an empty Parquet file with a minimal schema
                empty_schema = pa.schema([])
                writer = pq.ParquetWriter(
                    s3_parquet_path,
                    empty_schema,
                    filesystem=filesystem,
                    compression="zstd",
                )
                writer.close()
                rows_uploaded = 0
                schema = empty_schema
            else:
                writer.close()
                # Schema was captured from first batch

        except Exception as e:
            if writer is not None:
                writer.close()
            raise

        LOGGER.info("Read and wrote %s rows from PostgreSQL to Parquet", rows_uploaded)

        # Get file size
        file_info = filesystem.get_file_info(s3_parquet_path)
        bytes_uploaded = file_info.size if file_info.size is not None else 0

        LOGGER.info(
            "Exported query to Parquet via connectorx at s3://%s/%s",
            self.s3_bucket,
            parquet_path,
        )

        return {
            "rows_uploaded": rows_uploaded,
            "files_uploaded": 1,
            "bytes_uploaded": bytes_uploaded,
            "s3_path": parquet_path,
            "pyarrow_schema": schema,
        }

    def _export_query_to_parquet_with_connectorx_no_batch(
        self, query: str, s3_path: str
    ) -> Dict:
        """
        Read data from PostgreSQL using connectorx and write directly to Parquet on S3.
        Uses return_type="arrow" to load entire result set into memory at once.
        Connectorx handles type conversion automatically, so we don't need explicit schema.

        NOTE: This method loads the entire result set into memory. For very large datasets
        (e.g., >100M rows), consider using _export_query_to_parquet_with_connectorx() which
        streams data in batches instead.
        """
        parquet_path = (
            s3_path.rsplit(".", 1)[0] + ".parquet" if "." in s3_path else f"{s3_path}.parquet"
        )

        # Build PostgreSQL connection string
        conn_str = self._build_postgres_conn_string()

        # Configure S3 filesystem with proxy using PyArrow
        proxy_options = {
            "scheme": "http",
            "host": "proxy.int.kaligo.com",
            "port": 3128,
        }
        filesystem = pafs.S3FileSystem(region=self.s3_region, proxy_options=proxy_options)

        # Read entire result set into memory using connectorx
        # WARNING: This loads the entire result set into memory as an Arrow table.
        LOGGER.info("Reading data from PostgreSQL using connectorx (loading entire result set into memory)")
        table: pa.Table = cx.read_sql(
            conn_str,
            query,
            return_type="arrow"
        )

        rows_uploaded = len(table)
        LOGGER.info("Read %s rows from PostgreSQL", rows_uploaded)

        # Write to S3 as Parquet
        s3_parquet_path = f"{self.s3_bucket}/{parquet_path}"
        LOGGER.info("Writing Parquet file to s3://%s", s3_parquet_path)

        pq.write_table(
            table,
            s3_parquet_path,
            filesystem=filesystem,
            compression="zstd"
        )

        # Get file size
        file_info = filesystem.get_file_info(s3_parquet_path)
        bytes_uploaded = file_info.size if file_info.size is not None else 0

        LOGGER.info(
            "Exported query to Parquet via connectorx (no batch) at s3://%s/%s",
            self.s3_bucket,
            parquet_path,
        )

        return {
            "rows_uploaded": rows_uploaded,
            "files_uploaded": 1,
            "bytes_uploaded": bytes_uploaded,
            "s3_path": parquet_path,
            "pyarrow_schema": table.schema,
        }

    def _convert_csv_to_parquet(
        self, s3_path: str, desired_columns: List[str], md_map: Dict
    ) -> Tuple[str, pa.Schema]:
        """
        Convert an exported CSV on S3 to Parquet using an explicit schema.
        """
        parquet_path = (
            s3_path.rsplit(".", 1)[0] + ".parquet" if "." in s3_path else f"{s3_path}.parquet"
        )
        proxy_options = {
            "scheme": "http",
            "host": "proxy.int.kaligo.com",
            "port": 3128,
        }
        filesystem = pafs.S3FileSystem(region=self.s3_region, proxy_options=proxy_options)
        arrow_schema = self._build_pyarrow_schema(desired_columns, md_map)
        csv_format = ds.CsvFileFormat(
            read_options=pacsv.ReadOptions(
                column_names=self._get_export_column_names(desired_columns)
            )
        )
        dataset = ds.dataset(
            f"{self.s3_bucket}/{s3_path}",
            format=csv_format,
            schema=arrow_schema,
            filesystem=filesystem,
        )

        # Process in batches for better memory efficiency with large datasets
        batch_size = 500000  # Process 500k rows per batch
        rows_processed = 0

        with pq.ParquetWriter(
            f"{self.s3_bucket}/{parquet_path}",
            arrow_schema,
            filesystem=filesystem,
            compression="zstd",
        ) as writer:
            for batch in dataset.to_batches(batch_size=batch_size):
                writer.write_batch(batch)
                rows_processed += len(batch)
                # Log progress every 10 batches (every ~5M rows)
                if rows_processed % (batch_size * 10) == 0:
                    LOGGER.debug("Converted %s rows from CSV to Parquet", rows_processed)

        LOGGER.info(
            "Converted CSV export to Parquet at s3://%s/%s",
            self.s3_bucket,
            parquet_path,
        )

        return parquet_path, arrow_schema

    def _get_stream_version(self, state: Dict, stream: Dict) -> int:
        """
        Get or create stream version
        """
        stream_version = singer.get_bookmark(state, stream["tap_stream_id"], "version")
        if stream_version is None:
            stream_version = int(time.time() * 1000)
        return stream_version

    def _export_to_s3_and_get_info(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        conn,
        query: str,
        s3_path: str,
        time_extracted,
        replication_method: str,
        desired_columns: List[str],
        md_map: Dict,
    ) -> Dict:
        """
        Export data to S3 and return S3 info dictionary to be embedded in STATE message.

        Returns:
            Dictionary containing S3 export information
        """
        with metrics.record_counter(None) as counter:
            if self.conn_config.get("direct_query_to_parquet"):
                # Choose between connectorx (no batch), connectorx (streaming), or streaming PyArrow method
                if self.conn_config.get("use_connectorx_no_batch", False):
                    export_result = self._export_query_to_parquet_with_connectorx_no_batch(
                        query, s3_path
                    )
                    LOGGER.info(
                        "Directly exported %s rows to Parquet using connectorx (no batch) at s3://%s/%s",
                        export_result["rows_uploaded"],
                        self.s3_bucket,
                        export_result.get("s3_path", s3_path),
                    )
                elif self.conn_config.get("use_connectorx", False):
                    export_result = self._export_query_to_parquet_with_connectorx(
                        query, s3_path
                    )
                    LOGGER.info(
                        "Directly exported %s rows to Parquet using connectorx (streaming) at s3://%s/%s",
                        export_result["rows_uploaded"],
                        self.s3_bucket,
                        export_result.get("s3_path", s3_path),
                    )
                else:
                    export_result = self._export_query_to_parquet(
                        conn, query, s3_path, desired_columns, md_map
                    )
                    LOGGER.info(
                        "Directly exported %s rows to Parquet using streaming PyArrow at s3://%s/%s",
                        export_result["rows_uploaded"],
                        self.s3_bucket,
                        export_result.get("s3_path", s3_path),
                    )
                s3_path = export_result.get("s3_path", s3_path)
                format_type = "parquet"
                counter.increment(export_result["rows_uploaded"])
            else:
                export_result = self._export_to_s3(conn, query, s3_path)
                LOGGER.debug("Running query: %s", query)

                LOGGER.info(
                    "Exported %s rows to S3: s3://%s/%s",
                    export_result["rows_uploaded"],
                    self.s3_bucket,
                    s3_path,
                )

                counter.increment(export_result["rows_uploaded"])

                format_type = "csv"
                pyarrow_schema = None
                if self.conn_config.get("convert_to_parquet"):
                    try:
                        s3_path, pyarrow_schema = self._convert_csv_to_parquet(s3_path, desired_columns, md_map)
                        format_type = "parquet"
                    except Exception as exc:  # pylint: disable=broad-except
                        LOGGER.error(
                            "convert_to_parquet enabled but conversion failed; aborting. Error: %s",
                            exc,
                        )
                        raise

            # Create S3 info dictionary (without 'type' and 'stream' fields)
            # These will be embedded in STATE message bookmarks
            time_extracted_str = (
                time_extracted.isoformat()
                if isinstance(time_extracted, datetime.datetime)
                else str(time_extracted)
            )

            # Extract PyArrow schema from export_result if available (for parquet files)
            if format_type == "parquet":
                pyarrow_schema = export_result.get("pyarrow_schema") or pyarrow_schema
            else:
                pyarrow_schema = None

            s3_info = {
                "s3_bucket": self.s3_bucket,
                "s3_path": s3_path,
                "s3_region": self.s3_region,
                "rows_uploaded": export_result["rows_uploaded"],
                "files_uploaded": export_result["files_uploaded"],
                "bytes_uploaded": export_result["bytes_uploaded"],
                "time_extracted": time_extracted_str,
                "replication_method": replication_method,
                "file_format": format_type,
            }

            # Add PyArrow schema as JSON if available (for parquet files)
            if pyarrow_schema is not None:
                # Serialize PyArrow schema to JSON format
                serialized_schema = pyarrow_schema.serialize()
                s3_info["pyarrow_schema"] = base64.b64encode(serialized_schema).decode('utf-8')
                LOGGER.debug("Included PyArrow schema in s3_info for parquet file")

            return s3_info

    def _get_latest_replication_key_value(  # pylint: disable=invalid-name
        self, conn, schema_name: str, table_name: str, replication_key: str
    ) -> Optional[str]:
        """Get the latest replication key value from the database"""
        column_name = post_db.prepare_columns_sql(replication_key)
        select_sql = sync_common.get_select_latest_sql(
            {
                "escaped_columns": [column_name],
                "replication_key": replication_key,
                "schema_name": schema_name,
                "table_name": table_name,
            }
        )

        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(select_sql)
            result = cur.fetchone()

            if result:
                return result[replication_key]
        return None

    def _track_latest_replication_key_value(  # pylint: disable=invalid-name,too-many-arguments,too-many-positional-arguments
        self,
        state,
        stream,
        conn,
        schema_name: str,
        table_name: str,
        replication_key: str,
    ):
        """Track the latest replication key value in state"""
        if not replication_key:
            return state

        last_replication_key_value = self._get_latest_replication_key_value(
            conn, schema_name, table_name, replication_key
        )

        if last_replication_key_value:
            LOGGER.info("last_replication_key_value: %s", last_replication_key_value)

            # Always track the replication_key_value for logging,
            # and for cases we want to switch among replication methods.
            if isinstance(last_replication_key_value, datetime.datetime):
                replication_key_value_str = last_replication_key_value.isoformat()
            else:
                replication_key_value_str = str(last_replication_key_value)

            state = singer.write_bookmark(
                state,
                stream["tap_stream_id"],
                "replication_key_value",
                replication_key_value_str,
            )

        return state

    def sync_table(  # pylint: disable=too-many-arguments,too-many-positional-arguments,too-many-locals
        self,
        stream: Dict,
        state: Dict,
        desired_columns: List[str],
        md_map: Dict,
        replication_method: str,
        replication_key: Optional[str] = None,
        replication_key_value: Optional[str] = None,
    ) -> Dict:
        """
        Sync table using fast sync RDS strategy

        Args:
            stream: Stream dictionary
            state: State dictionary
            desired_columns: List of columns to sync
            md_map: Metadata map
            replication_method: Replication method ("FULL_TABLE" or "INCREMENTAL")
            replication_key: Optional replication key for incremental sync
            replication_key_value: Optional replication key value

        Returns:
            Updated state dictionary
        """
        time_extracted = utils.now()
        stream_version = self._get_stream_version(state, stream)

        state = singer.write_bookmark(
            state, stream["tap_stream_id"], "version", stream_version
        )

        schema_name = md_map.get(()).get("schema-name")
        table_name = stream["table_name"]

        replication_key_sql_datatype = None
        if replication_key:
            replication_key_sql_datatype = md_map.get(
                ("properties", replication_key)
            ).get("sql-datatype")

        LOGGER.info(
            "Singer bookmark data: %s",
            {
                "replication_key": replication_key,
                "replication_key_value": replication_key_value,
            },
        )

        query = self._build_select_query(
            schema_name=schema_name,
            table_name=table_name,
            desired_columns=desired_columns,
            md_map=md_map,
            replication_key=replication_key,
            replication_key_value=replication_key_value,
            replication_key_sql_datatype=replication_key_sql_datatype,
        )

        s3_path = self._generate_s3_path(schema_name, table_name)

        try:
            with post_db.open_connection(self.conn_config) as conn:
                # Track the latest replication key value before running the query
                # to avoid missing records on the next incremental run, since
                # the export query might take a long time to complete.
                state = self._track_latest_replication_key_value(
                    state, stream, conn, schema_name, table_name, replication_key
                )

                s3_info = self._export_to_s3_and_get_info(
                    conn,
                    query,
                    s3_path,
                    time_extracted,
                    replication_method,
                    desired_columns,
                    md_map,
                )

                LOGGER.debug("Final s3_info: %s", s3_info)

                if s3_info["rows_uploaded"] > 0:
                    # Store S3 info in state bookmarks for target to read from STATE message
                    state = singer.write_bookmark(
                        state, stream["tap_stream_id"], "fast_sync_s3_info", s3_info
                    )

        except Exception as exc:
            LOGGER.error("Failed to export to S3: %s", str(exc))
            raise

        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))
        activate_version_message = singer.ActivateVersionMessage(
            stream=post_db.calculate_destination_stream_name(stream, md_map),
            version=stream_version,
        )
        singer.write_message(activate_version_message)

        return state

    def sync_table_full(
        self, stream: Dict, state: Dict, desired_columns: List[str], md_map: Dict
    ) -> Dict:
        """Sync table using full table replication method"""
        return self.sync_table(stream, state, desired_columns, md_map, "FULL_TABLE")

    def sync_table_incremental(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        stream: Dict,
        state: Dict,
        desired_columns: List[str],
        md_map: Dict,
        replication_key: str,
        replication_key_value: str,
    ) -> Dict:
        """Sync table using incremental replication method"""
        return self.sync_table(
            stream,
            state,
            desired_columns,
            md_map,
            "INCREMENTAL",
            replication_key,
            replication_key_value,
        )
