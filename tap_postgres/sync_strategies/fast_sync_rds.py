"""
Fast Sync RDS Strategy for tap-postgres

This module implements a fast and efficient data syncing strategy designed for
AWS RDS PostgreSQL as data source and Redshift as the target. It leverages
aws_s3.query_export_to_s3 to export data directly from RDS PostgreSQL to S3,
bypassing the Singer Specification for optimized performance.
"""

import copy
import datetime
import os
import time
import uuid
import base64
import types
import boto3
import psycopg2
import psycopg2.extras
import singer

from typing import Dict, List, Optional
from singer import utils
from singer import metrics
from tap_postgres.db import LOGGER
from tap_postgres.parquet_writer import ParquetWriter

import tap_postgres.db as post_db
import tap_postgres.sync_strategies.common as sync_common

# It's important to use lowercase to match the default column order.
METADATA_COLUMNS = {
    "_sdc_batched_at": {
        "sql-datatype": "timestamp",
        "sql-value-for-export": "current_timestamp at time zone 'UTC'",
    },
    "_sdc_deleted_at": {
        "sql-datatype": "timestamp",
        "sql-value-for-export": "null",
    },
    "_sdc_extracted_at": {
        "sql-datatype": "timestamp",
        "sql-value-for-export": "current_timestamp at time zone 'UTC'",
    },
}

# Valid output formats for fast_sync_rds_output_format setting
VALID_OUTPUT_FORMATS = frozenset({"csv", "parquet"})


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
        # Store as private attribute and wrap in MappingProxyType for immutability
        self._conn_config = types.MappingProxyType(dict(conn_config))
        self.s3_bucket = s3_bucket
        self.s3_prefix = s3_prefix
        self.s3_region = s3_region

        # Validate output format setting
        self._validate_output_format()

    @property
    def conn_config(self) -> Dict:
        """
        Read-only access to connection configuration.

        Returns:
            Read-only view of the connection configuration dictionary
        """
        return self._conn_config

    def _validate_output_format(self) -> None:
        """
        Validate fast_sync_rds_output_format setting.

        Raises:
            ValueError: If output format is not one of the accepted values
        """
        output_format = self.conn_config.get("fast_sync_rds_output_format", "csv")

        if output_format not in VALID_OUTPUT_FORMATS:
            raise ValueError(
                f"Invalid value for 'fast_sync_rds_output_format': '{output_format}'. "
                f"Accepted values are: {', '.join(sorted(VALID_OUTPUT_FORMATS))}"
            )

    def _clean_name_for_s3(self, name: str) -> str:
        """
        Clean name for use in S3 path
        """
        return name.replace('"', "").replace("/", "_")

    def _generate_s3_path(self, schema_name: str, table_name: str) -> str:
        sync_id = str(uuid.uuid4())[:8]
        # Generate human-readable timestamp in YYYY-mm-DD-HHMMSS format (UTC)
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

    def _get_metadata_column_names(self) -> List[str]:
        if self.conn_config.get("fast_sync_rds_add_metadata_columns", True):
            return list(METADATA_COLUMNS.keys())
        return []

    def _get_metadata_column_sql(self, column_name: str) -> str:
        # Handle both lowercase and uppercase column names
        column_name_lower = column_name.lower()

        if column_name_lower in METADATA_COLUMNS:
            sql_default = METADATA_COLUMNS[column_name_lower]["sql-value-for-export"]
            return f"{sql_default} AS {column_name}"

        raise ValueError(f"Unknown metadata column: {column_name}")

    def _is_array_column(self, column_name: str, md_map: Dict) -> bool:
        """
        Check if a column is an array type based on metadata map.
        """
        column_metadata = md_map.get(("properties", column_name), {})
        sql_datatype = column_metadata.get("sql-datatype", "")
        # Array types in PostgreSQL have '[]' suffix in sql-datatype
        return sql_datatype.endswith("[]")

    def _convert_array_column_to_json(self, column_name: str) -> str:
        """
        Convert an array column to JSON format using PostgreSQL's array_to_json function.

        This converts PostgreSQL array format (e.g., {fashion}) to JSON format (e.g., ["fashion"]).
        The conversion happens before CSV export, so the exported CSV will contain JSON arrays
        instead of PostgreSQL array format.

        Args:
            column_name: Column name to convert

        Returns:
            SQL expression that converts array to JSON format
        """
        column_identifier = post_db.prepare_columns_sql(column_name)
        # array_to_json converts PostgreSQL arrays to JSON arrays
        # Returns NULL for NULL arrays (which is correct)
        return f"array_to_json({column_identifier}) AS {column_identifier}"

    def _build_sorted_columns(self, desired_columns: List[str]) -> List[str]:
        metadata_column_names = self._get_metadata_column_names()
        all_column_names = [*metadata_column_names, *desired_columns]
        # Sort columns to ensure the output CSV headers match target's schema order.
        return sorted(all_column_names)

    def _extend_metadata_map_with_metadata_columns(self, md_map: Dict) -> Dict:
        """
        Extend the original metadata dictionary with metadata columns info.
        """
        metadata_column_names = self._get_metadata_column_names()

        if not metadata_column_names:
            return md_map

        # use copy() to avoid modifying the original md_map.
        metadata = md_map.copy()

        for column_name in metadata_column_names:
            col_info = METADATA_COLUMNS[column_name]
            metadata[("properties", column_name)] = {
                "sql-datatype": col_info["sql-datatype"],
            }

        return metadata

    def _build_column_expressions(
        self,
        desired_columns: List[str],
        md_map: Dict,
        tap_stream_id: Optional[str] = None,
    ) -> List[str]:
        """
        Build SQL expressions for all columns (metadata + desired) in sorted order.

        Columns are sorted alphabetically to match target's schema order.
        This ensures the exported data column order matches the table column
        order exactly.

        Array columns are automatically converted from PostgreSQL array format
        (e.g., {fashion}) to JSON format (e.g., ["fashion"]) using array_to_json().

        If transformations are configured for the stream, they will be applied
        to the specified columns instead of the default column expressions.

        Args:
            desired_columns: List of desired column names from the source table
            md_map: Metadata map for column transformations
            tap_stream_id: Optional stream ID to look up transformations

        Returns:
            List of SQL expressions for columns in sorted order
        """
        # Get transformations for this stream if available
        transformations = {}
        if tap_stream_id:
            all_transformations = self.conn_config.get(
                "fast_sync_rds_transformations", {}
            )
            transformations = all_transformations.get(tap_stream_id, {})

        column_expressions = []
        for name in desired_columns:
            if name in METADATA_COLUMNS:
                column_expressions.append(self._get_metadata_column_sql(name))
            elif name in transformations:
                transformation_sql = transformations[name]
                column_identifier = post_db.prepare_columns_sql(name)
                column_expressions.append(
                    f"({transformation_sql}) AS {column_identifier}"
                )
            elif self._is_array_column(name, md_map):
                column_expressions.append(self._convert_array_column_to_json(name))
            else:
                column_expressions.append(
                    post_db.prepare_columns_for_select_sql(name, md_map=md_map)
                )

        return column_expressions

    def _build_select_query(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self,
        schema_name: str,
        table_name: str,
        desired_columns: List[str],
        md_map: Dict,
        replication_key: Optional[str] = None,
        replication_key_value: Optional[str] = None,
        replication_key_sql_datatype: Optional[str] = None,
        tap_stream_id: Optional[str] = None,
    ) -> str:
        columns = self._build_column_expressions(desired_columns, md_map, tap_stream_id)

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

    def _export_data_to_csv(self, conn, query: str, s3_path: str) -> Dict:
        """
        Export query results to CSV on S3 using aws_s3.query_export_to_s3

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

    def _get_stream_version(self, state: Dict, stream: Dict) -> int:
        """
        Get or create stream version
        """
        stream_version = singer.get_bookmark(state, stream["tap_stream_id"], "version")
        if stream_version is None:
            stream_version = int(time.time() * 1000)
        return stream_version

    def _iterate_csv_file_parts(self, csv_path: str, files_uploaded: int):
        """
        Iterate through CSV file parts for multi-part exports.

        Based on the spec of https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/postgresql-s3-export-functions.html
        When files_uploaded > 1, files are named with _part{num} suffix
        (e.g., path_part, path_part2).

        Args:
            csv_path: Base S3 path of the CSV file
            files_uploaded: Number of CSV files uploaded

        Yields:
            Tuple of (part_number, part_csv_path) for each file part
        """
        for part_num in range(1, files_uploaded + 1):
            part_csv_path = csv_path
            if files_uploaded > 1:
                part_csv_path = f"{csv_path}_part{part_num}"
            yield part_num, part_csv_path

    def _setup_s3_client(self):
        aws_profile = self.conn_config.get("aws_profile") or os.environ.get(
            "AWS_PROFILE"
        )
        aws_access_key_id = self.conn_config.get("aws_access_key_id") or os.environ.get(
            "AWS_ACCESS_KEY_ID"
        )
        aws_secret_access_key = self.conn_config.get(
            "aws_secret_access_key"
        ) or os.environ.get("AWS_SECRET_ACCESS_KEY")
        aws_session_token = self.conn_config.get("aws_session_token") or os.environ.get(
            "AWS_SESSION_TOKEN"
        )

        # Conditionally pass keys as this seems to affect whether instance
        # credentials are correctly loaded if the keys are None
        if aws_access_key_id and aws_secret_access_key:
            aws_session = boto3.session.Session(
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                aws_session_token=aws_session_token,
            )
        else:
            aws_session = boto3.session.Session(profile_name=aws_profile)

        return aws_session.client("s3", region_name=self.s3_region)

    def _delete_csv_files_from_s3(
        self, s3_client, csv_path: str, files_uploaded: int
    ) -> None:
        """
        Delete CSV files from S3 after successful Parquet conversion.

        Args:
            s3_client: boto3 S3 client instance
            csv_path: Base S3 path of the CSV file(s)
            files_uploaded: Number of CSV files to delete (for multi-part exports)
        """
        for _, part_csv_path in self._iterate_csv_file_parts(csv_path, files_uploaded):
            try:
                s3_client.delete_object(Bucket=self.s3_bucket, Key=part_csv_path)
                LOGGER.debug(
                    "Deleted CSV file from S3: s3://%s/%s",
                    self.s3_bucket,
                    part_csv_path,
                )
            except Exception as e:
                LOGGER.warning(
                    "Failed to delete CSV file s3://%s/%s: %s",
                    self.s3_bucket,
                    part_csv_path,
                    str(e),
                )
                # Continue deleting other files even if one fails
                continue

    def _cleanup_csv_files_after_parquet_conversion(
        self, csv_path: str, files_uploaded: int
    ) -> None:
        """
        Umbrella function to cleanup CSV files from S3 after Parquet conversion.

        Args:
            csv_path: Base S3 path of the CSV file(s)
            files_uploaded: Number of CSV files to delete
        """
        if not self.conn_config.get("fast_sync_rds_delete_intermediate_csv", False):
            LOGGER.debug(
                "CSV cleanup is disabled. Skipping deletion of CSV files from S3."
            )
            return

        try:
            s3_client = self._setup_s3_client()
            self._delete_csv_files_from_s3(s3_client, csv_path, files_uploaded)
        except Exception as e:
            LOGGER.error(
                "Failed to cleanup CSV files from S3: %s. CSV files may still exist at s3://%s/%s",
                str(e),
                self.s3_bucket,
                csv_path,
            )
            # Don't raise exception - cleanup failure shouldn't fail the sync

    def _build_parquet_path(self, csv_path: str) -> str:
        """
        Build Parquet file path based on CSV S3 path with Hive-style partitioning.

        Args:
            csv_path: S3 path to CSV file (e.g., "public-users_safe/2026-01-31-173212_e8ee5c13.csv")

        Returns:
            Hive-style partitioned Parquet path (e.g., "public-users_safe/_sdc_batched_at=2026-01-31/2026-01-31-173212_e8ee5c13.parquet")
        """
        # Get current date in YYYY-MM-DD format (UTC)
        current_date = time.strftime("%Y-%m-%d", time.gmtime())

        # Extract directory and filename from CSV path
        dir_path, filename = os.path.split(csv_path)

        # Remove .csv extension if present
        if filename.endswith(".csv"):
            filename = filename[:-4]

        # Build Hive-style partitioned path
        return f"{dir_path}/_sdc_batched_at={current_date}/{filename}.parquet"

    def _convert_csv_to_parquet(
        self,
        csv_path: str,
        files_uploaded: int,
        desired_columns: List[str],
        md_map: Dict,
    ) -> Dict:
        """
        Convert exported CSV on S3 to Parquet format.

        Args:
            csv_path: S3 path of the exported CSV
            files_uploaded: Number of CSV files uploaded (for multi-part exports)
            desired_columns: List of desired columns
            md_map: Metadata map

        Returns:
            Dictionary with keys: "file_format", "s3_path", and "pyarrow_schema"
        """
        proxy_options = self.conn_config.get("fast_sync_rds_proxy_options")
        if not proxy_options:
            # Need to set to None explicitly to avoid using empty dict
            proxy_options = None
        parquet_writer = ParquetWriter(
            self.s3_bucket, self.s3_region, proxy_options=proxy_options
        )
        arrow_schema = parquet_writer.build_pyarrow_schema(desired_columns, md_map)
        parquet_path = self._build_parquet_path(csv_path)

        for part_num, part_csv_path in self._iterate_csv_file_parts(
            csv_path, files_uploaded
        ):
            # Build corresponding parquet path for this part
            part_parquet_path = parquet_path
            if files_uploaded > 1:
                part_parquet_path = f"{parquet_path}_part{part_num}"

            LOGGER.info(
                "Converting %s to Parquet: %s", part_csv_path, part_parquet_path
            )

            parquet_writer.convert_csv_to_parquet(
                csv_path=part_csv_path,
                parquet_path=part_parquet_path,
                desired_columns=desired_columns,
                md_map=md_map,
                arrow_schema=arrow_schema,
            )

        # Serialize and encode the schema (all parts have the same schema)
        serialized_schema = arrow_schema.serialize()
        arrow_schema_b64 = base64.b64encode(serialized_schema).decode("utf-8")

        return {
            "file_format": "parquet",
            "s3_path": parquet_path,
            "pyarrow_schema": arrow_schema_b64,
        }

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
        output_format = self.conn_config.get("fast_sync_rds_output_format", "csv")
        s3_info = {
            "s3_bucket": self.s3_bucket,
            "s3_region": self.s3_region,
            "s3_path": s3_path,
            "replication_method": replication_method,
            "file_format": output_format,
        }

        with metrics.record_counter(None) as counter:
            export_result = self._export_data_to_csv(conn, query, s3_path)
            rows_uploaded = export_result["rows_uploaded"]
            LOGGER.debug("Running query: %s", query)

            LOGGER.info(
                "Exported %s rows to CSV on S3 at: s3://%s/%s",
                rows_uploaded,
                self.s3_bucket,
                s3_path,
            )

            counter.increment(rows_uploaded)

            if rows_uploaded > 0 and output_format == "parquet":
                parquet_info = self._convert_csv_to_parquet(
                    s3_path, export_result["files_uploaded"], desired_columns, md_map
                )
                s3_info.update(parquet_info)

                LOGGER.info(
                    "Converted exported CSV to Parquet at s3://%s/%s",
                    self.s3_bucket,
                    s3_info["s3_path"],
                )

                # Cleanup CSV files from S3 if configured
                self._cleanup_csv_files_after_parquet_conversion(
                    s3_path, export_result["files_uploaded"]
                )

            # Create S3 info dictionary (without 'type' and 'stream' fields)
            # These will be embedded in STATE message bookmarks
            time_extracted_str = (
                time_extracted.isoformat()
                if isinstance(time_extracted, datetime.datetime)
                else str(time_extracted)
            )
            s3_info.update(
                {
                    "rows_uploaded": rows_uploaded,
                    "files_uploaded": export_result["files_uploaded"],
                    "bytes_uploaded": export_result["bytes_uploaded"],
                    "time_extracted": time_extracted_str,
                }
            )

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

        desired_columns = self._build_sorted_columns(desired_columns)
        md_map = self._extend_metadata_map_with_metadata_columns(md_map)

        query = self._build_select_query(
            schema_name=schema_name,
            table_name=table_name,
            desired_columns=desired_columns,
            md_map=md_map,
            replication_key=replication_key,
            replication_key_value=replication_key_value,
            replication_key_sql_datatype=replication_key_sql_datatype,
            tap_stream_id=stream["tap_stream_id"],
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
