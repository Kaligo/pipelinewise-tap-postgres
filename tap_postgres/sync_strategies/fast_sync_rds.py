"""
Fast Sync RDS Strategy for tap-postgres

This module implements a fast and efficient data syncing strategy designed for
AWS RDS PostgreSQL as data source and Redshift as the target. It leverages
aws_s3.query_export_to_s3 to export data directly from RDS PostgreSQL to S3,
bypassing the Singer Specification for optimized performance.
"""

import copy
import datetime
import time
import uuid
from typing import Dict, List, Optional

import psycopg2
import psycopg2.extras
import singer

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

    def _build_sorted_column_expressions(
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
        metadata_column_names = self._get_metadata_column_names()
        all_column_names = [*metadata_column_names, *desired_columns]
        # Sort columns to ensure the output CSV headers match target's schema order.
        all_column_names.sort()

        # Get transformations for this stream if available
        transformations = {}
        if tap_stream_id:
            all_transformations = self.conn_config.get(
                "fast_sync_rds_transformations", {}
            )
            transformations = all_transformations.get(tap_stream_id, {})

        column_expressions = []
        for col, transformation in transformations.items():
            column_identifier = post_db.prepare_columns_sql(col)
            column_expressions.append(
                f"({transformation}) AS {column_identifier}"
            )

        for name in all_column_names:
            if name in transformations:
                continue

            if name in metadata_column_names:
                column_expressions.append(self._get_metadata_column_sql(name))
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
        columns = self._build_sorted_column_expressions(
            desired_columns, md_map, tap_stream_id
        )

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

    def _get_stream_version(self, state: Dict, stream: Dict) -> int:
        """
        Get or create stream version
        """
        stream_version = singer.get_bookmark(state, stream["tap_stream_id"], "version")
        if stream_version is None:
            stream_version = int(time.time() * 1000)
        return stream_version

    def _export_to_s3_and_get_info(  # pylint: disable=too-many-arguments,too-many-positional-arguments
        self, conn, query: str, s3_path: str, time_extracted, replication_method: str
    ) -> Dict:
        """
        Export data to S3 and return S3 info dictionary to be embedded in STATE message.

        Returns:
            Dictionary containing S3 export information
        """
        with metrics.record_counter(None) as counter:
            export_result = self._export_to_s3(conn, query, s3_path)
            LOGGER.debug("Running query: %s", query)

            LOGGER.info(
                "Exported %s rows to S3: s3://%s/%s",
                export_result["rows_uploaded"],
                self.s3_bucket,
                s3_path,
            )

            counter.increment(export_result["rows_uploaded"])

            # Create S3 info dictionary (without 'type' and 'stream' fields)
            # These will be embedded in STATE message bookmarks
            time_extracted_str = (
                time_extracted.isoformat()
                if isinstance(time_extracted, datetime.datetime)
                else str(time_extracted)
            )
            s3_info = {
                "s3_bucket": self.s3_bucket,
                "s3_path": s3_path,
                "s3_region": self.s3_region,
                "rows_uploaded": export_result["rows_uploaded"],
                "files_uploaded": export_result["files_uploaded"],
                "bytes_uploaded": export_result["bytes_uploaded"],
                "time_extracted": time_extracted_str,
                "replication_method": replication_method,
            }

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
                    conn, query, s3_path, time_extracted, replication_method
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
