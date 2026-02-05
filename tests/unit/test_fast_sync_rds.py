"""
Unit tests for fast_sync_rds strategy
"""

import re
import time
from unittest import TestCase
from unittest.mock import patch, MagicMock

import pyarrow as pa

from tap_postgres.sync_strategies import fast_sync_rds
import tap_postgres


class TestFastSyncRds(TestCase):
    """Test Cases for FastSyncRdsStrategy"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.conn_config = {
            "host": "test-host",
            "dbname": "test_db",
            "user": "test_user",
            "password": "test_pass",
            "port": 5432,
        }
        self.s3_bucket = "test-bucket"
        self.s3_prefix = "test/prefix"
        self.s3_region = "us-east-1"
        self.fast_sync_rds_strategy = fast_sync_rds.FastSyncRdsStrategy(
            self.conn_config, self.s3_bucket, self.s3_prefix, self.s3_region
        )

        self.stream = {
            "tap_stream_id": "test_schema-test_table",
            "stream": "test_table",
            "table_name": "test_table",
        }
        self.md_map = {
            (): {"schema-name": "test_schema"},
            ("properties", "id"): {"sql-datatype": "integer"},
            ("properties", "name"): {"sql-datatype": "varchar"},
        }
        self.state = {
            "bookmarks": {self.stream["tap_stream_id"]: {"version": 1234567890}}
        }
        self.desired_columns = ["id", "name"]

    def _create_mock_export_result(self, rows=50, files=1, bytes_uploaded=2500):
        """Helper to create mock export result"""
        mock_result = MagicMock()
        mock_result.__getitem__.side_effect = lambda key: {
            "rows_uploaded": rows,
            "files_uploaded": files,
            "bytes_uploaded": bytes_uploaded,
        }[key]
        return mock_result

    def _setup_mock_connection(self, export_result=None, replication_key_result=None):
        """Helper to set up mock database connection"""
        if export_result is None:
            export_result = self._create_mock_export_result()

        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor

        if replication_key_result:
            mock_cursor.fetchone.side_effect = [replication_key_result, export_result]
        else:
            mock_cursor.fetchone.return_value = export_result

        return mock_conn, mock_cursor

    def _extract_export_query(self, mock_cursor):
        """Helper to extract export query from executed queries"""
        execute_calls = [
            execute_call[0][0] for execute_call in mock_cursor.execute.call_args_list
        ]
        for query in execute_calls:
            if "aws_s3.query_export_to_s3" in query:
                return query
        return None

    def _create_pyarrow_schema(self):
        """Helper to create a standard PyArrow schema for tests"""
        return pa.schema(
            [
                pa.field("id", pa.int32(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ]
        )

    def _create_parquet_writer_mock(self, mock_parquet_writer_class, schema=None):
        """Helper to create and configure ParquetWriter mock"""
        if schema is None:
            schema = self._create_pyarrow_schema()

        mock_parquet_writer = MagicMock()
        mock_parquet_writer_class.return_value = mock_parquet_writer
        mock_parquet_writer.build_pyarrow_schema.return_value = schema
        mock_parquet_writer.convert_csv_to_parquet.return_value = {
            "file_format": "parquet",
            "s3_path": "test/path.parquet",
            "pyarrow_schema": "dummy_schema_b64",
        }
        return mock_parquet_writer

    def _create_boto3_mocks(self, mock_boto_session):
        """Helper to create boto3 S3 client mocks"""
        mock_s3_client = MagicMock()
        mock_aws_session = MagicMock()
        mock_aws_session.client.return_value = mock_s3_client
        mock_boto_session.return_value = mock_aws_session
        return mock_s3_client, mock_aws_session

    def _create_strategy_with_config(self, **config_overrides):
        """Helper to create strategy with modified config"""
        conn_config = self.conn_config.copy()
        conn_config.update(config_overrides)
        return fast_sync_rds.FastSyncRdsStrategy(
            conn_config, self.s3_bucket, self.s3_prefix, self.s3_region
        )

    def _create_md_map_with_replication_key(self, replication_key="id"):
        """Helper to create md_map with replication key"""
        md_map = self.md_map.copy()
        md_map[()]["replication-key"] = replication_key
        md_map[("properties", replication_key)] = {"sql-datatype": "integer"}
        return md_map

    def _assert_s3_info_in_state(
        self,
        state,
        stream_id,
        expected_rows=50,
        expected_method="FULL_TABLE",
        expected_bytes=2500,
        expected_file_format="csv",
        expected_files_uploaded=1,
    ):
        """Helper to assert S3 info is embedded in STATE message bookmarks"""
        self.assertIn("bookmarks", state)
        self.assertIn(stream_id, state["bookmarks"])
        self.assertIn("fast_sync_s3_info", state["bookmarks"][stream_id])

        s3_info = state["bookmarks"][stream_id]["fast_sync_s3_info"]
        self.assertEqual(s3_info["s3_bucket"], "test-bucket")
        self.assertIn("s3_path", s3_info)
        # Verify s3_path format - should match pattern for both CSV and Parquet
        if expected_file_format == "csv":
            self.assertRegex(
                s3_info["s3_path"], r"^test/prefix/test_schema-test_table/.+_.+\.csv$"
            )
        elif expected_file_format == "parquet":
            self.assertRegex(
                s3_info["s3_path"],
                r"^test/prefix/test_schema-test_table/.+_.+\.parquet$",
            )
        self.assertEqual(s3_info["s3_region"], "us-east-1")
        self.assertEqual(s3_info["rows_uploaded"], expected_rows)
        self.assertEqual(s3_info["files_uploaded"], expected_files_uploaded)
        self.assertEqual(s3_info["bytes_uploaded"], expected_bytes)
        self.assertIn("time_extracted", s3_info)
        self.assertEqual(s3_info["replication_method"], expected_method)
        # Verify file format and parquet-specific fields
        self.assertEqual(s3_info["file_format"], expected_file_format)
        if expected_file_format == "csv":
            self.assertNotIn("pyarrow_schema", s3_info)
        elif expected_file_format == "parquet":
            self.assertIn("pyarrow_schema", s3_info)

    def test_init(self):
        """Test FastSyncRdsStrategy initialization"""
        strategy = fast_sync_rds.FastSyncRdsStrategy(
            self.conn_config, self.s3_bucket, self.s3_prefix, self.s3_region
        )
        self.assertEqual(strategy.s3_bucket, "test-bucket")
        self.assertEqual(strategy.s3_prefix, "test/prefix")
        self.assertEqual(strategy.s3_region, "us-east-1")

    def test_init_validates_output_format(self):
        """Test initialization validates output format setting"""
        # Valid formats
        for output_format in ["csv", "parquet"]:
            strategy = self._create_strategy_with_config(
                fast_sync_rds_output_format=output_format
            )
            self.assertIsNotNone(strategy)

        # Default format
        strategy = fast_sync_rds.FastSyncRdsStrategy(
            self.conn_config, self.s3_bucket, self.s3_prefix, self.s3_region
        )
        self.assertIsNotNone(strategy)

        # Invalid format
        with self.assertRaises(ValueError) as context:
            self._create_strategy_with_config(fast_sync_rds_output_format="json")

        self.assertIn(
            "Invalid value for 'fast_sync_rds_output_format'", str(context.exception)
        )
        self.assertIn("json", str(context.exception))
        self.assertIn("csv, parquet", str(context.exception))

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full(self, mock_open_conn):
        """Test sync_table_full - verifies S3 path, message structure, and metadata columns"""
        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        state = self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify state was updated
        self.assertIsNotNone(state)
        self.assertIn("bookmarks", state)

        # Verify S3 info is embedded in STATE message bookmarks
        self._assert_s3_info_in_state(
            state, self.stream["tap_stream_id"], expected_method="FULL_TABLE"
        )

        # Verify S3 path format (tests _generate_s3_path indirectly)
        s3_info = state["bookmarks"][self.stream["tap_stream_id"]]["fast_sync_s3_info"]
        self.assertRegex(
            s3_info["s3_path"], r"^test/prefix/test_schema-test_table/.+_.+\.csv$"
        )

        # Verify metadata columns are in the query (tests _prepend_metadata_columns indirectly)
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertIn("_sdc_batched_at", export_query)
        self.assertIn("_sdc_deleted_at", export_query)
        self.assertIn("_sdc_extracted_at", export_query)

        # Verify metadata columns are present and in correct order
        batched_pos = export_query.find("_sdc_batched_at")
        deleted_pos = export_query.find("_sdc_deleted_at")
        extracted_pos = export_query.find("_sdc_extracted_at")
        id_pos = export_query.find('"id"')

        self.assertGreater(batched_pos, 0)
        self.assertGreater(deleted_pos, batched_pos)
        self.assertGreater(extracted_pos, deleted_pos)
        if id_pos > 0:
            self.assertGreater(id_pos, extracted_pos)

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_column_ordering(self, mock_open_conn):
        """Test sync_table_full orders columns alphabetically including metadata columns"""
        desired_columns = ["zebra", "_id", "active"]
        md_map_with_columns = {
            (): {"schema-name": "test_schema"},
            ("properties", "_id"): {"sql-datatype": "integer"},
            ("properties", "active"): {"sql-datatype": "boolean"},
            ("properties", "zebra"): {"sql-datatype": "varchar"},
        }

        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=desired_columns,
            md_map=md_map_with_columns,
        )

        # Verify export query contains all columns
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertIn("_sdc_batched_at", export_query)
        self.assertIn("_sdc_deleted_at", export_query)
        self.assertIn("_sdc_extracted_at", export_query)
        self.assertIn('"_id"', export_query)
        self.assertIn('"active"', export_query)
        self.assertIn('"zebra"', export_query)

        # Verify complete column ordering: all columns should be sorted alphabetically
        # Expected order: _id, _sdc_batched_at, _sdc_deleted_at, _sdc_extracted_at, active, zebra
        expected_columns = [
            '"_id"',
            "_sdc_batched_at",
            "_sdc_deleted_at",
            "_sdc_extracted_at",
            '"active"',
            '"zebra"',
        ]

        # Find positions of all columns
        column_positions = [export_query.find(col) for col in expected_columns]

        self.assertEqual(len(column_positions), len(expected_columns))

        # Verify all columns are found
        for col, pos in zip(expected_columns, column_positions):
            self.assertGreater(pos, 0, f"{col} should be found in query")

        # Verify columns are in correct order (positions should be ascending)
        self.assertEqual(
            column_positions,
            sorted(column_positions),
            f"Columns should be in alphabetical order. Found positions: {dict(zip(expected_columns, column_positions))}",
        )

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_no_prefix(self, mock_open_conn):
        """Test sync_table_full with empty prefix - verifies S3 path generation"""
        strategy = fast_sync_rds.FastSyncRdsStrategy(
            self.conn_config, "test-bucket", "", "us-east-1"
        )

        mock_conn, _ = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        state = strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify S3 path doesn't start with double slashes
        s3_info = state["bookmarks"][self.stream["tap_stream_id"]]["fast_sync_s3_info"]
        s3_path = s3_info["s3_path"]
        self.assertFalse(s3_path.startswith("/"))
        self.assertRegex(s3_path, r"^test_schema-test_table/.+_.+\.csv$")

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_metadata_columns_disabled(self, mock_open_conn):
        """Test sync_table_full excludes metadata columns when setting is disabled"""
        strategy = self._create_strategy_with_config(
            fast_sync_rds_add_metadata_columns=False
        )

        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify metadata columns are NOT in the query
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertNotIn("_sdc_batched_at", export_query)
        self.assertNotIn("_sdc_deleted_at", export_query)
        self.assertNotIn("_sdc_extracted_at", export_query)

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.singer.get_bookmark")
    def test_sync_table_incremental(self, mock_get_bookmark, mock_open_conn):
        """Test sync_table_incremental - verifies WHERE clause, replication key tracking, and message"""
        mock_get_bookmark.side_effect = lambda state, stream_id, key: {
            "version": 1234567890,
            "replication_key_value": "100",
        }.get(key)

        md_map_with_key = self._create_md_map_with_replication_key()

        # Mock replication key result
        mock_replication_key_result = MagicMock()
        mock_replication_key_result.__getitem__.return_value = "200"

        mock_conn, mock_cursor = self._setup_mock_connection(
            export_result=self._create_mock_export_result(rows=25, bytes_uploaded=1250),
            replication_key_result=mock_replication_key_result,
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        state = self.fast_sync_rds_strategy.sync_table_incremental(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=md_map_with_key,
            replication_key="id",
            replication_key_value="100",
        )

        # Verify WHERE clause was included in query (tests query building indirectly)
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertIn("WHERE", export_query)
        self.assertIn("pg_speedup_trick", export_query)
        self.assertIn("id", export_query)

        # Verify S3 info is embedded in STATE message bookmarks
        self._assert_s3_info_in_state(
            state,
            self.stream["tap_stream_id"],
            expected_rows=25,
            expected_method="INCREMENTAL",
            expected_bytes=1250,
        )

        # Verify replication_key_value was tracked in state
        self.assertIn(
            "replication_key_value", state["bookmarks"][self.stream["tap_stream_id"]]
        )

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.singer.get_bookmark")
    def test_sync_table_incremental_sql_injection_protection(
        self, mock_get_bookmark, mock_open_conn
    ):
        """Test sync_table_incremental properly escapes SQL injection attempts"""
        mock_get_bookmark.side_effect = lambda state, stream_id, key: {
            "version": 1234567890,
            "replication_key_value": "100' OR '1'='1",
        }.get(key)

        md_map_with_key = self._create_md_map_with_replication_key()

        mock_replication_key_result = MagicMock()
        mock_replication_key_result.__getitem__.return_value = "200"

        mock_conn, mock_cursor = self._setup_mock_connection(
            replication_key_result=mock_replication_key_result
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self.fast_sync_rds_strategy.sync_table_incremental(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=md_map_with_key,
            replication_key="id",
            replication_key_value="100' OR '1'='1",
        )

        # Verify quotes are escaped in the query
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        # Escaped quotes should appear as ''
        self.assertIn("''", export_query)

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_export_error(self, mock_open_conn):
        """Test sync_table_full handles export errors"""
        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_cursor.fetchone.return_value = None  # Simulate export failure
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        with self.assertRaises(Exception) as context:
            self.fast_sync_rds_strategy.sync_table_full(
                stream=self.stream,
                state=self.state,
                desired_columns=self.desired_columns,
                md_map=self.md_map,
            )

        self.assertIn("Export to S3 failed", str(context.exception))

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_multiple_files(self, mock_open_conn):
        """Test sync_table_full with multiple files (file splitting scenario)"""
        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(
                rows=1000000, files=3, bytes_uploaded=18000000000
            )
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        state = self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        s3_info = state["bookmarks"][self.stream["tap_stream_id"]]["fast_sync_s3_info"]
        self.assertEqual(s3_info["files_uploaded"], 3)
        self.assertEqual(s3_info["rows_uploaded"], 1000000)
        self.assertEqual(s3_info["bytes_uploaded"], 18000000000)

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.ParquetWriter")
    def test_sync_table_full_empty_table(
        self, mock_parquet_writer_class, mock_open_conn
    ):
        """Test sync_table_full with empty table (0 rows) - S3 info should not be stored and Parquet conversion should not happen"""
        # Test with default CSV output format
        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(rows=0, bytes_uploaded=0)
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        state = self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify S3 info is NOT stored in state when rows_uploaded is 0
        # (no need to store S3 info if there's no data to load)
        stream_bookmarks = state["bookmarks"][self.stream["tap_stream_id"]]
        self.assertNotIn("fast_sync_s3_info", stream_bookmarks)

        # Verify state still has version (sync still occurred)
        self.assertIn("version", stream_bookmarks)

        # Verify ParquetWriter was NOT initialized (no rows, so no conversion)
        mock_parquet_writer_class.assert_not_called()

        # Test with parquet output format - should still not convert when rows_uploaded is 0
        strategy_parquet = self._create_strategy_with_config(
            fast_sync_rds_output_format="parquet"
        )
        mock_parquet_writer_class.reset_mock()

        state_parquet = strategy_parquet.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify S3 info is still NOT stored
        stream_bookmarks_parquet = state_parquet["bookmarks"][
            self.stream["tap_stream_id"]
        ]
        self.assertNotIn("fast_sync_s3_info", stream_bookmarks_parquet)

        # Verify ParquetWriter was NOT initialized even with parquet output format
        # (no rows, so no conversion should happen)
        mock_parquet_writer_class.assert_not_called()

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_single_row(self, mock_open_conn):
        """Test sync_table_full with single row - verifies S3 info is stored when rows_uploaded > 0"""
        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(rows=1, bytes_uploaded=100)
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        state = self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify S3 info IS stored in state when rows_uploaded > 0 (even for single row)
        self._assert_s3_info_in_state(
            state, self.stream["tap_stream_id"], expected_rows=1, expected_bytes=100
        )

    @patch("tap_postgres.incremental.sync_table")
    @patch("tap_postgres.sync_common.send_schema_message")
    def test_do_sync_incremental_backward_compatibility_with_fast_sync_s3_info(
        self, mock_send_schema, mock_sync_table
    ):
        """
        Test backward compatibility: do_sync_incremental accepts fast_sync_s3_info in state
        when fast_sync_rds is disabled (ensures state from previous fast_sync_rds run doesn't break traditional sync)
        """
        # State with fast_sync_s3_info from previous fast_sync_rds run
        state_with_s3_info = {
            "bookmarks": {
                "test_schema-test_table": {
                    "version": 1234567890,
                    "replication_key": "updated_at",
                    "replication_key_value": "2025-01-01T00:00:00",
                    "last_replication_method": "INCREMENTAL",
                    "fast_sync_s3_info": {
                        "s3_bucket": "test-bucket",
                        "s3_path": "test/path.csv",
                        "s3_region": "us-east-1",
                        "rows_uploaded": 100,
                        "files_uploaded": 1,
                        "bytes_uploaded": 5000,
                        "time_extracted": "2025-01-01T12:00:00",
                        "replication_method": "INCREMENTAL",
                    },
                }
            }
        }

        # Stream with replication key
        stream_with_key = {
            "tap_stream_id": "test_schema-test_table",
            "stream": "test_table",
            "table_name": "test_table",
            "schema": {
                "properties": {
                    "id": {"type": "integer"},
                    "updated_at": {"type": "string", "format": "date-time"},
                }
            },
            "metadata": [
                {
                    "breadcrumb": [],
                    "metadata": {
                        "schema-name": "test_schema",
                        "replication-key": "updated_at",
                    },
                }
            ],
        }

        md_map_with_key = {
            (): {"schema-name": "test_schema", "replication-key": "updated_at"},
            ("properties", "updated_at"): {
                "sql-datatype": "timestamp without time zone"
            },
        }

        conn_config_no_fast_sync = {
            "host": "test-host",
            "dbname": "test_db",
            "user": "test_user",
            "password": "test_pass",
            "port": 5432,
            "fast_sync_rds": False,  # Disabled - should use traditional sync
        }

        # Mock sync_table to return updated state
        mock_sync_table.return_value = state_with_s3_info

        # This should not raise an exception - fast_sync_s3_info should be accepted
        result = tap_postgres.do_sync_incremental(
            conn_config_no_fast_sync,
            stream_with_key,
            state_with_s3_info,
            ["id", "updated_at"],
            md_map_with_key,
        )

        # Verify sync_table was called (traditional incremental sync executed)
        mock_sync_table.assert_called_once()
        mock_send_schema.assert_called_once()

        # Verify result is returned without errors
        self.assertIsNotNone(result)
        # Verify fast_sync_s3_info is still in state (not removed, just ignored)
        self.assertIn(
            "fast_sync_s3_info", result["bookmarks"]["test_schema-test_table"]
        )

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_with_array_columns(self, mock_open_conn):
        """Test sync_table_full converts array columns to JSON format in export query"""
        # Test with mixed array types (text, integer, date, timestamp) and non-array columns
        desired_columns = ["id", "_text", "name", "_date", "_int", "_timestamp"]
        md_map_with_arrays = {
            (): {"schema-name": "test_schema"},
            ("properties", "id"): {"sql-datatype": "integer"},
            ("properties", "_text"): {"sql-datatype": "text[]"},
            ("properties", "name"): {"sql-datatype": "varchar"},
            ("properties", "_date"): {"sql-datatype": "date[]"},
            ("properties", "_int"): {"sql-datatype": "integer[]"},
            ("properties", "_timestamp"): {
                "sql-datatype": "timestamp without time zone[]"
            },
        }

        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=desired_columns,
            md_map=md_map_with_arrays,
        )

        # Verify export query contains array_to_json for array columns
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)

        # Array columns should use array_to_json conversion
        self.assertIn("array_to_json", export_query)

        # Count array_to_json occurrences - should match number of array columns
        array_columns = ["_text", "_date", "_int", "_timestamp"]
        array_to_json_count = export_query.count("array_to_json")
        self.assertEqual(
            array_to_json_count,
            len(array_columns),
            "All array columns should use array_to_json",
        )

        # Verify each array column is present and wrapped with array_to_json
        # (accounting for possible whitespace variations in SQL formatting)
        for col in array_columns:
            self.assertIn(col, export_query)
            self.assertRegex(
                export_query,
                rf'array_to_json\s*\(\s*["\s]*{col}["\s]*\)',
                f"Array column '{col}' should be wrapped with array_to_json",
            )

        # Non-array columns should not be wrapped with array_to_json
        non_array_columns = ["id", "name"]
        for col in non_array_columns:
            self.assertIn(f'"{col}"', export_query)
            # Verify they are not wrapped
            self.assertNotRegex(
                export_query,
                rf'array_to_json\s*\(\s*["\s]*{col}["\s]*\)',
                f"Non-array column '{col}' should not use array_to_json",
            )

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.singer.get_bookmark")
    def test_sync_table_incremental_with_array_columns(
        self, mock_get_bookmark, mock_open_conn
    ):
        """Test sync_table_incremental converts array columns to JSON format"""
        mock_get_bookmark.side_effect = lambda state, stream_id, key: {
            "version": 1234567890,
            "replication_key_value": "100",
        }.get(key)

        desired_columns = ["id", "_text", "name", "_int"]
        md_map_with_arrays = {
            (): {"schema-name": "test_schema", "replication-key": "id"},
            ("properties", "id"): {"sql-datatype": "integer"},
            ("properties", "_text"): {"sql-datatype": "text[]"},
            ("properties", "name"): {"sql-datatype": "varchar"},
            ("properties", "_int"): {"sql-datatype": "integer[]"},
        }

        mock_replication_key_result = MagicMock()
        mock_replication_key_result.__getitem__.return_value = "200"

        mock_conn, mock_cursor = self._setup_mock_connection(
            export_result=self._create_mock_export_result(rows=25, bytes_uploaded=1250),
            replication_key_result=mock_replication_key_result,
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self.fast_sync_rds_strategy.sync_table_incremental(
            stream=self.stream,
            state=self.state,
            desired_columns=desired_columns,
            md_map=md_map_with_arrays,
            replication_key="id",
            replication_key_value="100",
        )

        # Verify export query contains array_to_json for array columns
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)

        # Array columns should use array_to_json
        self.assertIn("array_to_json", export_query)
        self.assertIn("_text", export_query)
        self.assertIn("_int", export_query)

        # Verify array columns are wrapped correctly (with flexible whitespace)
        self.assertRegex(
            export_query,
            r'array_to_json\s*\(\s*["\s]*_text["\s]*\)',
            "Array column '_text' should be wrapped with array_to_json",
        )
        self.assertRegex(
            export_query,
            r'array_to_json\s*\(\s*["\s]*_int["\s]*\)',
            "Array column '_int' should be wrapped with array_to_json",
        )

        # Non-array columns should not be wrapped
        self.assertIn('"id"', export_query)
        self.assertIn('"name"', export_query)
        # Verify they are not wrapped with array_to_json
        self.assertNotRegex(
            export_query,
            r'array_to_json\s*\(\s*["\s]*id["\s]*\)',
            "Non-array column 'id' should not use array_to_json",
        )
        self.assertNotRegex(
            export_query,
            r'array_to_json\s*\(\s*["\s]*name["\s]*\)',
            "Non-array column 'name' should not use array_to_json",
        )

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_with_transformations(self, mock_open_conn):
        """Test sync_table_full applies transformations when configured"""
        # Configure transformations for the stream
        strategy = self._create_strategy_with_config(
            fast_sync_rds_transformations={
                "test_schema-test_table": {
                    "name": "UPPER(name)",
                    "id": "id * 2",
                }
            }
        )

        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify transformation SQL is in the export query
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertIn("UPPER(name)", export_query)
        self.assertIn("id * 2", export_query)
        # Verify the transformations are wrapped with column aliases
        self.assertIn('(UPPER(name)) AS  "name"', export_query)
        self.assertIn('(id * 2) AS  "id"', export_query)

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_without_transformations(self, mock_open_conn):
        """Test sync_table_full does not apply transformations when not configured"""
        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify normal column references are used (not transformations)
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        # Should use normal column references, not transformation expressions
        self.assertIn('"id"', export_query)
        self.assertIn('"name"', export_query)
        # Should not have transformation-like patterns
        self.assertNotIn("UPPER", export_query)
        self.assertNotIn(" * 2", export_query)

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_transformations_different_stream(self, mock_open_conn):
        """Test transformations are only applied to the configured stream"""
        # Configure transformations for a different stream
        strategy = self._create_strategy_with_config(
            fast_sync_rds_transformations={
                "other_schema-other_table": {
                    "name": "UPPER(name)",
                }
            }
        )

        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify transformation is NOT applied (different stream)
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertNotIn("UPPER(name)", export_query)
        # Should use normal column references
        self.assertIn('"name"', export_query)

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.singer.get_bookmark")
    def test_sync_table_incremental_with_transformations(
        self, mock_get_bookmark, mock_open_conn
    ):
        """Test sync_table_incremental applies transformations when configured"""
        mock_get_bookmark.side_effect = lambda state, stream_id, key: {
            "version": 1234567890,
            "replication_key_value": "100",
        }.get(key)

        # Configure transformations
        strategy = self._create_strategy_with_config(
            fast_sync_rds_transformations={
                "test_schema-test_table": {
                    "name": "LOWER(name)",
                }
            }
        )

        md_map_with_key = self._create_md_map_with_replication_key()

        mock_replication_key_result = MagicMock()
        mock_replication_key_result.__getitem__.return_value = "200"

        mock_conn, mock_cursor = self._setup_mock_connection(
            export_result=self._create_mock_export_result(rows=25, bytes_uploaded=1250),
            replication_key_result=mock_replication_key_result,
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        strategy.sync_table_incremental(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=md_map_with_key,
            replication_key="id",
            replication_key_value="100",
        )

        # Verify transformation SQL is in the export query
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertIn("LOWER(name)", export_query)
        self.assertIn('(LOWER(name)) AS  "name"', export_query)

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_transformations_with_array_columns(self, mock_open_conn):
        """Test transformations work correctly with array columns"""
        # Configure transformations for a non-array column
        strategy = self._create_strategy_with_config(
            fast_sync_rds_transformations={
                "test_schema-test_table": {
                    "name": "UPPER(name)",
                }
            }
        )

        desired_columns = ["id", "name", "_text"]
        md_map_with_arrays = {
            (): {"schema-name": "test_schema"},
            ("properties", "id"): {"sql-datatype": "integer"},
            ("properties", "name"): {"sql-datatype": "varchar"},
            ("properties", "_text"): {"sql-datatype": "text[]"},
        }

        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=desired_columns,
            md_map=md_map_with_arrays,
        )

        # Verify transformation is applied to non-array column
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertIn("UPPER(name)", export_query)
        self.assertIn('(UPPER(name)) AS  "name"', export_query)

        # Verify array column still uses array_to_json (not transformation)
        self.assertIn("array_to_json", export_query)
        self.assertIn("_text", export_query)
        self.assertRegex(
            export_query,
            r'array_to_json\s*\(\s*["\s]*_text["\s]*\)',
            "Array column '_text' should use array_to_json, not transformation",
        )

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    def test_sync_table_full_transformations_with_metadata_and_partial_columns(
        self, mock_open_conn
    ):
        """Test transformations work correctly with metadata columns and can be applied to only some columns"""
        # Configure transformation for only one column (not "id")
        strategy = self._create_strategy_with_config(
            fast_sync_rds_transformations={
                "test_schema-test_table": {
                    "name": "UPPER(name)",
                    # No transformation for "id"
                }
            }
        )

        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify transformation is applied to "name"
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertIn("UPPER(name)", export_query)
        self.assertRegex(
            export_query,
            r'\(UPPER\(name\)\)\s+AS\s+"name"',
            "Transformation for 'name' should be wrapped with alias",
        )

        # Verify "id" uses normal column reference (no transformation)
        self.assertIn('"id"', export_query)
        self.assertNotRegex(
            export_query,
            r'\(.*\)\s+AS\s+"id"',
            "Column 'id' should not have transformation",
        )

        # Verify metadata columns are still present and not transformed
        self.assertIn("_sdc_batched_at", export_query)
        self.assertIn("_sdc_deleted_at", export_query)
        self.assertIn("_sdc_extracted_at", export_query)
        # Metadata columns should use their standard SQL, not transformations
        # SQL escaping converts single quotes to double single quotes
        # Use case-insensitive regex to match AS/as
        self.assertRegex(
            export_query,
            r"current_timestamp\s+at\s+time\s+zone\s+''UTC''\s+AS\s+_sdc_batched_at",
            "Metadata column '_sdc_batched_at' should use standard SQL",
        )

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.ParquetWriter")
    def test_sync_table_full_with_parquet_conversion(
        self, mock_parquet_writer_class, mock_open_conn
    ):
        """Test sync_table_full converts CSV to Parquet when configured"""
        strategy = self._create_strategy_with_config(
            fast_sync_rds_output_format="parquet"
        )

        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(
                rows=100, files=1, bytes_uploaded=5000
            )
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        # Mock ParquetWriter
        mock_parquet_writer = self._create_parquet_writer_mock(
            mock_parquet_writer_class
        )

        state = strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify ParquetWriter was initialized
        mock_parquet_writer_class.assert_called_once_with(
            self.s3_bucket, self.s3_region, proxy_options=None
        )

        # Verify convert_csv_to_parquet was called
        mock_parquet_writer.convert_csv_to_parquet.assert_called_once()
        call_kwargs = mock_parquet_writer.convert_csv_to_parquet.call_args[1]
        # desired_columns should be sorted and include metadata columns
        expected_columns = sorted(
            ["_sdc_batched_at", "_sdc_deleted_at", "_sdc_extracted_at", "id", "name"]
        )
        self.assertEqual(call_kwargs["desired_columns"], expected_columns)
        self.assertIn("md_map", call_kwargs)
        self.assertIn("arrow_schema", call_kwargs)

        # Verify S3 info includes parquet information
        self._assert_s3_info_in_state(
            state,
            self.stream["tap_stream_id"],
            expected_rows=100,
            expected_method="FULL_TABLE",
            expected_bytes=5000,
            expected_file_format="parquet",
        )
        # Verify s3_path points to parquet file (ends with .parquet)
        s3_info = state["bookmarks"][self.stream["tap_stream_id"]]["fast_sync_s3_info"]
        self.assertTrue(s3_info["s3_path"].endswith(".parquet"))

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.ParquetWriter")
    def test_sync_table_full_with_parquet_conversion_multiple_files(
        self, mock_parquet_writer_class, mock_open_conn
    ):
        """Test sync_table_full converts multiple CSV parts to Parquet"""
        strategy = self._create_strategy_with_config(
            fast_sync_rds_output_format="parquet"
        )

        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(
                rows=1000000, files=3, bytes_uploaded=50000000
            )
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        # Mock ParquetWriter
        mock_parquet_writer = self._create_parquet_writer_mock(
            mock_parquet_writer_class
        )

        state = strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify convert_csv_to_parquet was called 3 times (once per file part)
        self.assertEqual(mock_parquet_writer.convert_csv_to_parquet.call_count, 3)

        # Verify S3 info includes parquet information
        self._assert_s3_info_in_state(
            state,
            self.stream["tap_stream_id"],
            expected_rows=1000000,
            expected_method="FULL_TABLE",
            expected_bytes=50000000,
            expected_file_format="parquet",
            expected_files_uploaded=3,
        )

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.ParquetWriter")
    def test_build_parquet_path_hive_style_partitioning(
        self, mock_parquet_writer_class, mock_open_conn
    ):
        """Test _build_parquet_path creates Hive-style partitioned paths"""
        strategy = self._create_strategy_with_config(
            fast_sync_rds_output_format="parquet"
        )

        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(
                rows=100, files=1, bytes_uploaded=5000
            )
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        mock_parquet_writer = self._create_parquet_writer_mock(
            mock_parquet_writer_class
        )

        # Capture the parquet_path passed to convert_csv_to_parquet
        def capture_parquet_path(*args, **kwargs):
            parquet_path = kwargs.get("parquet_path")
            if parquet_path:
                # Store in mock for later retrieval
                mock_parquet_writer._captured_path = parquet_path
            return {
                "file_format": "parquet",
                "s3_path": "test/path.parquet",
                "pyarrow_schema": "dummy_schema_b64",
            }

        mock_parquet_writer.convert_csv_to_parquet.side_effect = capture_parquet_path

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify convert_csv_to_parquet was called
        self.assertTrue(mock_parquet_writer.convert_csv_to_parquet.called)

        # Get the parquet_path argument from the call
        call_kwargs = mock_parquet_writer.convert_csv_to_parquet.call_args[1]
        parquet_path_arg = call_kwargs.get("parquet_path")

        # Verify parquet_path has Hive-style partitioning
        if parquet_path_arg:
            # Should match pattern: {dir}/_sdc_batched_at={date}/{filename}.parquet
            pattern = r".+/_sdc_batched_at=\d{4}-\d{2}-\d{2}/.+\.parquet"
            self.assertRegex(parquet_path_arg, pattern)

            # Verify date format is YYYY-MM-DD
            date_match = re.search(
                r"_sdc_batched_at=(\d{4}-\d{2}-\d{2})", parquet_path_arg
            )
            self.assertIsNotNone(date_match)
            date_str = date_match.group(1)

            # Verify date is today's date (UTC)
            expected_date = time.strftime("%Y-%m-%d", time.gmtime())
            self.assertEqual(date_str, expected_date)

            # Verify filename is preserved (without .csv extension)
            csv_path_arg = call_kwargs.get("csv_path", "")
            if csv_path_arg:
                csv_filename = csv_path_arg.split("/")[-1].replace(".csv", "")
                parquet_filename = parquet_path_arg.split("/")[-1].replace(
                    ".parquet", ""
                )
                self.assertEqual(parquet_filename, csv_filename)

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.boto3.session.Session")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.ParquetWriter")
    def test_sync_table_full_with_csv_cleanup_enabled(
        self, mock_parquet_writer_class, mock_boto_session, mock_open_conn
    ):
        """Test sync_table_full deletes CSV files after Parquet conversion when cleanup is enabled"""
        strategy = self._create_strategy_with_config(
            fast_sync_rds_output_format="parquet",
            fast_sync_rds_delete_intermediate_csv=True,
        )

        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(
                rows=100, files=1, bytes_uploaded=5000
            )
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self._create_parquet_writer_mock(mock_parquet_writer_class)

        # Mock boto3 S3 client
        mock_s3_client, mock_aws_session = self._create_boto3_mocks(mock_boto_session)

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify S3 client was created
        mock_aws_session.client.assert_called_once_with(
            "s3", region_name=self.s3_region
        )

        # Verify delete_object was called for the CSV file
        mock_s3_client.delete_object.assert_called_once()
        delete_call = mock_s3_client.delete_object.call_args
        self.assertEqual(delete_call[1]["Bucket"], self.s3_bucket)
        # Verify the CSV path is in the key (extracted from export)
        self.assertIn("test", delete_call[1]["Key"])

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.boto3.session.Session")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.ParquetWriter")
    def test_sync_table_full_with_csv_cleanup_disabled(
        self, mock_parquet_writer_class, mock_boto_session, mock_open_conn
    ):
        """Test sync_table_full does not delete CSV files when cleanup is disabled"""
        strategy = self._create_strategy_with_config(
            fast_sync_rds_output_format="parquet",
            fast_sync_rds_delete_intermediate_csv=False,
        )

        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(
                rows=100, files=1, bytes_uploaded=5000
            )
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self._create_parquet_writer_mock(mock_parquet_writer_class)

        # Mock boto3 S3 client (should not be used)
        mock_s3_client, mock_aws_session = self._create_boto3_mocks(mock_boto_session)

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify S3 client was NOT created (cleanup is disabled)
        mock_aws_session.client.assert_not_called()
        mock_s3_client.delete_object.assert_not_called()

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.boto3.session.Session")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.ParquetWriter")
    def test_sync_table_full_with_csv_cleanup_multiple_files(
        self, mock_parquet_writer_class, mock_boto_session, mock_open_conn
    ):
        """Test sync_table_full deletes all CSV file parts for multi-part exports"""
        strategy = self._create_strategy_with_config(
            fast_sync_rds_output_format="parquet",
            fast_sync_rds_delete_intermediate_csv=True,
        )

        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(
                rows=1000000, files=3, bytes_uploaded=50000000
            )
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self._create_parquet_writer_mock(mock_parquet_writer_class)

        # Mock boto3 S3 client
        mock_s3_client, _ = self._create_boto3_mocks(mock_boto_session)

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify delete_object was called 3 times (once per file part)
        self.assertEqual(mock_s3_client.delete_object.call_count, 3)

        # Verify all calls have correct bucket
        for call in mock_s3_client.delete_object.call_args_list:
            self.assertEqual(call[1]["Bucket"], self.s3_bucket)
            # Verify part suffix in key for multi-part files
            key = call[1]["Key"]
            self.assertTrue("_part" in key or key.endswith(".csv"))

    @patch("tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.boto3.session.Session")
    @patch("tap_postgres.sync_strategies.fast_sync_rds.ParquetWriter")
    def test_sync_table_full_with_csv_cleanup_error_handling(
        self, mock_parquet_writer_class, mock_boto_session, mock_open_conn
    ):
        """Test sync_table_full handles CSV cleanup errors gracefully without failing sync"""
        strategy = self._create_strategy_with_config(
            fast_sync_rds_output_format="parquet",
            fast_sync_rds_delete_intermediate_csv=True,
        )

        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(
                rows=100, files=1, bytes_uploaded=5000
            )
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self._create_parquet_writer_mock(mock_parquet_writer_class)

        # Mock boto3 S3 client to raise an error
        mock_s3_client, _ = self._create_boto3_mocks(mock_boto_session)
        mock_s3_client.delete_object.side_effect = Exception("S3 deletion failed")

        # Should not raise exception - cleanup errors should be handled gracefully
        state = strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map,
        )

        # Verify sync completed successfully despite cleanup error
        self.assertIsNotNone(state)
        self.assertIn("bookmarks", state)
        # Verify S3 info is still in state (sync succeeded)
        self.assertIn(self.stream["tap_stream_id"], state["bookmarks"])
