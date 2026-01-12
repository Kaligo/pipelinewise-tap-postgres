"""
Unit tests for fast_sync_rds strategy
"""

from unittest import TestCase
from unittest.mock import patch, MagicMock
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

    def _assert_s3_info_in_state(
        self,
        state,
        stream_id,
        expected_rows=50,
        expected_method="FULL_TABLE",
        expected_bytes=2500,
    ):
        """Helper to assert S3 info is embedded in STATE message bookmarks"""
        self.assertIn("bookmarks", state)
        self.assertIn(stream_id, state["bookmarks"])
        self.assertIn("fast_sync_s3_info", state["bookmarks"][stream_id])

        s3_info = state["bookmarks"][stream_id]["fast_sync_s3_info"]
        self.assertEqual(s3_info["s3_bucket"], "test-bucket")
        self.assertIn("s3_path", s3_info)
        self.assertRegex(
            s3_info["s3_path"], r"^test/prefix/test_schema-test_table/.+_.+\.csv$"
        )
        self.assertEqual(s3_info["s3_region"], "us-east-1")
        self.assertEqual(s3_info["rows_uploaded"], expected_rows)
        self.assertEqual(s3_info["files_uploaded"], 1)
        self.assertEqual(s3_info["bytes_uploaded"], expected_bytes)
        self.assertIn("time_extracted", s3_info)
        self.assertEqual(s3_info["replication_method"], expected_method)

    def test_init(self):
        """Test FastSyncRdsStrategy initialization"""
        strategy = fast_sync_rds.FastSyncRdsStrategy(
            self.conn_config, self.s3_bucket, self.s3_prefix, self.s3_region
        )
        self.assertEqual(strategy.s3_bucket, "test-bucket")
        self.assertEqual(strategy.s3_prefix, "test/prefix")
        self.assertEqual(strategy.s3_region, "us-east-1")

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
        for i, (col, pos) in enumerate(zip(expected_columns, column_positions)):
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
        conn_config_no_metadata = self.conn_config.copy()
        conn_config_no_metadata["fast_sync_rds_add_metadata_columns"] = False
        strategy = fast_sync_rds.FastSyncRdsStrategy(
            conn_config_no_metadata, self.s3_bucket, self.s3_prefix, self.s3_region
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

        # Add replication key to metadata
        md_map_with_key = self.md_map.copy()
        md_map_with_key[()]["replication-key"] = "id"
        md_map_with_key[("properties", "id")] = {"sql-datatype": "integer"}

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

        md_map_with_key = self.md_map.copy()
        md_map_with_key[()]["replication-key"] = "id"
        md_map_with_key[("properties", "id")] = {"sql-datatype": "integer"}

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
    def test_sync_table_full_empty_table(self, mock_open_conn):
        """Test sync_table_full with empty table (0 rows)"""
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

        # Verify S3 info is still embedded in state even with 0 rows
        s3_info = state["bookmarks"][self.stream["tap_stream_id"]]["fast_sync_s3_info"]
        self.assertEqual(s3_info["rows_uploaded"], 0)
        self.assertEqual(s3_info["bytes_uploaded"], 0)

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
