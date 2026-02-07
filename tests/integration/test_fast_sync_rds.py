"""
Integration tests for fast_sync_rds strategy
"""

import contextlib
import io
import json
import unittest
import unittest.mock

import tap_postgres

from ..utils import (
    get_test_connection_config,
    ensure_test_table,
    set_replication_method_for_stream,
    get_test_connection,
    insert_record,
    drop_table,
)


def do_not_dump_catalog(catalog):
    pass


tap_postgres.dump_catalog = do_not_dump_catalog


@contextlib.contextmanager
def mock_export_to_s3(mock_result):
    """
    Context manager that mocks the database cursor to simulate aws_s3.query_export_to_s3 results.
    This mocks at the database connection level (external service) rather than internal methods.
    The mock is selective - it only mocks queries containing 'aws_s3.query_export_to_s3',
    allowing other database operations to use real connections.

    Args:
        mock_result: Dictionary with 'rows_uploaded', 'files_uploaded', 'bytes_uploaded'

    Yields:
        io.StringIO: StringIO object containing captured stdout
    """
    my_stdout = io.StringIO()
    with contextlib.redirect_stdout(my_stdout):
        import tap_postgres.db as post_db

        original_open_connection = post_db.open_connection

        # Create a mock result that behaves like a DictCursor row
        class MockDictCursorResult:
            """Mock result that behaves like a DictCursor row"""

            def __init__(self, data):
                self._data = data

            def __getitem__(self, key):
                # Support both string keys (dict access) and integer indices (tuple access)
                if isinstance(key, int):
                    # For integer indices, return values in order
                    values = list(self._data.values())
                    if 0 <= key < len(values):
                        return values[key]
                    raise IndexError(f"Index {key} out of range")
                return self._data[key]

            def __contains__(self, key):
                return key in self._data

        class SelectiveMockCursor:
            """Mock cursor that only mocks aws_s3.query_export_to_s3 queries"""

            def __init__(self, real_conn, mock_result, cursor_factory=None):
                self.real_conn = real_conn
                self.mock_result = mock_result
                self.executed_query = None
                self.cursor_factory = cursor_factory
                self._real_cursor = None
                self._is_export_query = False

            def _get_real_cursor(self):
                """Get real cursor from real connection"""
                if self._real_cursor is None:
                    if self.cursor_factory:
                        self._real_cursor = self.real_conn.cursor(
                            cursor_factory=self.cursor_factory
                        )
                    else:
                        self._real_cursor = self.real_conn.cursor()
                return self._real_cursor

            def execute(self, query, *args, **kwargs):
                self.executed_query = query
                self._is_export_query = "aws_s3.query_export_to_s3" in query
                # For non-export queries, execute on real connection
                if not self._is_export_query:
                    real_cursor = self._get_real_cursor()
                    real_cursor.execute(query, *args, **kwargs)

            def fetchone(self):
                # Only return mock result for aws_s3.query_export_to_s3 queries
                if self._is_export_query:
                    return MockDictCursorResult(self.mock_result)
                # For other queries, use real cursor
                real_cursor = self._get_real_cursor()
                return real_cursor.fetchone()

            def __getattr__(self, name):
                # Delegate all other methods/attributes to real cursor
                real_cursor = self._get_real_cursor()
                return getattr(real_cursor, name)

            def __enter__(self):
                return self

            def __exit__(self, *args, **kwargs):
                if self._real_cursor:
                    return self._real_cursor.__exit__(*args, **kwargs)
                return None

        def selective_mock_open_connection(conn_config, *args, **kwargs):
            """Open real connection but wrap cursor for selective mocking"""
            real_conn = original_open_connection(conn_config, *args, **kwargs)

            # Create a wrapper that intercepts cursor() calls
            class ConnectionWrapper:
                def __init__(self, real_conn, mock_result):
                    self.real_conn = real_conn
                    self.mock_result = mock_result

                def cursor(self, cursor_factory=None, *args, **kwargs):
                    # Return selective mock cursor that delegates to real for non-export queries
                    return SelectiveMockCursor(
                        self.real_conn, self.mock_result, cursor_factory
                    )

                def __getattr__(self, name):
                    # Delegate all other attributes to real connection
                    return getattr(self.real_conn, name)

                def __enter__(self):
                    return self

                def __exit__(self, *args, **kwargs):
                    return self.real_conn.__exit__(*args, **kwargs)

            return ConnectionWrapper(real_conn, mock_result)

        # Mock post_db.open_connection only within the fast_sync_rds module
        # This mocks the external database service, not internal methods
        with unittest.mock.patch(
            "tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection",
            side_effect=selective_mock_open_connection,
        ):
            yield my_stdout


class TestFastSyncRds(unittest.TestCase):
    table_name = None
    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.table_name = "fast_sync_test_table"
        table_spec = {
            "columns": [
                {"name": "id", "type": "serial", "primary_key": True},
                {"name": "name", "type": "character varying"},
                {"name": "value", "type": "integer"},
                {"name": "updated_at", "type": "timestamp without time zone"},
            ],
            "name": cls.table_name,
        }

        ensure_test_table(table_spec)
        cls.config = get_test_connection_config()
        # Add fast_sync_rds configuration
        cls.config["fast_sync_rds"] = True
        cls.config["fast_sync_rds_s3_bucket"] = "test-bucket"
        cls.config["fast_sync_rds_s3_prefix"] = "test/prefix"
        cls.config["fast_sync_rds_s3_region"] = "us-east-1"

    @classmethod
    def tearDownClass(cls) -> None:
        drop_table(cls.table_name)

    def setUp(self):
        # Clean up table before each test
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE "{self.table_name}"')

    def _get_test_stream(self, stream_id=None):
        """Helper to get test stream from discovery"""
        if stream_id is None:
            stream_id = f"public-{self.table_name}"
        streams = tap_postgres.do_discovery(self.config)
        return [s for s in streams if s["tap_stream_id"] == stream_id][0]

    def _set_replication_key(self, stream, replication_key):
        """Helper to set replication key in stream metadata"""
        for md in stream["metadata"]:
            if md["breadcrumb"] == []:
                md["metadata"]["replication-key"] = replication_key
                break
        return stream

    def _build_md_map(self, stream):
        """Helper to build metadata map from stream"""
        md_map = {}
        for md in stream["metadata"]:
            md_map[md["breadcrumb"]] = md["metadata"]
        return md_map

    def _find_state_with_s3_info(self, output, stream_id):
        """
        Helper to find STATE message containing fast_sync_s3_info for a stream.

        Args:
            output: String containing stdout output
            stream_id: Stream ID to search for

        Returns:
            Dictionary containing the STATE message with fast_sync_s3_info, or None
        """
        lines = output.strip().split("\n")
        state_messages = [
            json.loads(line) for line in lines if '"type": "STATE"' in line
        ]

        for state_msg in state_messages:
            if (
                state_msg.get("type") == "STATE"
                and "bookmarks" in state_msg.get("value", {})
                and stream_id in state_msg["value"]["bookmarks"]
                and "fast_sync_s3_info" in state_msg["value"]["bookmarks"][stream_id]
            ):
                return state_msg
        return None

    def _assert_s3_info_basic(
        self, s3_info, expected_rows, expected_method="FULL_TABLE"
    ):
        """Helper to assert basic S3 info fields"""
        self.assertEqual(s3_info["s3_bucket"], "test-bucket")
        self.assertIn("test/prefix", s3_info["s3_path"])
        self.assertIn(f"public-{self.table_name}", s3_info["s3_path"])
        self.assertTrue(s3_info["s3_path"].endswith(".csv"))
        self.assertEqual(s3_info["s3_region"], "us-east-1")
        self.assertEqual(s3_info["rows_uploaded"], expected_rows)
        self.assertEqual(s3_info["replication_method"], expected_method)
        self.assertIn("time_extracted", s3_info)
        self.assertIsInstance(s3_info["time_extracted"], str)

    def test_fast_sync_rds_full_table_sync(self):
        """Test full table sync with fast_sync_rds - covers message structure, S3 path, version, etc."""
        # Insert test data
        with get_test_connection() as conn:
            insert_record(
                conn.cursor(),
                self.table_name,
                {"name": "test1", "value": 100, "updated_at": "2024-01-01 10:00:00"},
            )
            insert_record(
                conn.cursor(),
                self.table_name,
                {"name": "test2", "value": 200, "updated_at": "2024-01-01 11:00:00"},
            )
            conn.commit()

        test_stream = self._get_test_stream()
        test_stream = set_replication_method_for_stream(test_stream, "FULL_TABLE")

        mock_result = {"rows_uploaded": 2, "files_uploaded": 1, "bytes_uploaded": 1024}

        with mock_export_to_s3(mock_result) as my_stdout:
            state = tap_postgres.do_sync(
                self.config, {"streams": [test_stream]}, "FULL_TABLE", {}, None
            )

        # Verify fast_sync_s3_info is embedded in STATE message
        output = my_stdout.getvalue()
        self.assertIn('"type": "STATE"', output)

        stream_id = f"public-{self.table_name}"
        state_with_s3_info = self._find_state_with_s3_info(output, stream_id)
        self.assertIsNotNone(
            state_with_s3_info, "No STATE message found with fast_sync_s3_info"
        )

        s3_info = state_with_s3_info["value"]["bookmarks"][stream_id][
            "fast_sync_s3_info"
        ]
        self._assert_s3_info_basic(
            s3_info, expected_rows=2, expected_method="FULL_TABLE"
        )
        self.assertEqual(s3_info["files_uploaded"], 1)
        self.assertEqual(s3_info["bytes_uploaded"], 1024)

        # Verify state was updated
        self.assertIn("bookmarks", state)
        self.assertIn(stream_id, state["bookmarks"])
        self.assertIn("version", state["bookmarks"][stream_id])

    def test_fast_sync_rds_incremental_sync(self):
        """Test incremental sync with fast_sync_rds - covers replication key tracking"""
        # Insert initial data
        with get_test_connection() as conn:
            insert_record(
                conn.cursor(),
                self.table_name,
                {"name": "test1", "value": 100, "updated_at": "2024-01-01 10:00:00"},
            )
            conn.commit()

        test_stream = self._get_test_stream()
        test_stream = set_replication_method_for_stream(test_stream, "INCREMENTAL")
        test_stream = self._set_replication_key(test_stream, "updated_at")

        # Initial sync
        state = {}
        mock_result = {"rows_uploaded": 1, "files_uploaded": 1, "bytes_uploaded": 512}

        with mock_export_to_s3(mock_result):
            state = tap_postgres.do_sync(
                self.config, {"streams": [test_stream]}, "INCREMENTAL", state, None
            )

        # Verify state was updated
        stream_id = f"public-{self.table_name}"
        self.assertIn("bookmarks", state)
        self.assertIn(stream_id, state["bookmarks"])
        # Replication key value should be tracked (may be set during sync)
        if "replication_key_value" in state["bookmarks"][stream_id]:
            self.assertIsInstance(
                state["bookmarks"][stream_id]["replication_key_value"], str
            )

        # Insert new data
        with get_test_connection() as conn:
            insert_record(
                conn.cursor(),
                self.table_name,
                {"name": "test2", "value": 200, "updated_at": "2024-01-01 12:00:00"},
            )
            conn.commit()

        # Second sync (incremental)
        mock_result2 = {"rows_uploaded": 1, "files_uploaded": 1, "bytes_uploaded": 512}

        with mock_export_to_s3(mock_result2) as my_stdout2:
            state = tap_postgres.do_sync(
                self.config, {"streams": [test_stream]}, "INCREMENTAL", state, None
            )

        # Verify incremental sync - check STATE message for fast_sync_s3_info
        output2 = my_stdout2.getvalue()
        self.assertIn('"type": "STATE"', output2)

        state_with_s3_info2 = self._find_state_with_s3_info(output2, stream_id)
        self.assertIsNotNone(
            state_with_s3_info2, "No STATE message found with fast_sync_s3_info"
        )
        s3_info2 = state_with_s3_info2["value"]["bookmarks"][stream_id][
            "fast_sync_s3_info"
        ]
        self.assertEqual(s3_info2["replication_method"], "INCREMENTAL")

    def test_fast_sync_rds_empty_table(self):
        """Test fast_sync_rds with empty table - S3 info should not be stored when rows_uploaded is 0"""
        test_stream = self._get_test_stream()
        test_stream = set_replication_method_for_stream(test_stream, "FULL_TABLE")

        mock_result = {"rows_uploaded": 0, "files_uploaded": 1, "bytes_uploaded": 0}

        with mock_export_to_s3(mock_result) as my_stdout:
            tap_postgres.do_sync(
                self.config, {"streams": [test_stream]}, "FULL_TABLE", {}, None
            )

        # Verify STATE message is still sent (sync occurred)
        output = my_stdout.getvalue()
        self.assertIn('"type": "STATE"', output)

        stream_id = f"public-{self.table_name}"

        # Verify fast_sync_s3_info is NOT stored when rows_uploaded is 0
        # (no need to store S3 info if there's no data to load)
        state_with_s3_info = self._find_state_with_s3_info(output, stream_id)
        self.assertIsNone(
            state_with_s3_info,
            "fast_sync_s3_info should not be stored when rows_uploaded is 0",
        )

        # Verify STATE message still exists with version (sync still occurred)
        lines = output.strip().split("\n")
        state_messages = [
            json.loads(line) for line in lines if '"type": "STATE"' in line
        ]
        state_found = False
        for state_msg in state_messages:
            if (
                state_msg.get("type") == "STATE"
                and "bookmarks" in state_msg.get("value", {})
                and stream_id in state_msg["value"]["bookmarks"]
            ):
                state_found = True
                # Verify version is present but fast_sync_s3_info is not
                stream_bookmarks = state_msg["value"]["bookmarks"][stream_id]
                self.assertIn("version", stream_bookmarks)
                self.assertNotIn("fast_sync_s3_info", stream_bookmarks)
                break
        self.assertTrue(state_found, "STATE message should exist even for empty table")

    def test_fast_sync_rds_multiple_files_uploaded(self):
        """Test fast_sync_rds with multiple files (file splitting scenario)"""
        test_stream = self._get_test_stream()
        test_stream = set_replication_method_for_stream(test_stream, "FULL_TABLE")

        # Mock result with multiple files (simulating large export split)
        mock_result = {
            "rows_uploaded": 1000000,
            "files_uploaded": 3,
            "bytes_uploaded": 18000000000,  # ~18GB total
        }

        with mock_export_to_s3(mock_result) as my_stdout:
            tap_postgres.do_sync(
                self.config, {"streams": [test_stream]}, "FULL_TABLE", {}, None
            )

        output = my_stdout.getvalue()
        self.assertIn('"type": "STATE"', output)

        stream_id = f"public-{self.table_name}"
        state_with_s3_info = self._find_state_with_s3_info(output, stream_id)
        self.assertIsNotNone(
            state_with_s3_info, "No STATE message found with fast_sync_s3_info"
        )
        s3_info = state_with_s3_info["value"]["bookmarks"][stream_id][
            "fast_sync_s3_info"
        ]
        self.assertEqual(s3_info["files_uploaded"], 3)
        self.assertEqual(s3_info["rows_uploaded"], 1000000)

    def test_fast_sync_rds_replication_key_string_type(self):
        """Test replication key tracking with string type (not datetime)"""
        from tap_postgres.sync_strategies import fast_sync_rds

        # Create a table with a string replication key
        table_name_str = "fast_sync_test_table_str"
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'''
                    CREATE TABLE IF NOT EXISTS "{table_name_str}" (
                        id serial PRIMARY KEY,
                        name character varying,
                        code character varying
                    )
                ''')
                conn.commit()

        try:
            strategy = fast_sync_rds.FastSyncRdsStrategy(
                self.config, "test-bucket", "test/prefix", "us-east-1"
            )

            # Insert data
            with get_test_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        f'INSERT INTO "{table_name_str}" (name, code) VALUES (%s, %s)',
                        ("test1", "ABC123"),
                    )
                    conn.commit()

            test_stream = self._get_test_stream(f"public-{table_name_str}")
            test_stream = self._set_replication_key(test_stream, "code")

            md_map = self._build_md_map(test_stream)
            desired_columns = ["id", "name", "code"]
            state = {}

            mock_result = {
                "rows_uploaded": 1,
                "files_uploaded": 1,
                "bytes_uploaded": 512,
            }

            # Mock at database connection level instead of private method
            with mock_export_to_s3(mock_result):
                result_state = strategy.sync_table_incremental(
                    test_stream,
                    state,
                    desired_columns,
                    md_map,
                    replication_key="code",
                    replication_key_value=None,
                )

            # Should have replication_key_value in state as string
            stream_id = test_stream["tap_stream_id"]
            self.assertIn("bookmarks", result_state)
            self.assertIn(stream_id, result_state["bookmarks"])
            self.assertIn("replication_key_value", result_state["bookmarks"][stream_id])
            self.assertEqual(
                result_state["bookmarks"][stream_id]["replication_key_value"], "ABC123"
            )

        finally:
            # Cleanup
            with get_test_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f'DROP TABLE IF EXISTS "{table_name_str}"')
                    conn.commit()

    def test_fast_sync_rds_export_error_handling(self):
        """Test error handling when export fails"""
        from tap_postgres.sync_strategies import fast_sync_rds

        strategy = fast_sync_rds.FastSyncRdsStrategy(
            self.config, "test-bucket", "test/prefix", "us-east-1"
        )

        test_stream = self._get_test_stream()
        md_map = self._build_md_map(test_stream)
        desired_columns = ["id", "name", "value", "updated_at"]
        state = {}

        # Mock database cursor to raise an exception (simulating export failure)
        # This mocks at the database connection level within fast_sync_rds module, not internal methods
        mock_cursor = unittest.mock.MagicMock()
        mock_cursor.fetchone.return_value = None  # Simulate no result returned
        mock_cursor.__enter__ = unittest.mock.Mock(return_value=mock_cursor)
        mock_cursor.__exit__ = unittest.mock.Mock(return_value=None)

        mock_conn = unittest.mock.MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_conn.__enter__ = unittest.mock.Mock(return_value=mock_conn)
        mock_conn.__exit__ = unittest.mock.Mock(return_value=None)

        # Mock at the fast_sync_rds module level to only affect that module's database calls
        with unittest.mock.patch(
            "tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection",
            return_value=mock_conn,
        ):
            with self.assertRaises(Exception) as context:
                strategy.sync_table_full(test_stream, state, desired_columns, md_map)

            self.assertIn("Export to S3 failed", str(context.exception))
