"""
Unit tests for sync_strategies.common module
"""

from unittest import TestCase
from unittest.mock import patch

import tap_postgres.sync_strategies.common as sync_common


class TestCommon(TestCase):
    """Test Cases for common sync strategies utilities"""

    def test_escape_sql_string(self):
        """Test escaping single quotes in SQL strings"""
        self.assertEqual(sync_common.escape_sql_string("test"), "test")
        self.assertEqual(sync_common.escape_sql_string("test'value"), "test''value")
        self.assertEqual(sync_common.escape_sql_string("'quoted'"), "''quoted''")
        self.assertEqual(sync_common.escape_sql_string(""), "")

    def test_get_select_latest_sql(self):
        """Test building SELECT query for latest row"""
        params = {
            "escaped_columns": ["id", "name"],
            "replication_key": "updated_at",
            "schema_name": "public",
            "table_name": "users",
        }
        sql = sync_common.get_select_latest_sql(params)

        self.assertIn("SELECT", sql)
        self.assertIn("id,name", sql)
        self.assertIn("FROM", sql)
        self.assertIn('"public"', sql)
        self.assertIn('"users"', sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("updated_at", sql)
        self.assertIn("DESC LIMIT 1", sql)

    def test_get_query_for_replication_data_full_table(self):
        """Test building query for full table sync (no replication key)"""
        params = {
            "escaped_columns": ["id", "name"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": None,
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("SELECT", sql)
        self.assertIn("id,name", sql)
        self.assertIn("FROM", sql)
        self.assertIn('"public"', sql)
        self.assertIn('"users"', sql)
        self.assertNotIn("WHERE", sql)
        self.assertNotIn("ORDER BY", sql)

    def test_get_query_for_replication_data_incremental(self):
        """Test building query for incremental sync with replication_key_value"""
        params = {
            "escaped_columns": ["id", "name"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": "updated_at",
            "replication_key_value": "2025-01-01 00:00:00",
            "replication_key_sql_datatype": "timestamp",
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("SELECT", sql)
        self.assertIn("FROM", sql)
        self.assertIn("WHERE", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("updated_at", sql)
        self.assertIn("ASC", sql)
        self.assertIn("'2025-01-01 00:00:00'", sql)
        self.assertIn("timestamp", sql)

    def test_get_query_for_replication_data_without_replication_key_value(self):
        """Test building query with replication_key but no replication_key_value"""
        params = {
            "escaped_columns": ["id", "name"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": "updated_at",
            "replication_key_value": None,
            "replication_key_sql_datatype": "timestamp",
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("SELECT", sql)
        self.assertIn("FROM", sql)
        self.assertNotIn("WHERE", sql)
        self.assertIn("ORDER BY", sql)
        self.assertIn("updated_at", sql)

    def test_get_query_for_replication_data_escapes_quotes(self):
        """Test that query properly escapes single quotes to prevent SQL injection"""
        params = {
            "escaped_columns": ["id", "name"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": "updated_at",
            "replication_key_value": "2025-01-01' OR '1'='1",  # SQL injection attempt
            "replication_key_sql_datatype": "timestamp",
        }
        sql = sync_common.get_query_for_replication_data(params)

        # Should escape single quotes
        self.assertIn("''", sql)  # Escaped quotes
        self.assertNotIn("' OR '1'='1", sql)  # Should be escaped

    def test_get_query_for_replication_data_with_look_back_n_seconds(self):
        """Test building query with look_back_n_seconds for timestamp types"""
        params = {
            "escaped_columns": ["id", "name"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": "updated_at",
            "replication_key_value": "2025-01-01 00:00:00",
            "replication_key_sql_datatype": "timestamp with time zone",
            "look_back_n_seconds": 300,
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("WHERE", sql)
        self.assertIn("- interval", sql)
        self.assertIn("300 seconds", sql)

    def test_get_query_for_replication_data_with_skip_last_n_seconds(self):
        """Test building query with skip_last_n_seconds"""
        params = {
            "escaped_columns": ["id", "name"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": "updated_at",
            "replication_key_value": "2025-01-01 00:00:00",
            "replication_key_sql_datatype": "timestamp with time zone",
            "skip_last_n_seconds": 60,
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("WHERE", sql)
        self.assertIn("<=", sql)
        self.assertIn("NOW()", sql)
        self.assertIn("- interval", sql)
        self.assertIn("60 seconds", sql)

    def test_get_query_for_replication_data_with_both_look_back_and_skip(self):
        """Test building query with both look_back_n_seconds and skip_last_n_seconds"""
        params = {
            "escaped_columns": ["id", "name"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": "updated_at",
            "replication_key_value": "2025-01-01 00:00:00",
            "replication_key_sql_datatype": "timestamp with time zone",
            "look_back_n_seconds": 300,
            "skip_last_n_seconds": 60,
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("WHERE", sql)
        self.assertIn("AND", sql)
        self.assertIn(">=", sql)
        self.assertIn("<=", sql)

    def test_get_query_for_replication_data_with_recover_mappings(self):
        """Test building query with recover_mappings"""
        recover_mappings = {"public-users": ["2025-01-01", "2025-01-02"]}
        params = {
            "escaped_columns": ["id", "name"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": "updated_at",
            "replication_key_value": "2025-01-01 00:00:00",
            "replication_key_sql_datatype": "timestamp",
            "recover_mappings": recover_mappings,
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("WHERE", sql)
        self.assertIn("::DATE", sql)
        self.assertIn("'2025-01-01'", sql)
        self.assertIn("'2025-01-02'", sql)
        self.assertIn("in (", sql)

    def test_get_query_for_replication_data_look_back_only_for_timestamp(self):
        """Test that look_back_n_seconds only applies to timestamp types, not integers"""
        params = {
            "escaped_columns": ["id", "name"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": "id",
            "replication_key_value": "100",
            "replication_key_sql_datatype": "integer",
            "look_back_n_seconds": 300,
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("WHERE", sql)
        self.assertNotIn("- interval", sql)

    def test_get_query_for_replication_data_with_limit(self):
        """Test building query with limit"""
        params = {
            "escaped_columns": ["id"],
            "schema_name": "public",
            "table_name": "users",
            "limit": 1000,
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("LIMIT 1000", sql)

    def test_get_query_for_replication_data_with_skip_order(self):
        """Test building query with skip_order=True"""
        params = {
            "escaped_columns": ["id"],
            "schema_name": "public",
            "table_name": "users",
            "replication_key": "updated_at",
            "skip_order": True,
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertNotIn("ORDER BY", sql)

    def test_get_query_for_replication_data_pg_speedup_trick(self):
        """Test that query uses pg_speedup_trick subquery"""
        params = {
            "escaped_columns": ["id"],
            "schema_name": "public",
            "table_name": "users",
        }
        sql = sync_common.get_query_for_replication_data(params)

        self.assertIn("pg_speedup_trick", sql)
        self.assertIn("SELECT *", sql)
        self.assertIn("FROM (", sql)

    @patch("sys.stdout")
    def test_write_message(self, mock_stdout):
        """Test write_message function writes to stdout"""
        test_message = {"type": "TEST", "data": "test_value"}
        sync_common.write_message(test_message)

        # Verify sys.stdout.write was called
        mock_stdout.write.assert_called_once()
        json_str = mock_stdout.write.call_args[0][0]

        # Verify the message was JSON serialized and is valid JSON
        import json

        self.assertTrue(json_str.endswith("\n"))

        # Validate that the output is valid JSON
        parsed_message = json.loads(json_str)
        self.assertEqual(parsed_message["type"], "TEST")
        self.assertEqual(parsed_message["data"], "test_value")

        # Verify sys.stdout.flush was called
        mock_stdout.flush.assert_called_once()
