"""
Unit tests for fast_sync_rds strategy
"""
from unittest import TestCase
from unittest.mock import patch, MagicMock
from tap_postgres.sync_strategies import fast_sync_rds


class TestFastSyncRds(TestCase):
    """Test Cases for FastSyncRdsStrategy"""

    def setUp(self) -> None:
        """Set up test fixtures"""
        self.conn_config = {
            'host': 'test-host',
            'dbname': 'test_db',
            'user': 'test_user',
            'password': 'test_pass',
            'port': 5432,
        }
        self.s3_bucket = 'test-bucket'
        self.s3_prefix = 'test/prefix'
        self.s3_region = 'us-east-1'
        self.fast_sync_rds_strategy = fast_sync_rds.FastSyncRdsStrategy(
            self.conn_config, self.s3_bucket, self.s3_prefix, self.s3_region
        )

        self.stream = {
            'tap_stream_id': 'test_schema-test_table',
            'stream': 'test_table',
            'table_name': 'test_table'
        }
        self.md_map = {
            (): {'schema-name': 'test_schema'},
            ('properties', 'id'): {'sql-datatype': 'integer'},
            ('properties', 'name'): {'sql-datatype': 'varchar'}
        }
        self.state = {
            'bookmarks': {
                self.stream['tap_stream_id']: {
                    'version': 1234567890
                }
            }
        }
        self.desired_columns = ['id', 'name']

    def _create_mock_export_result(self, rows=50, files=1, bytes_uploaded=2500):
        """Helper to create mock export result"""
        mock_result = MagicMock()
        mock_result.__getitem__.side_effect = lambda key: {
            'rows_uploaded': rows,
            'files_uploaded': files,
            'bytes_uploaded': bytes_uploaded
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
        execute_calls = [execute_call[0][0] for execute_call in mock_cursor.execute.call_args_list]
        for query in execute_calls:
            if 'aws_s3.query_export_to_s3' in query:
                return query
        return None

    def _assert_s3_info_message(self, message, expected_rows=50, expected_method='FULL_TABLE', expected_bytes=2500):
        """Helper to assert S3 info message structure"""
        self.assertEqual(message['type'], 'FAST_SYNC_RDS_S3_INFO')
        self.assertEqual(message['stream'], 'test_schema-test_table')
        self.assertEqual(message['s3_bucket'], 'test-bucket')
        self.assertIn('s3_path', message)
        self.assertRegex(message['s3_path'], r'^test/prefix/test_schema-test_table/.+_.+\.csv$')
        self.assertEqual(message['s3_region'], 'us-east-1')
        self.assertEqual(message['rows_uploaded'], expected_rows)
        self.assertEqual(message['files_uploaded'], 1)
        self.assertEqual(message['bytes_uploaded'], expected_bytes)
        self.assertIn('time_extracted', message)
        self.assertIn('version', message)
        self.assertEqual(message['replication_method'], expected_method)

    def test_init(self):
        """Test FastSyncRdsStrategy initialization"""
        strategy = fast_sync_rds.FastSyncRdsStrategy(
            self.conn_config, self.s3_bucket, self.s3_prefix, self.s3_region
        )
        self.assertEqual(strategy.s3_bucket, 'test-bucket')
        self.assertEqual(strategy.s3_prefix, 'test/prefix')
        self.assertEqual(strategy.s3_region, 'us-east-1')

    @patch('tap_postgres.sync_strategies.fast_sync_rds.sync_common.write_message')
    @patch('tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection')
    def test_sync_table_full(self, mock_open_conn, mock_write_message):
        """Test sync_table_full - verifies S3 path, message structure, and metadata columns"""
        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        state = self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map
        )

        # Verify state was updated
        self.assertIsNotNone(state)
        self.assertIn('bookmarks', state)

        # Verify S3 info message was sent
        mock_write_message.assert_called_once()
        message = mock_write_message.call_args[0][0]
        self._assert_s3_info_message(message, expected_method='FULL_TABLE')

        # Verify S3 path format (tests _generate_s3_path indirectly)
        self.assertRegex(message['s3_path'], r'^test/prefix/test_schema-test_table/.+_.+\.csv$')

        # Verify metadata columns are in the query (tests _prepend_metadata_columns indirectly)
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertIn('_SDC_BATCHED_AT', export_query)
        self.assertIn('_SDC_DELETED_AT', export_query)
        self.assertIn('_SDC_EXTRACTED_AT', export_query)

        # Verify metadata columns order
        batched_pos = export_query.find('_SDC_BATCHED_AT')
        deleted_pos = export_query.find('_SDC_DELETED_AT')
        extracted_pos = export_query.find('_SDC_EXTRACTED_AT')
        id_pos = export_query.find('"id"')

        self.assertGreater(batched_pos, 0)
        self.assertGreater(deleted_pos, batched_pos)
        self.assertGreater(extracted_pos, deleted_pos)
        if id_pos > 0:
            self.assertGreater(id_pos, extracted_pos)

    @patch('tap_postgres.sync_strategies.fast_sync_rds.sync_common.write_message')
    @patch('tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection')
    def test_sync_table_full_no_prefix(self, mock_open_conn, mock_write_message):
        """Test sync_table_full with empty prefix - verifies S3 path generation"""
        strategy = fast_sync_rds.FastSyncRdsStrategy(
            self.conn_config, 'test-bucket', '', 'us-east-1'
        )

        mock_conn, _ = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map
        )

        # Verify S3 path doesn't start with double slashes
        mock_write_message.assert_called_once()
        message = mock_write_message.call_args[0][0]
        s3_path = message['s3_path']
        self.assertFalse(s3_path.startswith('/'))
        self.assertRegex(s3_path, r'^test_schema-test_table/.+_.+\.csv$')

    @patch('tap_postgres.sync_strategies.fast_sync_rds.sync_common.write_message')
    @patch('tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection')
    def test_sync_table_full_metadata_columns_disabled(self, mock_open_conn, _mock_write_message):
        """Test sync_table_full excludes metadata columns when setting is disabled"""
        conn_config_no_metadata = self.conn_config.copy()
        conn_config_no_metadata['fast_sync_rds_add_metadata_columns'] = False
        strategy = fast_sync_rds.FastSyncRdsStrategy(
            conn_config_no_metadata, self.s3_bucket, self.s3_prefix, self.s3_region
        )

        mock_conn, mock_cursor = self._setup_mock_connection()
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map
        )

        # Verify metadata columns are NOT in the query
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertNotIn('_SDC_BATCHED_AT', export_query)
        self.assertNotIn('_SDC_DELETED_AT', export_query)
        self.assertNotIn('_SDC_EXTRACTED_AT', export_query)

    @patch('tap_postgres.sync_strategies.fast_sync_rds.sync_common.write_message')
    @patch('tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection')
    @patch('tap_postgres.sync_strategies.fast_sync_rds.singer.get_bookmark')
    def test_sync_table_incremental(self, mock_get_bookmark, mock_open_conn, mock_write_message):
        """Test sync_table_incremental - verifies WHERE clause, replication key tracking, and message"""
        mock_get_bookmark.side_effect = lambda state, stream_id, key: {
            'version': 1234567890,
            'replication_key_value': '100'
        }.get(key)

        # Add replication key to metadata
        md_map_with_key = self.md_map.copy()
        md_map_with_key[()]['replication-key'] = 'id'
        md_map_with_key[('properties', 'id')] = {'sql-datatype': 'integer'}

        # Mock replication key result
        mock_replication_key_result = MagicMock()
        mock_replication_key_result.__getitem__.return_value = '200'

        mock_conn, mock_cursor = self._setup_mock_connection(
            export_result=self._create_mock_export_result(rows=25, bytes_uploaded=1250),
            replication_key_result=mock_replication_key_result
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        state = self.fast_sync_rds_strategy.sync_table_incremental(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=md_map_with_key,
            replication_key='id',
            replication_key_value='100'
        )

        # Verify WHERE clause was included in query (tests query building indirectly)
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        self.assertIn('WHERE', export_query)
        self.assertIn('pg_speedup_trick', export_query)
        self.assertIn('id', export_query)

        # Verify S3 info message
        mock_write_message.assert_called_once()
        message = mock_write_message.call_args[0][0]
        self._assert_s3_info_message(message, expected_rows=25, expected_method='INCREMENTAL', expected_bytes=1250)

        # Verify replication_key_value was tracked in state
        self.assertIn('replication_key_value', state['bookmarks'][self.stream['tap_stream_id']])

    @patch('tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection')
    @patch('tap_postgres.sync_strategies.fast_sync_rds.singer.get_bookmark')
    def test_sync_table_incremental_sql_injection_protection(self, mock_get_bookmark, mock_open_conn):
        """Test sync_table_incremental properly escapes SQL injection attempts"""
        mock_get_bookmark.side_effect = lambda state, stream_id, key: {
            'version': 1234567890,
            'replication_key_value': "100' OR '1'='1"
        }.get(key)

        md_map_with_key = self.md_map.copy()
        md_map_with_key[()]['replication-key'] = 'id'
        md_map_with_key[('properties', 'id')] = {'sql-datatype': 'integer'}

        mock_replication_key_result = MagicMock()
        mock_replication_key_result.__getitem__.return_value = '200'

        mock_conn, mock_cursor = self._setup_mock_connection(
            replication_key_result=mock_replication_key_result
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self.fast_sync_rds_strategy.sync_table_incremental(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=md_map_with_key,
            replication_key='id',
            replication_key_value="100' OR '1'='1"
        )

        # Verify quotes are escaped in the query
        export_query = self._extract_export_query(mock_cursor)
        self.assertIsNotNone(export_query)
        # Escaped quotes should appear as ''
        self.assertIn("''", export_query)

    @patch('tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection')
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
                md_map=self.md_map
            )

        self.assertIn('Export to S3 failed', str(context.exception))

    @patch('tap_postgres.sync_strategies.fast_sync_rds.sync_common.write_message')
    @patch('tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection')
    def test_sync_table_full_multiple_files(self, mock_open_conn, mock_write_message):
        """Test sync_table_full with multiple files (file splitting scenario)"""
        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(rows=1000000, files=3, bytes_uploaded=18000000000)
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map
        )

        mock_write_message.assert_called_once()
        message = mock_write_message.call_args[0][0]
        self.assertEqual(message['files_uploaded'], 3)
        self.assertEqual(message['rows_uploaded'], 1000000)
        self.assertEqual(message['bytes_uploaded'], 18000000000)

    @patch('tap_postgres.sync_strategies.fast_sync_rds.sync_common.write_message')
    @patch('tap_postgres.sync_strategies.fast_sync_rds.post_db.open_connection')
    def test_sync_table_full_empty_table(self, mock_open_conn, mock_write_message):
        """Test sync_table_full with empty table (0 rows)"""
        mock_conn, _ = self._setup_mock_connection(
            export_result=self._create_mock_export_result(rows=0, bytes_uploaded=0)
        )
        mock_open_conn.return_value.__enter__.return_value = mock_conn

        self.fast_sync_rds_strategy.sync_table_full(
            stream=self.stream,
            state=self.state,
            desired_columns=self.desired_columns,
            md_map=self.md_map
        )

        # Verify message was still sent even with 0 rows
        mock_write_message.assert_called_once()
        message = mock_write_message.call_args[0][0]
        self.assertEqual(message['rows_uploaded'], 0)
        self.assertEqual(message['bytes_uploaded'], 0)
