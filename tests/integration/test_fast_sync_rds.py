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
    drop_table
)


def do_not_dump_catalog(catalog):
    pass


tap_postgres.dump_catalog = do_not_dump_catalog


@contextlib.contextmanager
def mock_export_to_s3(mock_result):
    """
    Context manager that mocks _export_to_s3 and captures stdout.

    Args:
        mock_result: Dictionary with 'rows_uploaded', 'files_uploaded', 'bytes_uploaded'

    Yields:
        io.StringIO: StringIO object containing captured stdout
    """
    my_stdout = io.StringIO()
    with contextlib.redirect_stdout(my_stdout):
        with unittest.mock.patch('tap_postgres.sync_strategies.fast_sync_rds.FastSyncRdsStrategy._export_to_s3',
                                 return_value=mock_result):
            yield my_stdout


class TestFastSyncRds(unittest.TestCase):
    table_name = None
    maxDiff = None

    @classmethod
    def setUpClass(cls) -> None:
        cls.table_name = 'fast_sync_test_table'
        table_spec = {
            "columns": [
                {"name": "id", "type": "serial", "primary_key": True},
                {"name": "name", "type": "character varying"},
                {"name": "value", "type": "integer"},
                {"name": "updated_at", "type": "timestamp without time zone"},
            ],
            "name": cls.table_name
        }

        ensure_test_table(table_spec)
        cls.config = get_test_connection_config()
        # Add fast_sync_rds configuration
        cls.config['fast_sync_rds'] = True
        cls.config['fast_sync_rds_s3_bucket'] = 'test-bucket'
        cls.config['fast_sync_rds_s3_prefix'] = 'test/prefix'
        cls.config['fast_sync_rds_s3_region'] = 'us-east-1'

    @classmethod
    def tearDownClass(cls) -> None:
        drop_table(cls.table_name)

    def setUp(self):
        # Clean up table before each test
        with get_test_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f'TRUNCATE TABLE "{self.table_name}"')

    def test_fast_sync_rds_full_table_discovery(self):
        """Test that fast_sync_rds streams are discovered correctly"""
        streams = tap_postgres.do_discovery(self.config)

        test_stream = [s for s in streams if s['tap_stream_id'] == f'public-{self.table_name}'][0]
        self.assertIsNotNone(test_stream)
        self.assertEqual(test_stream['table_name'], self.table_name)

    def test_fast_sync_rds_full_table_sync(self):
        """Test full table sync with fast_sync_rds - covers message structure, S3 path, version, etc."""
        # Insert test data
        with get_test_connection() as conn:
            insert_record(conn.cursor(), self.table_name, {
                'name': 'test1',
                'value': 100,
                'updated_at': '2024-01-01 10:00:00'
            })
            insert_record(conn.cursor(), self.table_name, {
                'name': 'test2',
                'value': 200,
                'updated_at': '2024-01-01 11:00:00'
            })
            conn.commit()

        streams = tap_postgres.do_discovery(self.config)
        test_stream = [s for s in streams if s['tap_stream_id'] == f'public-{self.table_name}'][0]
        test_stream = set_replication_method_for_stream(test_stream, 'FULL_TABLE')

        mock_result = {
            'rows_uploaded': 2,
            'files_uploaded': 1,
            'bytes_uploaded': 1024
        }

        with mock_export_to_s3(mock_result) as my_stdout:
            state = tap_postgres.do_sync(
                self.config,
                {'streams': [test_stream]},
                'FULL_TABLE',
                {},
                None
            )

        # Verify FAST_SYNC_RDS_S3_INFO message was emitted with correct structure
        output = my_stdout.getvalue()
        self.assertIn('FAST_SYNC_RDS_S3_INFO', output)

        lines = output.strip().split('\n')
        s3_info_messages = [json.loads(line) for line in lines if 'FAST_SYNC_RDS_S3_INFO' in line]

        self.assertGreater(len(s3_info_messages), 0)
        message = s3_info_messages[0]

        # Verify message structure
        self.assertEqual(message['type'], 'FAST_SYNC_RDS_S3_INFO')
        self.assertEqual(message['stream'], f'public-{self.table_name}')
        self.assertEqual(message['s3_bucket'], 'test-bucket')
        self.assertIn('test/prefix', message['s3_path'])
        self.assertIn('public-fast_sync_test_table', message['s3_path'])
        self.assertTrue(message['s3_path'].endswith('.csv'))
        self.assertEqual(message['s3_region'], 'us-east-1')
        self.assertEqual(message['rows_uploaded'], 2)
        self.assertEqual(message['files_uploaded'], 1)
        self.assertEqual(message['bytes_uploaded'], 1024)
        self.assertEqual(message['replication_method'], 'FULL_TABLE')
        self.assertIn('time_extracted', message)
        self.assertIsInstance(message['time_extracted'], str)
        self.assertIn('version', message)
        self.assertIsInstance(message['version'], int)

        # Verify state was updated
        self.assertIn('bookmarks', state)
        stream_id = f'public-{self.table_name}'
        self.assertIn(stream_id, state['bookmarks'])
        self.assertIn('version', state['bookmarks'][stream_id])

    def test_fast_sync_rds_incremental_sync(self):
        """Test incremental sync with fast_sync_rds - covers replication key tracking"""
        # Insert initial data
        with get_test_connection() as conn:
            insert_record(conn.cursor(), self.table_name, {
                'name': 'test1',
                'value': 100,
                'updated_at': '2024-01-01 10:00:00'
            })
            conn.commit()

        streams = tap_postgres.do_discovery(self.config)
        test_stream = [s for s in streams if s['tap_stream_id'] == f'public-{self.table_name}'][0]
        test_stream = set_replication_method_for_stream(test_stream, 'INCREMENTAL')
        # Set replication key
        for md in test_stream['metadata']:
            if md['breadcrumb'] == []:
                md['metadata']['replication-key'] = 'updated_at'
                break

        # Initial sync
        state = {}
        mock_result = {
            'rows_uploaded': 1,
            'files_uploaded': 1,
            'bytes_uploaded': 512
        }

        with mock_export_to_s3(mock_result) as my_stdout:
            state = tap_postgres.do_sync(
                self.config,
                {'streams': [test_stream]},
                'INCREMENTAL',
                state,
                None
            )

        # Verify state was updated
        self.assertIn('bookmarks', state)
        stream_id = f'public-{self.table_name}'
        self.assertIn(stream_id, state['bookmarks'])
        # Replication key value should be tracked (may be set during sync)
        if 'replication_key_value' in state['bookmarks'][stream_id]:
            self.assertIsInstance(state['bookmarks'][stream_id]['replication_key_value'], str)

        # Insert new data
        with get_test_connection() as conn:
            insert_record(conn.cursor(), self.table_name, {
                'name': 'test2',
                'value': 200,
                'updated_at': '2024-01-01 12:00:00'
            })
            conn.commit()

        # Second sync (incremental)
        mock_result2 = {
            'rows_uploaded': 1,
            'files_uploaded': 1,
            'bytes_uploaded': 512
        }

        with mock_export_to_s3(mock_result2) as my_stdout2:
            state = tap_postgres.do_sync(
                self.config,
                {'streams': [test_stream]},
                'INCREMENTAL',
                state,
                None
            )

        # Verify incremental sync message
        output2 = my_stdout2.getvalue()
        self.assertIn('FAST_SYNC_RDS_S3_INFO', output2)

        lines2 = output2.strip().split('\n')
        s3_info_messages2 = [json.loads(line) for line in lines2 if 'FAST_SYNC_RDS_S3_INFO' in line]

        self.assertGreater(len(s3_info_messages2), 0)
        message2 = s3_info_messages2[0]
        self.assertEqual(message2['replication_method'], 'INCREMENTAL')

    def test_fast_sync_rds_empty_table(self):
        """Test fast_sync_rds with empty table"""
        streams = tap_postgres.do_discovery(self.config)
        test_stream = [s for s in streams if s['tap_stream_id'] == f'public-{self.table_name}'][0]
        test_stream = set_replication_method_for_stream(test_stream, 'FULL_TABLE')

        mock_result = {
            'rows_uploaded': 0,
            'files_uploaded': 1,
            'bytes_uploaded': 0
        }

        with mock_export_to_s3(mock_result) as my_stdout:
            state = tap_postgres.do_sync(
                self.config,
                {'streams': [test_stream]},
                'FULL_TABLE',
                {},
                None
            )

        # Verify message was still emitted even with 0 rows
        output = my_stdout.getvalue()
        self.assertIn('FAST_SYNC_RDS_S3_INFO', output)

        lines = output.strip().split('\n')
        s3_info_messages = [json.loads(line) for line in lines if 'FAST_SYNC_RDS_S3_INFO' in line]

        self.assertGreater(len(s3_info_messages), 0)
        message = s3_info_messages[0]
        self.assertEqual(message['rows_uploaded'], 0)

    def test_fast_sync_rds_multiple_files_uploaded(self):
        """Test fast_sync_rds with multiple files (file splitting scenario)"""
        streams = tap_postgres.do_discovery(self.config)
        test_stream = [s for s in streams if s['tap_stream_id'] == f'public-{self.table_name}'][0]
        test_stream = set_replication_method_for_stream(test_stream, 'FULL_TABLE')

        # Mock result with multiple files (simulating large export split)
        mock_result = {
            'rows_uploaded': 1000000,
            'files_uploaded': 3,
            'bytes_uploaded': 18000000000  # ~18GB total
        }

        with mock_export_to_s3(mock_result) as my_stdout:
            state = tap_postgres.do_sync(
                self.config,
                {'streams': [test_stream]},
                'FULL_TABLE',
                {},
                None
            )

        output = my_stdout.getvalue()
        self.assertIn('FAST_SYNC_RDS_S3_INFO', output)

        lines = output.strip().split('\n')
        s3_info_messages = [json.loads(line) for line in lines if 'FAST_SYNC_RDS_S3_INFO' in line]

        self.assertGreater(len(s3_info_messages), 0)
        message = s3_info_messages[0]
        self.assertEqual(message['files_uploaded'], 3)
        self.assertEqual(message['rows_uploaded'], 1000000)

    def test_fast_sync_rds_replication_key_string_type(self):
        """Test replication key tracking with string type (not datetime)"""
        from tap_postgres.sync_strategies import fast_sync_rds

        # Create a table with a string replication key
        table_name_str = 'fast_sync_test_table_str'
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
                self.config,
                'test-bucket',
                'test/prefix',
                'us-east-1'
            )

            # Insert data
            with get_test_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute(f'INSERT INTO "{table_name_str}" (name, code) VALUES (%s, %s)', ('test1', 'ABC123'))
                    conn.commit()

            streams = tap_postgres.do_discovery(self.config)
            test_stream = [s for s in streams if s['tap_stream_id'] == f'public-{table_name_str}'][0]

            # Set replication key in metadata
            for md in test_stream['metadata']:
                if md['breadcrumb'] == []:
                    md['metadata']['replication-key'] = 'code'

            md_map = {}
            for md in test_stream['metadata']:
                md_map[md['breadcrumb']] = md['metadata']

            desired_columns = ['id', 'name', 'code']
            state = {}

            mock_result = {
                'rows_uploaded': 1,
                'files_uploaded': 1,
                'bytes_uploaded': 512
            }

            with unittest.mock.patch('tap_postgres.sync_strategies.fast_sync_rds.FastSyncRdsStrategy._export_to_s3',
                                     return_value=mock_result):
                result_state = strategy.sync_table_incremental(
                    test_stream,
                    state,
                    desired_columns,
                    md_map,
                    replication_key='code',
                    replication_key_value=None
                )

            # Should have replication_key_value in state as string
            stream_id = test_stream['tap_stream_id']
            self.assertIn('bookmarks', result_state)
            self.assertIn(stream_id, result_state['bookmarks'])
            self.assertIn('replication_key_value', result_state['bookmarks'][stream_id])
            self.assertEqual(result_state['bookmarks'][stream_id]['replication_key_value'], 'ABC123')

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
            self.config,
            'test-bucket',
            'test/prefix',
            'us-east-1'
        )

        streams = tap_postgres.do_discovery(self.config)
        test_stream = [s for s in streams if s['tap_stream_id'] == f'public-{self.table_name}'][0]

        md_map = {}
        for md in test_stream['metadata']:
            md_map[md['breadcrumb']] = md['metadata']

        desired_columns = ['id', 'name', 'value', 'updated_at']
        state = {}

        # Mock _export_to_s3 to raise an exception
        with unittest.mock.patch('tap_postgres.sync_strategies.fast_sync_rds.FastSyncRdsStrategy._export_to_s3',
                                 side_effect=Exception("Export to S3 failed: No result returned")):
            with self.assertRaises(Exception) as context:
                strategy.sync_table_full(
                    test_stream,
                    state,
                    desired_columns,
                    md_map
                )

            self.assertIn('Export to S3 failed', str(context.exception))
