"""
Unit tests for ParquetWriter
"""

from unittest import TestCase
from unittest.mock import patch, MagicMock

import pyarrow as pa

from tap_postgres.parquet_writer import (
    ParquetWriter,
    DEFAULT_COMPRESSION,
    DEFAULT_COMPRESSION_LEVEL,
)


class TestParquetWriter(TestCase):
    """Test Cases for ParquetWriter"""

    def setUp(self):
        """Set up test fixtures"""
        self.s3_bucket = "test-bucket"
        self.s3_region = "us-east-1"
        with patch("tap_postgres.parquet_writer.pafs.S3FileSystem"):
            self.parquet_writer = ParquetWriter(self.s3_bucket, self.s3_region)
        # Replace filesystem with a mock for testing
        self.parquet_writer.filesystem = MagicMock()

    def _create_md_map(self, **column_types):
        """Helper to create metadata map from column type mappings"""
        md_map = {}
        for col_name, sql_type in column_types.items():
            md_map[("properties", col_name)] = {"sql-datatype": sql_type}
        return md_map

    def _create_simple_schema(self):
        """Helper to create a simple two-column schema"""
        return pa.schema(
            [
                pa.field("id", pa.int32(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ]
        )

    def _create_record_batch(self, num_rows, schema=None):
        """Helper to create a real PyArrow record batch"""
        if schema is None:
            schema = self._create_simple_schema()

        # Create arrays matching the schema
        arrays = []
        for field in schema:
            if field.type == pa.int32():
                arrays.append(pa.array(list(range(num_rows)), type=pa.int32()))
            elif field.type == pa.string():
                arrays.append(
                    pa.array([f"value_{i}" for i in range(num_rows)], type=pa.string())
                )
            elif field.type == pa.int64():
                arrays.append(pa.array(list(range(num_rows)), type=pa.int64()))
            elif field.type == pa.int16():
                arrays.append(pa.array(list(range(num_rows)), type=pa.int16()))
            else:
                # Default to string for unknown types
                arrays.append(
                    pa.array([f"value_{i}" for i in range(num_rows)], type=pa.string())
                )
        return pa.RecordBatch.from_arrays(arrays, schema=schema)

    def _create_mock_csv_reader(self, batches):
        """Helper to create a mock CSV reader with batches"""
        mock_reader = MagicMock()
        mock_reader.__iter__ = MagicMock(return_value=iter(batches))
        mock_reader.__enter__ = MagicMock(return_value=mock_reader)
        mock_reader.__exit__ = MagicMock(return_value=None)
        return mock_reader

    def _setup_mock_file_and_writer(self, mock_parquet_writer_class):
        """Helper to set up mock file and Parquet writer"""
        mock_file = MagicMock()
        mock_file.__enter__ = MagicMock(return_value=mock_file)
        mock_file.__exit__ = MagicMock(return_value=None)
        self.parquet_writer.filesystem.open_input_file.return_value = mock_file

        mock_writer = MagicMock()
        mock_writer.__enter__ = MagicMock(return_value=mock_writer)
        mock_writer.__exit__ = MagicMock(return_value=None)
        mock_parquet_writer_class.return_value = mock_writer

        return mock_writer

    def _setup_mock_conversion(self, mock_open_csv, mock_parquet_writer_class, batches):
        """Helper to set up mocks for conversion tests"""
        mock_reader = self._create_mock_csv_reader(batches)
        mock_open_csv.return_value = mock_reader
        return self._setup_mock_file_and_writer(mock_parquet_writer_class)

    @patch("tap_postgres.parquet_writer.pafs.S3FileSystem")
    def test_init(self, mock_s3_filesystem):
        """Test ParquetWriter initialization without proxy_options"""
        writer = ParquetWriter("my-bucket", "us-west-2")
        self.assertEqual(writer.s3_bucket, "my-bucket")
        self.assertEqual(writer.s3_region, "us-west-2")
        self.assertIsNotNone(writer.filesystem)
        # Verify S3FileSystem was called with None for proxy_options
        mock_s3_filesystem.assert_called_once_with(
            region="us-west-2", proxy_options=None
        )

    @patch("tap_postgres.parquet_writer.pafs.S3FileSystem")
    def test_init_with_proxy_options(self, mock_s3_filesystem):
        """Test ParquetWriter initialization with proxy_options"""
        proxy_options = {
            "scheme": "http",
            "host": "proxy.example.com",
            "port": 3128,
        }
        writer = ParquetWriter("my-bucket", "us-west-2", proxy_options=proxy_options)
        self.assertEqual(writer.s3_bucket, "my-bucket")
        self.assertEqual(writer.s3_region, "us-west-2")
        self.assertIsNotNone(writer.filesystem)
        # Verify S3FileSystem was called with proxy_options
        mock_s3_filesystem.assert_called_once_with(
            region="us-west-2", proxy_options=proxy_options
        )

    def test_build_pyarrow_schema_all_types(self):
        """Test build_pyarrow_schema with all PostgreSQL type mappings"""
        desired_columns = [
            "id",
            "small_id",
            "big_id",
            "name",
            "active",
            "created_at",
            "event_date",
            "price",
            "rate",
            "real_val",
            "double_val",
            "varchar_col",
            "char_col",
            "text_col",
            "json_col",
            "uuid_col",
        ]
        md_map = self._create_md_map(
            id="integer",
            small_id="smallint",
            big_id="bigint",
            name="varchar",
            active="boolean",
            created_at="timestamp without time zone",
            event_date="date",
            price="numeric(10,2)",
            rate="numeric",
            real_val="real",
            double_val="double precision",
            varchar_col="character varying",
            char_col="char",
            text_col="text",
            json_col="json",
            uuid_col="uuid",
        )

        schema = self.parquet_writer.build_pyarrow_schema(desired_columns, md_map)

        self.assertIsInstance(schema, pa.Schema)
        self.assertEqual(len(schema), len(desired_columns))
        # Verify type mappings
        self.assertEqual(schema.field("id").type, pa.int32())
        self.assertEqual(schema.field("small_id").type, pa.int16())
        self.assertEqual(schema.field("big_id").type, pa.int64())
        self.assertEqual(schema.field("name").type, pa.string())
        self.assertEqual(schema.field("active").type, pa.bool_())
        self.assertEqual(schema.field("created_at").type, pa.timestamp("us"))
        self.assertEqual(schema.field("event_date").type, pa.timestamp("us"))
        self.assertEqual(schema.field("price").type, pa.decimal128(10, 2))
        self.assertEqual(
            schema.field("rate").type, pa.string()
        )  # numeric without precision
        self.assertEqual(schema.field("real_val").type, pa.float32())
        self.assertEqual(schema.field("double_val").type, pa.float64())
        self.assertEqual(schema.field("varchar_col").type, pa.string())
        self.assertEqual(schema.field("char_col").type, pa.string())
        self.assertEqual(schema.field("text_col").type, pa.string())
        self.assertEqual(schema.field("json_col").type, pa.string())
        self.assertEqual(schema.field("uuid_col").type, pa.string())
        # All fields should be nullable
        for field in schema:
            self.assertTrue(field.nullable)
        # Verify column order is preserved
        self.assertEqual(list(schema.names), desired_columns)

    def test_build_pyarrow_schema_fallback_behavior(self):
        """Test build_pyarrow_schema fallback to string for unknown/missing types"""
        # Test unknown type
        schema1 = self.parquet_writer.build_pyarrow_schema(
            ["unknown_col"], self._create_md_map(unknown_col="custom_type")
        )
        self.assertEqual(schema1.field("unknown_col").type, pa.string())

        # Test missing sql-datatype
        schema2 = self.parquet_writer.build_pyarrow_schema(
            ["missing_col"], {("properties", "missing_col"): {}}
        )
        self.assertEqual(schema2.field("missing_col").type, pa.string())

    @patch("tap_postgres.parquet_writer.pq.ParquetWriter")
    @patch("tap_postgres.parquet_writer.pacsv.open_csv")
    def test_convert_csv_to_parquet_basic(
        self, mock_open_csv, mock_parquet_writer_class
    ):
        """Test convert_csv_to_parquet core functionality with path variations"""
        desired_columns = ["id", "name"]
        md_map = self._create_md_map(id="integer", name="varchar")

        schema = self.parquet_writer.build_pyarrow_schema(desired_columns, md_map)
        batch = self._create_record_batch(100, schema)
        mock_writer = self._setup_mock_conversion(
            mock_open_csv, mock_parquet_writer_class, [batch]
        )

        # Test with .csv extension
        csv_path = "test/path.csv"
        parquet_path, returned_schema = self.parquet_writer.convert_csv_to_parquet(
            csv_path, desired_columns, md_map
        )

        self.assertEqual(parquet_path, "test/path.parquet")
        self.assertIsInstance(returned_schema, pa.Schema)
        self.assertEqual(len(returned_schema), 2)

        # Verify ParquetWriter was created with correct parameters
        mock_parquet_writer_class.assert_called_once()
        call_args = mock_parquet_writer_class.call_args
        self.assertIn(f"{self.s3_bucket}/{parquet_path}", call_args[0][0])
        self.assertEqual(call_args[1]["compression"], DEFAULT_COMPRESSION)
        self.assertEqual(call_args[1]["compression_level"], DEFAULT_COMPRESSION_LEVEL)
        self.assertTrue(mock_writer.write_table.called)

        # Test without .csv extension
        mock_parquet_writer_class.reset_mock()
        csv_path_no_ext = "test/path"
        parquet_path_no_ext, _ = self.parquet_writer.convert_csv_to_parquet(
            csv_path_no_ext, desired_columns, md_map
        )
        self.assertEqual(parquet_path_no_ext, "test/path.parquet")

        # Test with custom parquet_path
        mock_parquet_writer_class.reset_mock()
        custom_parquet_path = "custom/output.parquet"
        parquet_path_custom, _ = self.parquet_writer.convert_csv_to_parquet(
            csv_path, desired_columns, md_map, parquet_path=custom_parquet_path
        )
        self.assertEqual(parquet_path_custom, custom_parquet_path)
        call_args = mock_parquet_writer_class.call_args
        self.assertIn(f"{self.s3_bucket}/{custom_parquet_path}", call_args[0][0])

    @patch("tap_postgres.parquet_writer.pq.ParquetWriter")
    @patch("tap_postgres.parquet_writer.pacsv.open_csv")
    def test_convert_csv_to_parquet_custom_parameters(
        self, mock_open_csv, mock_parquet_writer_class
    ):
        """Test convert_csv_to_parquet with custom schema, compression, and row group size"""
        desired_columns = ["id", "name"]
        md_map = self._create_md_map(id="integer", name="varchar")

        # Test custom schema
        custom_schema = pa.schema(
            [
                pa.field("id", pa.int64(), nullable=True),
                pa.field("name", pa.string(), nullable=True),
            ]
        )
        batch = self._create_record_batch(10, custom_schema)
        mock_writer = self._setup_mock_conversion(
            mock_open_csv, mock_parquet_writer_class, [batch]
        )

        csv_path = "test/path.csv"
        parquet_path, returned_schema = self.parquet_writer.convert_csv_to_parquet(
            csv_path, desired_columns, md_map, arrow_schema=custom_schema
        )

        self.assertEqual(returned_schema, custom_schema)
        call_args = mock_parquet_writer_class.call_args
        self.assertEqual(call_args[0][1], custom_schema)

        # Test custom compression
        mock_parquet_writer_class.reset_mock()
        self.parquet_writer.convert_csv_to_parquet(
            csv_path,
            desired_columns,
            md_map,
            compression="snappy",
            compression_level=1,
        )
        call_args = mock_parquet_writer_class.call_args
        self.assertEqual(call_args[1]["compression"], "snappy")
        self.assertEqual(call_args[1]["compression_level"], 1)

        # Test custom row group size
        schema = self.parquet_writer.build_pyarrow_schema(desired_columns, md_map)
        batch = self._create_record_batch(10000, schema)
        mock_writer = self._setup_mock_conversion(
            mock_open_csv, mock_parquet_writer_class, [batch]
        )
        mock_parquet_writer_class.reset_mock()

        custom_row_group_size = 10000
        self.parquet_writer.convert_csv_to_parquet(
            csv_path, desired_columns, md_map, row_group_size=custom_row_group_size
        )
        call_args = mock_writer.write_table.call_args
        self.assertEqual(call_args[1]["row_group_size"], custom_row_group_size)

    @patch("tap_postgres.parquet_writer.pq.ParquetWriter")
    @patch("tap_postgres.parquet_writer.pacsv.open_csv")
    def test_convert_csv_to_parquet_buffering_and_edge_cases(
        self, mock_open_csv, mock_parquet_writer_class
    ):
        """Test convert_csv_to_parquet buffering logic and edge cases"""
        desired_columns = ["id"]
        md_map = self._create_md_map(id="integer")

        # Test multiple batches with buffering
        schema = self.parquet_writer.build_pyarrow_schema(desired_columns, md_map)
        batch1 = self._create_record_batch(300000, schema)
        batch2 = self._create_record_batch(300000, schema)
        batch3 = self._create_record_batch(100000, schema)
        mock_writer = self._setup_mock_conversion(
            mock_open_csv, mock_parquet_writer_class, [batch1, batch2, batch3]
        )

        csv_path = "test/path.csv"
        self.parquet_writer.convert_csv_to_parquet(
            csv_path, desired_columns, md_map, row_group_size=500000
        )

        # Should write multiple times due to buffering logic
        self.assertGreater(mock_writer.write_table.call_count, 0)

        # Test empty batches
        mock_parquet_writer_class.reset_mock()
        mock_reader = self._create_mock_csv_reader([])
        mock_open_csv.return_value = mock_reader
        self._setup_mock_file_and_writer(mock_parquet_writer_class)

        parquet_path, schema = self.parquet_writer.convert_csv_to_parquet(
            csv_path, desired_columns, md_map
        )

        self.assertEqual(parquet_path, "test/path.parquet")
        self.assertIsInstance(schema, pa.Schema)
