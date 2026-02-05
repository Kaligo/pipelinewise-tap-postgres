import re
from contextlib import contextmanager
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import pyarrow.csv as pacsv

from typing import Dict, List, Optional, Tuple

# Default compression algorithm for Parquet files.
DEFAULT_COMPRESSION = "zstd"

# For zstd, range is 1 (fastest) to 22 (best compression).
DEFAULT_COMPRESSION_LEVEL = 8

# Default row group size for Parquet files
DEFAULT_ROW_GROUP_SIZE = 500000

# 128MB CSV blocks
BATCH_SIZE_BYTES = 128 * 1024 * 1024

# To not hold more than 8*BATCH_SIZE_BYTES in memory
MAX_BATCHES = 8


class ParquetWriter:
    """
    Class to handle conversion of CSV files exported from PostgreSQL to Parquet format.
    """

    def __init__(
        self, s3_bucket: str, s3_region: str, proxy_options: Optional[Dict] = None
    ):
        """
        Initialize ParquetWriter.

        Args:
            s3_bucket: S3 bucket name
            s3_region: AWS region for S3
            proxy_options: Optional proxy configuration dict with keys:
                - scheme: Proxy scheme (e.g., "http", "https")
                - host: Proxy hostname
                - port: Proxy port number
        """
        self.s3_bucket = s3_bucket
        self.s3_region = s3_region
        self.filesystem = pafs.S3FileSystem(
            region=self.s3_region, proxy_options=proxy_options
        )

    def _postgres_type_to_pyarrow(self, postgres_type: str) -> pa.DataType:
        """
        Convert PostgreSQL type string to a PyArrow DataType.
        Keeps mapping intentionally small and defaults to string for unknowns.
        """
        postgres_type_lower = postgres_type.lower()

        if "timestamp" in postgres_type_lower:
            return pa.timestamp("us")

        if (
            "character varying" in postgres_type_lower
            or "varchar" in postgres_type_lower
        ):
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
                return pa.decimal128(int(match.group(1)), int(match.group(2) or 0))
            # fallback to string to avoid silent corruption
            return pa.string()

        if "real" in postgres_type_lower or "float4" in postgres_type_lower:
            return pa.float32()
        if "double precision" in postgres_type_lower or "float8" in postgres_type_lower:
            return pa.float64()

        if "boolean" in postgres_type_lower or "bool" in postgres_type_lower:
            return pa.bool_()

        if "date" in postgres_type_lower:
            # Use timestamp for compatibility with Spectrum expectations on date columns
            # See https://github.com/Kaligo/pipelinewise-target-redshift/blob/ea606dfe58d2b675a2de931ca62209cf08bcc186/target_redshift/db_sync.py#L70-L71
            return pa.timestamp("us")

        if "json" in postgres_type_lower:
            return pa.string()

        if "uuid" in postgres_type_lower:
            return pa.string()

        # Default fallback
        return pa.string()

    def build_pyarrow_schema(
        self, desired_columns: List[str], md_map: Dict
    ) -> pa.Schema:
        """
        Build a PyArrow schema matching the exported CSV order.
        """
        fields: List[pa.Field] = []

        for col_name in desired_columns:
            sql_datatype = md_map.get(("properties", col_name), {}).get(
                "sql-datatype", "text"
            )
            pa_type = self._postgres_type_to_pyarrow(sql_datatype)
            fields.append(pa.field(col_name, pa_type, nullable=True))

        return pa.schema(fields)

    def _build_parquet_path(
        self, csv_path: str, parquet_path: Optional[str] = None
    ) -> str:
        """
        Build the Parquet output path from CSV path if not provided.

        Args:
            csv_path: Input CSV path
            parquet_path: Optional explicit Parquet path

        Returns:
            Parquet path string
        """
        if parquet_path:
            return parquet_path
        if "." in csv_path:
            return csv_path.rsplit(".", 1)[0] + ".parquet"
        return f"{csv_path}.parquet"

    @contextmanager
    def _create_csv_reader(
        self, input_path: str, desired_columns: List[str], schema: pa.Schema
    ):
        """
        Create a CSV reader with configured options.

        Args:
            input_path: S3 path to the CSV file (bucket/path)
            desired_columns: List of column names
            schema: PyArrow schema for type conversion

        Yields:
            CSV reader context manager
        """
        with self.filesystem.open_input_file(input_path) as input_file:
            with pacsv.open_csv(
                input_file,
                read_options=pacsv.ReadOptions(
                    column_names=desired_columns,
                    block_size=BATCH_SIZE_BYTES,
                    use_threads=True,
                ),
                convert_options=pacsv.ConvertOptions(
                    column_types=schema,
                ),
            ) as reader:
                yield reader

    def _write_buffered_batches(
        self,
        writer: pq.ParquetWriter,
        buffered_batches: List[pa.RecordBatch],
        row_group_size: int,
    ) -> None:
        """
        Write buffered batches to Parquet as a table.

        Args:
            writer: Parquet writer instance
            buffered_batches: List of record batches to write
            row_group_size: Target row group size
        """
        if not buffered_batches:
            return
        table = pa.Table.from_batches(buffered_batches)
        writer.write_table(table, row_group_size=row_group_size)

    def _process_csv_to_parquet(
        self,
        reader: pacsv.CSVStreamingReader,
        writer: pq.ParquetWriter,
        row_group_size: int,
    ) -> None:
        """
        Process CSV batches and write to Parquet with buffering.

        Args:
            reader: CSV reader yielding batches
            writer: Parquet writer instance
            row_group_size: Target row group size for row groups
        """
        buffered_batches = []
        buffered_rows = 0

        for batch in reader:
            buffered_batches.append(batch)
            buffered_rows += batch.num_rows

            # Write when we reach target row group size or max batches limit
            if buffered_rows >= row_group_size or len(buffered_batches) >= MAX_BATCHES:
                self._write_buffered_batches(writer, buffered_batches, row_group_size)
                buffered_batches.clear()
                buffered_rows = 0

        # Flush remaining batches
        self._write_buffered_batches(writer, buffered_batches, row_group_size)

    def convert_csv_to_parquet(
        self,
        csv_path: str,
        desired_columns: List[str],
        md_map: Dict,
        parquet_path: Optional[str] = None,
        arrow_schema: Optional[pa.Schema] = None,
        compression: str = DEFAULT_COMPRESSION,
        compression_level: int = DEFAULT_COMPRESSION_LEVEL,
        row_group_size: int = DEFAULT_ROW_GROUP_SIZE,
    ) -> Tuple[str, pa.Schema]:
        """
        Convert CSV file from S3 to Parquet format.

        Args:
            csv_path: S3 path to CSV file (relative to bucket)
            desired_columns: List of column names in order
            md_map: Metadata map containing sql-datatype for each column
            parquet_path: Optional S3 path for output Parquet file
            arrow_schema: Optional PyArrow schema (if not provided, built from md_map)
            compression: Compression algorithm (default: zstd)
            compression_level: Compression level (default: 8 for zstd)
            row_group_size: Target rows per row group (default: 500000)

        Returns:
            Tuple of (parquet_path, PyArrow schema)
        """
        parquet_path = self._build_parquet_path(csv_path, parquet_path)
        schema = arrow_schema or self.build_pyarrow_schema(desired_columns, md_map)

        input_path = f"{self.s3_bucket}/{csv_path}"
        output_path = f"{self.s3_bucket}/{parquet_path}"

        with self._create_csv_reader(input_path, desired_columns, schema) as reader:
            with pq.ParquetWriter(
                output_path,
                schema,
                filesystem=self.filesystem,
                compression=compression,
                compression_level=compression_level,
            ) as writer:
                self._process_csv_to_parquet(reader, writer, row_group_size)

        return parquet_path, schema
