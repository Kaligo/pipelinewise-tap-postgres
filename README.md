# pipelinewise-tap-postgres

[![PyPI version](https://badge.fury.io/py/pipelinewise-tap-postgres.svg)](https://badge.fury.io/py/pipelinewise-tap-postgres)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/pipelinewise-tap-postgres.svg)](https://pypi.org/project/pipelinewise-tap-postgres/)
[![License: MIT](https://img.shields.io/badge/License-GPLv3-yellow.svg)](https://opensource.org/licenses/GPL-3.0)

[Singer](https://www.singer.io/) tap that extracts data from a [PostgreSQL](https://www.postgresql.com/) database and produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This is a [PipelineWise](https://transferwise.github.io/pipelinewise) compatible tap connector.

## How to use it

The recommended method of running this tap is to use it from [PipelineWise](https://transferwise.github.io/pipelinewise). When running it from PipelineWise you don't need to configure this tap with JSON files and most of things are automated. Please check the related documentation at [Tap Postgres](https://transferwise.github.io/pipelinewise/connectors/taps/postgres.html)

If you want to run this [Singer Tap](https://singer.io) independently please read further.

### Install and Run

First, make sure Python 3 is installed on your system or follow these
installation instructions for [Mac](http://docs.python-guide.org/en/latest/starting/install3/osx/) or
[Ubuntu](https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04).


It's recommended to use a virtualenv:

```bash
  python3 -m venv venv
  pip install pipelinewise-tap-postgres
```

or

```bash
  make venv
```

### Create a config.json

```
{
  "host": "localhost",
  "port": 5432,
  "user": "postgres",
  "password": "secret",
  "dbname": "db"
}
```

These are the same basic configuration properties used by the PostgreSQL command-line client (`psql`).

Full list of options in `config.json`:

| Property                   | Type    | Required? | Default | Description                                                                                                                                                                                |
|----------------------------|---------|----------|---------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| host                       | String  | Yes      | -       | PostgreSQL host                                                                                                                                                                            |
| port                       | Integer | Yes      | -       | PostgreSQL port                                                                                                                                                                            |
| user                       | String  | Yes      | -       | PostgreSQL user                                                                                                                                                                            |
| password                   | String  | Yes      | -       | PostgreSQL password                                                                                                                                                                        |
| dbname                     | String  | Yes      | -       | PostgreSQL database name                                                                                                                                                                   |
| filter_schemas             | String  | No       | None    | Comma separated schema names to scan only the required schemas to improve the performance of data extraction.                                                                              |
| ssl                        | String  | No       | None    | If set to `"true"` then use SSL via postgres sslmode `require` option. If the server does not accept SSL connections or the client certificate is not recognized the connection will fail. |
| logical_poll_total_seconds | Integer | No       | 10800   | Stop running the tap when no data received from wal after certain number of seconds.                                                                                                       |
| break_at_end_lsn           | Boolean | No       | true    | Stop running the tap if the newly received lsn is after the max lsn that was detected when the tap started.                                                                                |
| max_run_seconds            | Integer | No       | 43200   | Stop running the tap after certain number of seconds.                                                                                                                                      |
| debug_lsn                  | String  | No       | None    | If set to `"true"` then add `_sdc_lsn` property to the singer messages to debug postgres LSN position in the WAL stream.                                                                   |
| tap_id                     | String  | No       | None    | ID of the pipeline/tap                                                                                                                                                                     |
| itersize                   | Integer | No       | 20000   | Size of PG cursor iterator when doing INCREMENTAL or FULL_TABLE                                                                                                                            |
| default_replication_method | String  | No       | None    | Default replication method to use when no one is provided in the catalog (Values: `LOG_BASED`, `INCREMENTAL` or `FULL_TABLE`)                                                              |
| use_secondary              | Boolean | No       | False   | Use a database replica for `INCREMENTAL` and `FULL_TABLE` replication                                                                                                                      |
| secondary_host             | String  | No       | -       | PostgreSQL Replica host (required if `use_secondary` is `True`)                                                                                                                            |
| secondary_port             | Integer | No       | -       | PostgreSQL Replica port (required if `use_secondary` is `True`)                                                                                                                            |
| limit                      | Integer | No       | None    | Adds a limit to INCREMENTAL queries to limit the number of records returns per run                                                                                                         |
| fast_sync_rds              | Boolean | No       | False   | Enable fast sync RDS mode for optimized performance. When enabled, data is exported directly from RDS PostgreSQL to S3 using `aws_s3.query_export_to_s3`. Requires AWS RDS PostgreSQL with aws_s3 extension. |
| fast_sync_rds_s3_bucket    | String  | No       | -       | S3 bucket name for fast sync RDS exports (required if `fast_sync_rds` is `True`)                                                                                                                    |
| fast_sync_rds_s3_prefix    | String  | No       | ""      | S3 prefix/path for fast sync RDS exports                                                                                                                                                        |
| fast_sync_rds_s3_region    | String  | No       | -       | AWS region where the S3 bucket is located (required if `fast_sync_rds` is `True`)                                                                                                                    |
| fast_sync_rds_add_metadata_columns | Boolean | No       | True    | Whether to add metadata columns (`_SDC_BATCHED_AT`, `_SDC_DELETED_AT`, `_SDC_EXTRACTED_AT`) to the exported data. Set to `False` to exclude these columns. **Note:** This setting must be synced with the `add_metadata_columns` setting in `pipelinewise-target-redshift` to ensure consistent schema between tap and target. |


### Run the tap in Discovery Mode

```
tap-postgres --config config.json --discover                # Should dump a Catalog to stdout
tap-postgres --config config.json --discover > catalog.json # Capture the Catalog
```

### Add Metadata to the Catalog

Each entry under the Catalog's "stream" key will need the following metadata:

```
{
  "streams": [
    {
      "stream_name": "my_topic"
      "metadata": [{
        "breadcrumb": [],
        "metadata": {
          "selected": true,
          "replication-method": "LOG_BASED",
        }
      }]
    }
  ]
}
```

The replication method can be one of `FULL_TABLE`, `INCREMENTAL` or `LOG_BASED`.

**Note**: Log based replication requires a few adjustments in the source postgres database, please read further
for more information.

### Run the tap in Sync Mode

```
tap-postgres --config config.json --catalog catalog.json
```

The tap will write bookmarks to stdout which can be captured and passed as an optional `--state state.json` parameter
to the tap for the next sync.

### Fast Sync RDS Mode

Fast Sync RDS is a high-performance data syncing strategy designed specifically for AWS RDS PostgreSQL as the data source and Redshift as the target. It bypasses the Singer Specification for optimized performance by exporting data directly from RDS PostgreSQL to S3 using the `aws_s3.query_export_to_s3` function.

#### Fast Sync RDS Requirements

* **AWS RDS PostgreSQL**: Fast sync RDS requires an AWS RDS PostgreSQL instance (or Aurora PostgreSQL) with the `aws_s3` and `aws_commons` extensions installed.

* **S3 Bucket**: An S3 bucket accessible from both the RDS PostgreSQL instance and the Redshift cluster.

* **IAM Permissions**: The RDS PostgreSQL instance must have IAM permissions to write to the S3 bucket. See [AWS document](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/postgresql-s3-export.html) for detailed setup instructions.

#### Enabling Fast Sync RDS

To enable fast sync RDS, add the following configuration to your `config.json`:

```json
{
  "fast_sync_rds": true,
  "fast_sync_rds_s3_bucket": "your-s3-bucket-name",
  "fast_sync_rds_s3_prefix": "postgres/exports",
  "fast_sync_rds_s3_region": "us-east-1",
  "fast_sync_rds_add_metadata_columns": true
}
```

When fast sync RDS is enabled, streams with `FULL_TABLE` or `INCREMENTAL` replication methods will automatically use the fast sync RDS strategy. The tap will export data directly to S3 and output S3 path information as `FAST_SYNC_RDS_S3_INFO` messages that can be consumed by the target (Redshift) side.

For detailed setup instructions, see [Exporting data from an Aurora PostgreSQL DB cluster to Amazon S3](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/postgresql-s3-export.html).

#### Fast Sync RDS Benefits

* **Performance**: Direct export from PostgreSQL to S3 bypasses intermediate processing
* **Scalability**: Handles large tables efficiently
* **Deletion Detection**: Supports propagating row deletions from source to target by comparing full table dumps

#### Fast Sync RDS Limitations

* **Bypasses Data Processing**: This approach exports data directly to S3, which bypasses regular mappers and other plugins that may be configured in your pipeline. As a result, this method should only be used for safe views and tables that do not contain PII (Personally Identifiable Information) or other sensitive data that requires transformation or filtering.

* **File Splitting Behavior**: `aws_s3.query_export_to_s3` automatically splits large exports into multiple files when data exceeds approximately 6GB per file. Files are named with the pattern:
  - Base file: `{s3_path}`
  - Additional files: `{s3_path}_part2`, `{s3_path}_part3`, etc.

* **Metadata Columns Configuration**: The `fast_sync_rds_add_metadata_columns` setting in this tap must match the `add_metadata_columns` setting in `pipelinewise-target-redshift`. If they differ, schema mismatches will occur when loading data from S3 to Redshift. Both settings should be set to the same value (either both `True` or both `False`).

#### FAST_SYNC_RDS_S3_INFO Message Format

When fast sync RDS is enabled, the tap outputs `FAST_SYNC_RDS_S3_INFO` messages to stdout. These messages contain all the information needed by the target (Redshift) to load data from S3. The message format is:

```json
{
  "type": "FAST_SYNC_RDS_S3_INFO",
  "stream": "schema_name-table_name",
  "s3_bucket": "your-s3-bucket-name",
  "s3_path": "postgres/exports/schema_name-table_name/2025-01-15-123456_abc12345.csv",
  "s3_region": "us-east-1",
  "rows_uploaded": 1000,
  "files_uploaded": 1,
  "bytes_uploaded": 50000,
  "version": 1705323456000,
  "time_extracted": "2025-01-15T12:34:56.789000+00:00",
  "replication_method": "FULL_TABLE"
}
```

**Message Fields:**

| Field                | Type    | Description                                                                                                 |
|----------------------|---------|-------------------------------------------------------------------------------------------------------------|
| `type`               | String  | Always `"FAST_SYNC_RDS_S3_INFO"`                                                                            |
| `stream`              | String  | Destination stream name in format `{schema_name}-{table_name}`                                              |
| `s3_bucket`           | String  | S3 bucket name where the exported data is stored                                                           |
| `s3_path`             | String  | S3 path to the exported CSV file(s). Format: `{s3_prefix}/{schema_name}-{table_name}/{timestamp}_{sync_id}.csv` |
| `s3_region`           | String  | AWS region where the S3 bucket is located                                                                   |
| `rows_uploaded`       | Integer | Number of rows exported to S3                                                                               |
| `files_uploaded`      | Integer | Number of files created (may be > 1 if export was split)                                                   |
| `bytes_uploaded`      | Integer | Total bytes exported to S3                                                                                 |
| `version`             | Integer | Stream version (timestamp in milliseconds)                                                                |
| `time_extracted`      | String  | ISO 8601 formatted timestamp when data was extracted                                                         |
| `replication_method`  | String  | Replication method used: `"FULL_TABLE"` or `"INCREMENTAL"`                                                 |

**Note:** When `files_uploaded > 1`, the target must handle loading from multiple files. Additional files follow the naming pattern: `{s3_path}_part2`, `{s3_path}_part3`, etc.

### Log Based replication requirements

* PostgreSQL databases running **PostgreSQL versions 9.4.x or greater**. To avoid a critical PostgreSQL bug,
  use at least one of the following minor versions:
   - PostgreSQL 12.0
   - PostgreSQL 11.2
   - PostgreSQL 10.7
   - PostgreSQL 9.6.12
   - PostgreSQL 9.5.16
   - PostgreSQL 9.4.21

* **A connection to the master instance**. Log-based replication will only work by connecting to the master instance.

* **wal2json plugin**: To use Log Based for your PostgreSQL integration, you must install the wal2json plugin version >= 2.3.
  The wal2json plugin outputs JSON objects for logical decoding, which the tap then uses to perform Log-based Replication.
  Steps for installing the plugin vary depending on your operating system. Instructions for each operating system type
  are in the wal2json's GitHub repository:

  * [Unix-based operating systems](https://github.com/eulerto/wal2json#unix-based-operating-systems)
  * [Windows](https://github.com/eulerto/wal2json#windows)


* **postgres config file**: Locate the database configuration file (usually `postgresql.conf`) and define
  the parameters as follows:

    ```
    wal_level=logical
    max_replication_slots=5
    max_wal_senders=5
    ```

    Restart your PostgreSQL service to ensure the changes take effect.

    **Note**: For `max_replication_slots` and `max_wal_senders`, we're defaulting to a value of 5.
    This should be sufficient unless you have a large number of read replicas connected to the master instance.


* **Existing replication slot**: Log based replication requires a dedicated logical replication slot.
  In PostgreSQL, a logical replication slot represents a stream of database changes that can then be replayed to a
  client in the order they were made on the original server. Each slot streams a sequence of changes from a single
  database.

  Login to the master instance as a superuser and using the `wal2json` plugin, create a logical replication slot:
  ```
    SELECT *
    FROM pg_create_logical_replication_slot('pipelinewise_<database_name>', 'wal2json');
  ```

  **Note**: Replication slots are specific to a given database in a cluster. If you want to connect multiple
  databases - whether in one integration or several - you'll need to create a replication slot for each database.

### To run tests:

1. Install python test dependencies in a virtual env:
```
 make venv
```

2. You need to have a postgres database to run the tests and export its credentials.

You can make use of the local docker-compose to spin up a test database by running `make start_db`

Test objects will be created in the `postgres` database.

3. To run the unit tests:
```
  make unit_test
```

4. To run the integration tests:
```
  make integration_test
```

### To run pylint:

Install python dependencies and run python linter
```
  make venv
  make pylint
```
