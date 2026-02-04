import sys
import simplejson as json
import singer
from typing import Dict, Optional
from singer import metadata
import tap_postgres.db as post_db


# pylint: disable=invalid-name,missing-function-docstring
def should_sync_column(md_map, field_name):
    field_metadata = md_map.get(("properties", field_name), {})
    return singer.should_sync_field(
        field_metadata.get("inclusion"), field_metadata.get("selected"), True
    )


def write_message(message):
    sys.stdout.write(json.dumps(message, use_decimal=True) + "\n")
    sys.stdout.flush()


# extra_properties is a collection of col schemas that are created on the fly (not in orginal catalog)
def send_schema_message(stream, bookmark_properties, extra_properties={}):
    s_md = metadata.to_map(stream["metadata"])
    if s_md.get((), {}).get("is-view"):
        key_properties = s_md.get((), {}).get("view-key-properties", [])
    else:
        key_properties = s_md.get((), {}).get("table-key-properties", [])

    stream["schema"]["properties"].update(extra_properties)

    schema_message = {
        "type": "SCHEMA",
        "stream": post_db.calculate_destination_stream_name(stream, s_md),
        "schema": stream["schema"],
        "key_properties": key_properties,
        "bookmark_properties": bookmark_properties,
    }

    write_message(schema_message)


def escape_sql_string(value: str) -> str:
    """
    Escape single quotes in SQL string literals
    """
    return value.replace("'", "''")


def get_select_latest_sql(params: Dict) -> str:
    """
    Build a SQL query to select the latest row from a table based on replication key.

    The query selects the row with the maximum value of the replication key,
    which is useful for getting the most recent record in incremental sync scenarios.

    Args:
        params: Dictionary containing:
            - escaped_columns (List[str]): List of column names to select (already escaped)
            - replication_key (str): Column name to use for ordering (will be escaped)
            - schema_name (str): Database schema name
            - table_name (str): Table name

    Returns:
        str: SQL query string that selects the latest row ordered by replication_key DESC

    Example:
        >>> params = {
        ...     'escaped_columns': ['id', 'name'],
        ...     'replication_key': 'updated_at',
        ...     'schema_name': 'public',
        ...     'table_name': 'users'
        ... }
        >>> sql = get_select_latest_sql(params)
        >>> # Returns: "SELECT id,name FROM "public"."users" ORDER BY updated_at DESC LIMIT 1;"
    """
    escaped_columns = params["escaped_columns"]
    replication_key = post_db.prepare_columns_sql(params["replication_key"])
    schema_name = params["schema_name"]
    table_name = params["table_name"]
    select_sql = f"""
    SELECT {",".join(escaped_columns)}
    FROM {post_db.fully_qualified_table_name(schema_name, table_name)}
    ORDER BY {replication_key} DESC LIMIT 1;"""

    return select_sql


def _build_where_clause_for_replication_query(params: dict) -> str:
    replication_key = params.get("replication_key", None)

    if not replication_key:
        return ""

    replication_key = post_db.prepare_columns_sql(replication_key)
    schema_name = params["schema_name"]
    table_name = params["table_name"]
    replication_key_value = params.get("replication_key_value", None)
    replication_key_sql_datatype = params.get("replication_key_sql_datatype", None)
    look_back_n_seconds = params.get("look_back_n_seconds", None)
    skip_last_n_seconds = params.get("skip_last_n_seconds", None)
    recover_mappings = params.get("recover_mappings", {})

    if reconcile_dates := recover_mappings.get(f"{schema_name}-{table_name}"):
        date_list = ",".join(f"'{date}'" for date in reconcile_dates)
        return f"WHERE {replication_key}::DATE in ({date_list})"

    replication_key_value = (
        escape_sql_string(replication_key_value) if replication_key_value else None
    )

    where_incr = (
        f"{replication_key} >= '{replication_key_value}'::{replication_key_sql_datatype}"
        if replication_key_value
        else ""
    )
    where_incr += (
        f" - interval '{look_back_n_seconds} seconds'"
        if look_back_n_seconds
        and replication_key_sql_datatype.startswith("timestamp")
        and replication_key_value
        else ""
    )

    where_skip = (
        f"{replication_key} <= NOW() - interval '{skip_last_n_seconds} seconds'"
        if skip_last_n_seconds and replication_key_sql_datatype.startswith("timestamp")
        else ""
    )

    where_statement = (
        f"WHERE {where_incr}{' AND ' if where_incr and where_skip else ''}{where_skip}"
        if where_incr or where_skip
        else ""
    )

    return where_statement


def get_query_for_replication_data(params: Dict) -> str:
    """
    Build a SQL query for replication data with optional filtering, ordering, and limiting.

    This function constructs a SELECT query that retrieves data for replication purposes.
    It uses a subquery pattern with the alias 'pg_speedup_trick' to optimize query
    performance in PostgreSQL.

    Args:
        params: Dictionary containing:
            - escaped_columns (List[str]): List of column names to select (already escaped)
            - schema_name (str): Database schema name (required)
            - table_name (str): Table name (required)
            - limit (int, optional): Maximum number of rows to return
            - replication_key (str, optional): Column name to use for ordering and filtering
            - replication_key_value (str, optional): Value to filter replication_key against
            - replication_key_sql_datatype (str, optional): SQL data type of replication_key
            - look_back_n_seconds (int, optional): Number of seconds to look back for timestamp-based replication
            - skip_last_n_seconds (int, optional): Number of seconds to skip from the end for timestamp-based replication
            - recover_mappings (Dict, optional): Dictionary mapping table names to reconcile dates
            - skip_order (bool, optional): If True, skip ORDER BY clause even if replication_key is provided

    Returns:
        str: SQL query string that selects replication data with optional WHERE, ORDER BY, and LIMIT clauses

    Example:
        >>> params = {
        ...     'escaped_columns': ['id', 'name', 'updated_at'],
        ...     'schema_name': 'public',
        ...     'table_name': 'users',
        ...     'replication_key': 'updated_at',
        ...     'replication_key_value': '2025-01-01 00:00:00',
        ...     'replication_key_sql_datatype': 'timestamp',
        ...     'limit': 1000
        ... }
        >>> sql = get_query_for_replication_data(params)
        >>> # Returns a query with WHERE, ORDER BY, and LIMIT clauses
    """
    escaped_columns = params["escaped_columns"]
    schema_name = params["schema_name"]
    table_name = params["table_name"]
    limit_value = params.get("limit", None)
    limit_statement = f"LIMIT {limit_value}" if limit_value else ""
    order_statement = ""
    replication_key = params.get("replication_key", None)

    if replication_key and not params.get("skip_order", False):
        escaped_column = post_db.prepare_columns_sql(replication_key)
        order_statement = f"ORDER BY {escaped_column} ASC"

    where_statement = _build_where_clause_for_replication_query(params)

    select_sql = f"""
    SELECT {",".join(escaped_columns)}
    FROM (
        SELECT *
        FROM {post_db.fully_qualified_table_name(schema_name, table_name)}
        {where_statement}
        {order_statement} {limit_statement}
    ) pg_speedup_trick;"""

    return select_sql
