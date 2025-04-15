import copy
import time
import psycopg2
import psycopg2.extras
import singer

from singer import utils
from functools import partial
from singer import metrics

import tap_postgres.db as post_db


LOGGER = singer.get_logger('tap_postgres')

UPDATE_BOOKMARK_PERIOD = 10000


# pylint: disable=invalid-name,missing-function-docstring
def fetch_max_replication_key(conn_config, replication_key, schema_name, table_name):
    with post_db.open_connection(conn_config, False) as conn:
        with conn.cursor() as cur:
            max_key_sql = f"""
                SELECT max({post_db.prepare_columns_sql(replication_key)})
                FROM {post_db.fully_qualified_table_name(schema_name, table_name)}"""

            LOGGER.info("determine max replication key value: %s", max_key_sql)
            cur.execute(max_key_sql)
            max_key = cur.fetchone()[0]

            LOGGER.info("max replication key value: %s", max_key)
            return max_key


# pylint: disable=too-many-locals
def sync_table(conn_info, stream, state, desired_columns, md_map):
    time_extracted = utils.now()

    stream_version = singer.get_bookmark(state, stream['tap_stream_id'], 'version')
    if stream_version is None:
        stream_version = int(time.time() * 1000)

    state = singer.write_bookmark(state,
                                  stream['tap_stream_id'],
                                  'version',
                                  stream_version)
    singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

    schema_name = md_map.get(()).get('schema-name')

    escaped_columns = list(map(partial(post_db.prepare_columns_for_select_sql, md_map=md_map), desired_columns))

    activate_version_message = singer.ActivateVersionMessage(
        stream=post_db.calculate_destination_stream_name(stream, md_map),
        version=stream_version)

    singer.write_message(activate_version_message)

    replication_key = md_map.get((), {}).get('replication-key')
    replication_key_value = singer.get_bookmark(state, stream['tap_stream_id'], 'replication_key_value')
    replication_key_sql_datatype = md_map.get(('properties', replication_key)).get('sql-datatype')

    hstore_available = post_db.hstore_available(conn_info)
    with metrics.record_counter(None) as counter:
        with post_db.open_connection(conn_info) as conn:

            # Client side character encoding defaults to the value in postgresql.conf under client_encoding.
            # The server / db can also have its own configured encoding.
            with conn.cursor() as cur:
                cur.execute("show server_encoding")
                LOGGER.info("Current Server Encoding: %s", cur.fetchone()[0])
                cur.execute("show client_encoding")
                LOGGER.info("Current Client Encoding: %s", cur.fetchone()[0])

            if hstore_available:
                LOGGER.info("hstore is available")
                psycopg2.extras.register_hstore(conn)
            else:
                LOGGER.info("hstore is UNavailable")

            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='pipelinewise') as cur:
                cur.itersize = post_db.CURSOR_ITER_SIZE
                LOGGER.info("Beginning new incremental replication sync %s", stream_version)
                select_sql = _get_select_sql({"escaped_columns": escaped_columns,
                                              "replication_key": replication_key,
                                              "replication_key_sql_datatype": replication_key_sql_datatype,
                                              "replication_key_value": replication_key_value,
                                              "schema_name": schema_name,
                                              "table_name": stream['table_name'],
                                              "limit": conn_info['limit'],
                                              "skip_last_n_seconds": conn_info['skip_last_n_seconds'],
                                              "look_back_n_seconds": conn_info['look_back_n_seconds'],
                                              "recover_mappings": conn_info['recover_mappings'],
                                              })
                LOGGER.info('select statement: %s with itersize %s', select_sql, cur.itersize)
                cur.execute(select_sql)

                rows_saved = 0

                for rec in cur:
                    record_message = post_db.selected_row_to_singer_message(stream,
                                                                            rec,
                                                                            stream_version,
                                                                            desired_columns,
                                                                            time_extracted,
                                                                            md_map)

                    singer.write_message(record_message)
                    rows_saved += 1

                    #Picking a replication_key with NULL values will result in it ALWAYS been synced which is not great
                    #event worse would be allowing the NULL value to enter into the state
                    if record_message.record[replication_key] is not None:
                        state = singer.write_bookmark(state,
                                                      stream['tap_stream_id'],
                                                      'replication_key_value',
                                                      record_message.record[replication_key])

                    if rows_saved % UPDATE_BOOKMARK_PERIOD == 0:
                        singer.write_message(singer.StateMessage(value=copy.deepcopy(state)))

                    counter.increment()
                
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor, name='pipelinewise') as cur:
                if rows_saved == 0:
                    latest_sql = _get_select_latest_sql({"escaped_columns": escaped_columns,
                                              "replication_key": replication_key,
                                              "schema_name": schema_name,
                                              "table_name": stream['table_name'],
                                            })
                    LOGGER.info('select latest row statement: %s', latest_sql)
                    cur.execute(latest_sql)
                    rec = cur.fetchone()
                    if rec:
                        LOGGER.info(f"The latest record of the table is: {rec}")
                        record_message = post_db.selected_row_to_singer_message(stream,
                                                                                rec,
                                                                                stream_version,
                                                                                desired_columns,
                                                                                time_extracted,
                                                                                md_map)
                        singer.write_message(record_message)
                        if record_message.record[replication_key] is not None:
                            state = singer.write_bookmark(state,
                                                        stream['tap_stream_id'],
                                                        'replication_key_value',
                                                        record_message.record[replication_key])

    return state

def _get_select_latest_sql(params):
    escaped_columns = params['escaped_columns']
    replication_key = post_db.prepare_columns_sql(params['replication_key'])
    schema_name = params['schema_name']
    table_name = params['table_name']
    select_sql = f"""
    SELECT {','.join(escaped_columns)}
    FROM {post_db.fully_qualified_table_name(schema_name, table_name)} 
    ORDER BY {replication_key} DESC LIMIT 1;"""

    return select_sql

def _get_select_sql(params):
    escaped_columns = params['escaped_columns']
    replication_key = post_db.prepare_columns_sql(params['replication_key'])
    replication_key_sql_datatype = params['replication_key_sql_datatype']
    replication_key_value = params['replication_key_value']
    schema_name = params['schema_name']
    table_name = params['table_name']
    recover_mappings = params['recover_mappings']

    limit_statement = f'LIMIT {params["limit"]}' if params["limit"] else ''

    if reconcile_dates:=recover_mappings.get(table_name):
        where_statement = f"WHERE {replication_key} in ({','.join(map(lambda reconcile_date: f"'{reconcile_date}'", reconcile_dates))})"
    else:
        where_incr = f"{replication_key} >= '{replication_key_value}'::{replication_key_sql_datatype}" \
            if replication_key_value else ""

        where_incr += f" - interval '{params['look_back_n_seconds']} seconds'" \
            if params["look_back_n_seconds"] and replication_key_sql_datatype.startswith("timestamp") and replication_key_value else ""

        where_skip = f"{replication_key} <= NOW() - interval '{params['skip_last_n_seconds']} seconds'" \
            if params["skip_last_n_seconds"] and replication_key_sql_datatype.startswith("timestamp") else ""

        where_statement = f"WHERE {where_incr}{' AND ' if where_incr and where_skip else ''}{where_skip}" \
            if where_incr or where_skip else ""

    select_sql = f"""
    SELECT {','.join(escaped_columns)}
    FROM (
        SELECT *
        FROM {post_db.fully_qualified_table_name(schema_name, table_name)} 
        {where_statement}
        ORDER BY {replication_key} ASC {limit_statement}
    ) pg_speedup_trick;"""

    return select_sql
