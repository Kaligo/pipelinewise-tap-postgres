import copy
import json
import sys
import singer

from typing import List, Dict
from singer import metadata

from tap_postgres.db import open_connection
from tap_postgres.discovery_utils import discover_db

LOGGER = singer.get_logger('tap_postgres')


def dump_catalog(all_streams: List[Dict]) -> None:
    """
    Prints the catalog to the std output
    Args:
        all_streams: List of streams to dump
    """
    json.dump({'streams': all_streams}, sys.stdout, indent=2)


def is_selected_via_metadata(stream: Dict) -> bool:
    """
    Checks if stream is selected ia metadata
    Args:
        stream: stream dictionary

    Returns: True if selected, False otherwise.
    """
    table_md = metadata.to_map(stream['metadata']).get((), {})
    return table_md.get('selected', False)


def clear_state_on_replication_change(state: Dict,
                                      tap_stream_id: str,
                                      replication_key: str,
                                      replication_method: str) -> Dict:
    """
    Update state if replication method change is detected
    Returns: new state dictionary
    """
    # user changed replication, nuke state
    last_replication_method = singer.get_bookmark(state, tap_stream_id, 'last_replication_method')
    if last_replication_method is not None and (replication_method != last_replication_method):
        state = singer.reset_stream(state, tap_stream_id)

    # key changed
    if replication_method == 'INCREMENTAL' and \
            replication_key != singer.get_bookmark(state, tap_stream_id, 'replication_key'):
        state = singer.reset_stream(state, tap_stream_id)

    state = singer.write_bookmark(state, tap_stream_id, 'last_replication_method', replication_method)

    return state


def refresh_streams_schema(conn_config: Dict, streams: List[Dict]):
    """
    Updates the streams schema & metadata with new discovery
    The given streams list of dictionaries would be mutated and updated
    """
    LOGGER.debug('Refreshing streams schemas ...')

    LOGGER.debug('Current streams schemas %s', streams)

    # Run discovery to get the streams most up to date json schemas
    with open_connection(conn_config) as conn:
        new_discovery = {
            stream['tap_stream_id']: stream
            for stream in discover_db(conn, conn_config.get('filter_schemas'), [st['table_name'] for st in streams])
        }

        LOGGER.debug('New discovery schemas %s', new_discovery)

        # For every stream dictionary, update the schema and metadata from the new discovery
        for idx, stream in enumerate(streams):
            # update schema
            discovered_stream = new_discovery[stream['tap_stream_id']]
            streams[idx]['schema'] = merge_stream_schema(stream, discovered_stream)

            # Update metadata
            #
            # 1st step: new discovery doesn't contain non-discoverable metadata: e.g replication method & key, selected
            # so let's copy those from the original stream object
            md_map = metadata.to_map(stream['metadata'])
            meta = md_map.get(())

            for idx_met, metadatum in enumerate(discovered_stream['metadata']):
                if not metadatum['breadcrumb']:
                    meta.update(merge_stream_metadata(meta, discovered_stream['metadata'][idx_met]['metadata']))
                    discovered_stream['metadata'][idx_met]['metadata'] = meta

            # 2nd step: now copy all the metadata from the updated new discovery to the original stream
            streams[idx]['metadata'] = copy.deepcopy(discovered_stream['metadata'])

    LOGGER.debug('Updated streams schemas %s', streams)
    LOGGER.info("The first stream is %s", streams)

def merge_stream_metadata(metadata, discovered_metadata):
    """
    Merge metadata from the discovery with the existing metadata from the stream.
    When the `merge_metadata` key is set to true in the metadata, the existing metadata is used.
    """
    if metadata.get('merge_metadata'):
        return copy.deepcopy(metadata)
    return copy.deepcopy(discovered_metadata)


def merge_stream_schema(stream, discovered_stream):
    """
    When the db discovery happens, the stream schema (infered from the catalog) is updated with the new schema in the discovery.
    This scenario makes the schema specified in the config yaml file become in vain. 
    This function merges the schema from the catalog with the schema from the discovery, 
    hence helping the tap to resist to the schema evolution but retain the configured schema from users.
    """
    discovered_schema = copy.deepcopy(discovered_stream['schema'])
    for column, column_schema in stream['schema']['properties'].items():
        if column in discovered_schema['properties'] and column_schema != discovered_schema['properties'][column]:
            override = copy.deepcopy(stream['schema']['properties'][column])
            LOGGER.info('Overriding schema for %s.%s with %s', stream['tap_stream_id'], column, override)
            discovered_schema['properties'][column].update(override)
    return discovered_schema
    
def any_logical_streams(streams, default_replication_method):
    """
    Checks if streams list contains any stream with log_based method
    """
    for stream in streams:
        stream_metadata = metadata.to_map(stream['metadata'])
        replication_method = stream_metadata.get((), {}).get('replication-method', default_replication_method)
        if replication_method == 'LOG_BASED':
            return True

    return False
