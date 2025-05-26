import os
import json
from singer import metadata
from tap_braintree.streams import STREAMS


def get_abs_path(path):
    """
    Return full path for the specified file
    """

    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def get_schemas():
    """
    Fetch and return metadata and schema for all the streams
    """

    schemas = {}
    field_metadata = {}

    for stream_name, stream_metadata in STREAMS.items():

        schema_path = get_abs_path("schemas/{}.json".format(stream_name))
        with open(schema_path, "r") as file:
            schema = json.load(file)
        schemas[stream_name] = schema

        mdata = metadata.new()

        mdata = metadata.get_standard_metadata(
            schema=schema,
            key_properties=stream_metadata.key_properties,
            valid_replication_keys=stream_metadata.replication_keys,
            replication_method=stream_metadata.replication_method,
        )

        mdata = metadata.to_map(mdata)
        # Loop through all keys and make replication keys of automatic inclusion
        for field_name in schema["properties"].keys():

            if (
                stream_metadata.replication_keys
                and field_name in stream_metadata.replication_keys
            ):
                mdata = metadata.write(
                    mdata,
                    ("properties", field_name),
                    "inclusion",
                    "automatic",
                )

        mdata = metadata.to_list(mdata)
        field_metadata[stream_name] = mdata

    return schemas, field_metadata
