import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_braintree.streams import STREAMS
from tap_braintree.schema import get_schemas

LOGGER = singer.get_logger()

def discover():
    """
    Generate catalog for call the streams
    """

    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    for stream_name, schema_dict in schemas.items():
        schema = Schema.from_dict(schema_dict)
        mdata = field_metadata[stream_name]

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=STREAMS[stream_name].key_properties,
                schema=schema,
                metadata=mdata,
            )
        )

    return catalog
