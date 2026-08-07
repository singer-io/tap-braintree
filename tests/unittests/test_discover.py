import unittest
import importlib
from unittest import mock

discover_module = importlib.import_module("tap_braintree.discover")


class TestDiscoverModule(unittest.TestCase):
    @mock.patch("tap_braintree.discover.get_schemas")
    def test_discover_builds_catalog(self, mock_get_schemas):
        schema_dict = {
            "type": "object",
            "properties": {"id": {"type": "string"}},
        }
        stream_metadata = [{"breadcrumb": [], "metadata": {"table-key-properties": ["id"]}}]
        mock_get_schemas.return_value = (
            {"transactions": schema_dict},
            {"transactions": stream_metadata},
        )

        catalog = discover_module.discover()
        self.assertEqual(len(catalog.streams), 1)
        self.assertEqual(catalog.streams[0].tap_stream_id, "transactions")
        self.assertEqual(catalog.streams[0].key_properties, ["id"])

    @mock.patch("tap_braintree.discover.LOGGER")
    @mock.patch("tap_braintree.discover.get_schemas")
    def test_discover_logs_and_reraises_schema_error(
        self, mock_get_schemas, mock_logger
    ):
        bad_schema = {"type": "array"}
        mock_get_schemas.return_value = ({"transactions": bad_schema}, {})

        with self.assertRaises(Exception):
            discover_module.discover()

        self.assertTrue(mock_logger.error.called)
