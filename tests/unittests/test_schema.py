import unittest

from tap_braintree import schema as schema_module


class TestSchemaModule(unittest.TestCase):
    def test_get_abs_path_points_to_schema_file(self):
        path = schema_module.get_abs_path("schemas/transactions.json")
        self.assertTrue(path.replace("\\", "/").endswith("schemas/transactions.json"))

    def test_get_schemas_returns_transactions_and_automatic_metadata(self):
        schemas, field_metadata = schema_module.get_schemas()
        self.assertIn("transactions", schemas)
        self.assertIn("properties", schemas["transactions"])

        metadata_entries = field_metadata["transactions"]
        updated_at_entry = next(
            item
            for item in metadata_entries
            if list(item.get("breadcrumb", [])) == ["properties", "updated_at"]
        )
        self.assertEqual(
            updated_at_entry.get("metadata", {}).get("inclusion"), "automatic"
        )
