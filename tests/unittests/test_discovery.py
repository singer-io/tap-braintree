import unittest
from unittest import mock

from braintree.exceptions.authorization_error import AuthorizationError

from tap_braintree.discover import _apply_access_checks, _prune_inaccessible_children, discover
from tap_braintree.streams import STREAMS, Transaction


ALL_STREAM_NAMES = list(STREAMS.keys())


class TestCheckAccess(unittest.TestCase):

    def test_check_access_returns_true_on_success(self):
        with mock.patch("braintree.Transaction.search"):
            stream = Transaction()
            self.assertTrue(stream.check_access())

    def test_check_access_returns_false_on_authorization_error(self):
        with mock.patch("braintree.Transaction.search", side_effect=AuthorizationError()):
            stream = Transaction()
            self.assertFalse(stream.check_access())

    def test_check_access_returns_true_for_child_stream(self):
        with mock.patch.object(Transaction, "parent_stream", "some_parent"):
            stream = Transaction()
            self.assertTrue(stream.check_access())

    def test_check_access_does_not_swallow_authentication_errors(self):
        from braintree.exceptions.authentication_error import AuthenticationError
        with mock.patch("braintree.Transaction.search", side_effect=AuthenticationError()):
            stream = Transaction()
            with self.assertRaises(AuthenticationError):
                stream.check_access()


class TestGetSchemas(unittest.TestCase):

    def test_all_streams_have_schema(self):
        from tap_braintree.schema import get_schemas
        schemas, field_metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(ALL_STREAM_NAMES))
        self.assertEqual(set(field_metadata.keys()), set(ALL_STREAM_NAMES))

    def test_schema_keys_match_metadata_keys(self):
        from tap_braintree.schema import get_schemas
        schemas, field_metadata = get_schemas()
        self.assertEqual(set(schemas.keys()), set(field_metadata.keys()))

    def test_transactions_has_expected_key_properties(self):
        from tap_braintree.schema import get_schemas
        from singer import metadata as singer_metadata
        schemas, field_metadata = get_schemas()
        mdata_map = singer_metadata.to_map(field_metadata["transactions"])
        key_props = mdata_map.get((), {}).get("table-key-properties")
        self.assertEqual(key_props, ["id"])

    def test_transactions_has_replication_key(self):
        from tap_braintree.schema import get_schemas
        from singer import metadata as singer_metadata
        schemas, field_metadata = get_schemas()
        mdata_map = singer_metadata.to_map(field_metadata["transactions"])
        valid_replication_keys = mdata_map.get((), {}).get("valid-replication-keys")
        self.assertIn("updated_at", valid_replication_keys)


class TestPruneInaccessibleChildren(unittest.TestCase):

    def test_no_children_nothing_pruned(self):
        from tap_braintree.schema import get_schemas
        schemas, field_metadata = get_schemas()
        original_keys = set(schemas.keys())
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertEqual(set(schemas.keys()), original_keys)

    def test_prune_child_when_parent_missing(self):
        with mock.patch.object(Transaction, "parent_stream", "missing_parent"):
            schemas = {"transactions": {"type": "object"}}
            field_metadata = {"transactions": {}}
            _prune_inaccessible_children(schemas, field_metadata)
            self.assertNotIn("transactions", schemas)
            self.assertNotIn("transactions", field_metadata)

    def test_keep_child_when_parent_present(self):
        with mock.patch.object(Transaction, "parent_stream", "parent_stream_name"):
            schemas = {
                "parent_stream_name": {"type": "object"},
                "transactions": {"type": "object"},
            }
            field_metadata = {"parent_stream_name": {}, "transactions": {}}
            _prune_inaccessible_children(schemas, field_metadata)
            self.assertIn("transactions", schemas)
            self.assertIn("parent_stream_name", schemas)

    def test_stream_without_parent_not_pruned(self):
        schemas = {"transactions": {"type": "object"}}
        field_metadata = {"transactions": {}}
        _prune_inaccessible_children(schemas, field_metadata)
        self.assertIn("transactions", schemas)


class TestApplyAccessChecks(unittest.TestCase):

    def _get_schemas(self):
        from tap_braintree.schema import get_schemas
        return get_schemas()

    def test_all_accessible_no_streams_removed(self):
        schemas, field_metadata = self._get_schemas()
        with mock.patch("braintree.Transaction.search"):
            _apply_access_checks(schemas, field_metadata)
        self.assertIn("transactions", schemas)
        self.assertEqual(set(schemas.keys()), set(ALL_STREAM_NAMES))

    def test_all_inaccessible_raises_authorization_error(self):
        schemas, field_metadata = self._get_schemas()
        with mock.patch("braintree.Transaction.search", side_effect=AuthorizationError()):
            with self.assertRaises(AuthorizationError):
                _apply_access_checks(schemas, field_metadata)

    def test_inaccessible_stream_removed_from_schemas(self):
        schemas, field_metadata = self._get_schemas()
        with mock.patch("braintree.Transaction.search", side_effect=AuthorizationError()):
            with self.assertRaises(AuthorizationError):
                _apply_access_checks(schemas, field_metadata)
        self.assertNotIn("transactions", schemas)
        self.assertNotIn("transactions", field_metadata)

    def test_authentication_error_propagates(self):
        from braintree.exceptions.authentication_error import AuthenticationError
        schemas, field_metadata = self._get_schemas()
        with mock.patch("braintree.Transaction.search", side_effect=AuthenticationError()):
            with self.assertRaises(AuthenticationError):
                _apply_access_checks(schemas, field_metadata)


class TestDiscover(unittest.TestCase):

    def test_discover_returns_catalog_with_all_streams(self):
        with mock.patch("braintree.Transaction.search"):
            catalog = discover()
        stream_ids = {entry.tap_stream_id for entry in catalog.streams}
        self.assertEqual(stream_ids, set(ALL_STREAM_NAMES))

    def test_discover_all_forbidden_raises_authorization_error(self):
        with mock.patch("braintree.Transaction.search", side_effect=AuthorizationError()):
            with self.assertRaises(AuthorizationError):
                discover()

    def test_discover_catalog_entry_has_key_properties(self):
        with mock.patch("braintree.Transaction.search"):
            catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(entry.key_properties)
            self.assertIsInstance(entry.key_properties, list)

    def test_discover_catalog_entry_has_schema(self):
        with mock.patch("braintree.Transaction.search"):
            catalog = discover()
        for entry in catalog.streams:
            self.assertIsNotNone(entry.schema)

    def test_discover_returns_catalog(self):
        with mock.patch("braintree.Transaction.search"):
            catalog = discover()
        self.assertIsNotNone(catalog)

    def test_discover_transactions_stream_has_correct_tap_stream_id(self):
        with mock.patch("braintree.Transaction.search"):
            catalog = discover()
        tap_stream_ids = [entry.tap_stream_id for entry in catalog.streams]
        self.assertIn("transactions", tap_stream_ids)


if __name__ == "__main__":
    unittest.main(verbosity=2)
