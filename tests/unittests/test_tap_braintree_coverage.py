import os
import unittest
from datetime import datetime
from unittest import mock

import braintree
import pytz
from braintree.exceptions import AuthenticationError
from singer.catalog import Catalog, CatalogEntry, Schema

import tap_braintree


class _DummyResult:
    def __init__(self, rows):
        self.maximum_size = len(rows)
        self._rows = rows

    def __iter__(self):
        return iter(self._rows)


class _DummyTransformerContext:
    def __init__(self, transformer):
        self._transformer = transformer

    def __enter__(self):
        return self._transformer

    def __exit__(self, exc_type, exc, tb):
        return False


class TestTapBraintreeCoverage(unittest.TestCase):
    def setUp(self):
        self.prev_config = dict(tap_braintree.CONFIG)
        self.prev_state = dict(tap_braintree.STATE)
        self.prev_catalog = tap_braintree.CATALOG
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CATALOG = None

    def tearDown(self):
        tap_braintree.CONFIG.clear()
        tap_braintree.CONFIG.update(self.prev_config)
        tap_braintree.STATE.clear()
        tap_braintree.STATE.update(self.prev_state)
        tap_braintree.CATALOG = self.prev_catalog

    def test_get_stream_metadata_map_hit(self):
        schema = Schema.from_dict({"type": "object", "properties": {"id": {"type": "string"}}})
        entry = CatalogEntry(
            stream="transactions",
            tap_stream_id="transactions",
            key_properties=["id"],
            schema=schema,
            metadata=[
                {"breadcrumb": [], "metadata": {"selected": True}},
                {"breadcrumb": ["properties", "id"], "metadata": {"inclusion": "automatic"}},
            ],
        )
        tap_braintree.CATALOG = Catalog([entry])

        mdata_map = tap_braintree.get_stream_metadata_map("transactions")
        self.assertTrue(mdata_map[()]["selected"])

    def test_get_abs_path_and_load_schema(self):
        abs_path = tap_braintree.get_abs_path("schemas/transactions.json")
        self.assertIn("schemas", abs_path)
        self.assertTrue(abs_path.endswith("transactions.json"))

        with mock.patch("tap_braintree.utils.load_json", return_value={"type": "object"}) as mock_load:
            value = tap_braintree.load_schema("transactions")
        self.assertEqual({"type": "object"}, value)
        mock_load.assert_called_once()

    @mock.patch("tap_braintree.logger.info")
    @mock.patch("tap_braintree.sync_transactions")
    def test_do_sync_logs_and_calls_sync_transactions(self, mock_sync_transactions, mock_log_info):
        tap_braintree.do_sync()
        mock_sync_transactions.assert_called_once_with()
        mock_log_info.assert_any_call("Starting sync")
        mock_log_info.assert_any_call("Sync completed")

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_record")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={"type": "object", "properties": {"id": {"type": "string"}}})
    @mock.patch("tap_braintree.daterange")
    @mock.patch("tap_braintree.get_transactions_data")
    def test_sync_transactions_covers_metadata_and_skip_branches(
            self,
            mock_get_data,
            mock_daterange,
            _mock_load_schema,
            _mock_write_schema,
            mock_write_record,
            _mock_write_state,
    ):
        tap_braintree.CONFIG["start_date"] = "2024-01-01T00:00:00Z"
        tap_braintree.STATE.update({
            "bookmarks": {
                "transactions": {
                    "latest_updated_at": "2024-01-10T00:00:00Z",
                    "latest_disbursement_date": "2024-01-10T00:00:00Z",
                    "updated_at": "2024-01-10T00:00:00Z",
                }
            }
        })

        start = datetime(2024, 1, 10, tzinfo=pytz.UTC)
        end = datetime(2024, 1, 11, tzinfo=pytz.UTC)
        mock_daterange.return_value = [(start, end)]

        row_write = mock.MagicMock()
        row_write.updated_at = None
        row_write.created_at = datetime(2024, 1, 11, 12, 0, 0)
        row_write.disbursement_details = mock.MagicMock()
        row_write.disbursement_details.disbursement_date = None

        row_skip = mock.MagicMock()
        row_skip.updated_at = datetime(2024, 1, 1, 0, 0, 0)
        row_skip.created_at = datetime(2024, 1, 1, 0, 0, 0)
        row_skip.disbursement_details = None

        mock_get_data.return_value = _DummyResult([row_write, row_skip])

        schema = Schema.from_dict({"type": "object", "properties": {"id": {"type": "string"}}})
        entry = CatalogEntry(
            stream="transactions",
            tap_stream_id="transactions",
            key_properties=["id"],
            schema=schema,
            metadata=[{"breadcrumb": [], "metadata": {"selected": True}}],
        )
        tap_braintree.CATALOG = Catalog([entry])

        transformer = mock.MagicMock()
        transformer.transform.side_effect = lambda rec, *_args, **_kwargs: rec

        with mock.patch("tap_braintree.transform_row", return_value={"id": "tx"}), \
                mock.patch("tap_braintree.singer.Transformer", return_value=_DummyTransformerContext(transformer)), \
                mock.patch("tap_braintree.utils.now", return_value=end):
            tap_braintree.sync_transactions()

        self.assertEqual(1, mock_write_record.call_count)
        transformer.transform.assert_called()

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_record")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={"type": "object", "properties": {"id": {"type": "string"}}})
    @mock.patch("tap_braintree.daterange")
    @mock.patch("tap_braintree.get_transactions_data")
    def test_sync_transactions_does_not_emit_older_updated_at_even_with_newer_disbursement(
            self,
            mock_get_data,
            mock_daterange,
            _mock_load_schema,
            _mock_write_schema,
            mock_write_record,
            _mock_write_state,
    ):
        tap_braintree.CONFIG["start_date"] = "2024-01-01T00:00:00Z"
        tap_braintree.STATE.update({
            "bookmarks": {
                "transactions": {
                    "latest_updated_at": "2024-01-10T00:00:00Z",
                    "latest_disbursement_date": "2024-01-10T00:00:00Z",
                    "updated_at": "2024-01-10T00:00:00Z",
                }
            }
        })

        start = datetime(2024, 1, 10, tzinfo=pytz.UTC)
        end = datetime(2024, 1, 11, tzinfo=pytz.UTC)
        mock_daterange.return_value = [(start, end)]

        row_old_updated_new_disb = mock.MagicMock()
        row_old_updated_new_disb.updated_at = datetime(2024, 1, 1, 0, 0, 0)
        row_old_updated_new_disb.created_at = datetime(2024, 1, 1, 0, 0, 0)
        row_old_updated_new_disb.disbursement_details = mock.MagicMock()
        row_old_updated_new_disb.disbursement_details.disbursement_date = datetime(2024, 1, 20).date()

        mock_get_data.return_value = _DummyResult([row_old_updated_new_disb])

        schema = Schema.from_dict({"type": "object", "properties": {"id": {"type": "string"}}})
        entry = CatalogEntry(
            stream="transactions",
            tap_stream_id="transactions",
            key_properties=["id"],
            schema=schema,
            metadata=[{"breadcrumb": [], "metadata": {"selected": True}}],
        )
        tap_braintree.CATALOG = Catalog([entry])

        transformer = mock.MagicMock()
        transformer.transform.side_effect = lambda rec, *_args, **_kwargs: rec

        with mock.patch("tap_braintree.transform_row", return_value={"id": "tx"}), \
                mock.patch("tap_braintree.singer.Transformer", return_value=_DummyTransformerContext(transformer)), \
                mock.patch("tap_braintree.utils.now", return_value=end):
            tap_braintree.sync_transactions()

        self.assertEqual(0, mock_write_record.call_count)

    @mock.patch("tap_braintree.utils.parse_args")
    def test_main_invalid_request_timeout_raises_value_error(self, mock_parse_args):
        args = mock.MagicMock()
        args.config = {
            "merchant_id": "m",
            "public_key": "p",
            "private_key": "k",
            "start_date": "2024-01-01T00:00:00Z",
            "request_timeout": "bad",
        }
        args.state = None
        args.catalog = None
        args.discover = False
        mock_parse_args.return_value = args

        with self.assertRaises(ValueError):
            tap_braintree.main.__wrapped__()

    @mock.patch("tap_braintree.logger.warning")
    @mock.patch("tap_braintree.do_sync")
    @mock.patch("tap_braintree.braintree.Configuration.configure")
    @mock.patch("tap_braintree.utils.parse_args")
    def test_main_timeout_zero_and_catalog_sync_path(self, mock_parse_args, _mock_configure, mock_do_sync, mock_warning):
        args = mock.MagicMock()
        args.config = {
            "merchant_id": "m",
            "public_key": "p",
            "private_key": "k",
            "start_date": "2024-01-01T00:00:00Z",
            "request_timeout": 0,
            "environment": "Production",
        }
        args.state = {"bookmarks": {"transactions": {"updated_at": "2024-01-01T00:00:00Z"}}}
        args.catalog = {"streams": []}
        args.discover = False
        mock_parse_args.return_value = args

        tap_braintree.main.__wrapped__()

        mock_do_sync.assert_called_once_with()
        self.assertTrue(mock_warning.called)

    @mock.patch("tap_braintree.braintree.Configuration.configure", side_effect=AuthenticationError())
    @mock.patch("tap_braintree.logger.critical")
    @mock.patch("tap_braintree.utils.parse_args")
    def test_main_handles_authentication_error(self, mock_parse_args, mock_critical, _mock_configure):
        args = mock.MagicMock()
        args.config = {
            "merchant_id": "m",
            "public_key": "p",
            "private_key": "k",
            "start_date": "2024-01-01T00:00:00Z",
        }
        args.state = None
        args.catalog = {"streams": []}
        args.discover = False
        mock_parse_args.return_value = args

        tap_braintree.main.__wrapped__()
        mock_critical.assert_called_once()

    @mock.patch("tap_braintree.braintree.Configuration.configure")
    @mock.patch("tap_braintree.do_sync", side_effect=TypeError("bad"))
    @mock.patch("tap_braintree.utils.parse_args")
    def test_main_raises_type_error_wrapper(self, mock_parse_args, _mock_do_sync, _mock_configure):
        args = mock.MagicMock()
        args.config = {
            "merchant_id": "m",
            "public_key": "p",
            "private_key": "k",
            "start_date": "2024-01-01T00:00:00Z",
        }
        args.state = None
        args.catalog = {"streams": []}
        args.discover = False
        mock_parse_args.return_value = args

        with self.assertRaises(TypeError):
            tap_braintree.main.__wrapped__()

    @mock.patch("tap_braintree.braintree.Configuration.configure")
    @mock.patch("tap_braintree.do_sync", side_effect=RuntimeError("boom"))
    @mock.patch("tap_braintree.utils.parse_args")
    def test_main_raises_runtime_error_wrapper(self, mock_parse_args, _mock_do_sync, _mock_configure):
        args = mock.MagicMock()
        args.config = {
            "merchant_id": "m",
            "public_key": "p",
            "private_key": "k",
            "start_date": "2024-01-01T00:00:00Z",
        }
        args.state = None
        args.catalog = {"streams": []}
        args.discover = False
        mock_parse_args.return_value = args

        with self.assertRaises(RuntimeError):
            tap_braintree.main.__wrapped__()

