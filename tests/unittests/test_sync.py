import unittest
from unittest import mock
from datetime import datetime

import pytz

import tap_braintree


class TestSyncBookmarks(unittest.TestCase):
    def setUp(self):
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CONFIG["start_date"] = "2024-01-01T00:00:00Z"

    def test_get_start_returns_config_default(self):
        result = tap_braintree.get_start("transactions", replication_key="updated_at")
        self.assertEqual(result, "2024-01-01T00:00:00Z")

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_record")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={})
    @mock.patch("tap_braintree.transform_row", return_value={"id": "tx1"})
    @mock.patch("tap_braintree.get_transactions_data")
    def test_sync_transactions_writes_bookmarks(
        self,
        mock_get_data,
        _mock_transform,
        _mock_load_schema,
        _mock_write_schema,
        _mock_write_record,
        mock_write_state,
    ):
        row = mock.MagicMock()
        row.updated_at = datetime(2024, 1, 2, 12, 0, 0)
        row.created_at = datetime(2024, 1, 2, 12, 0, 0)
        row.disbursement_details = None

        result = mock.MagicMock()
        result.maximum_size = 1
        result.__iter__ = mock.Mock(return_value=iter([row]))
        mock_get_data.return_value = result

        with mock.patch("tap_braintree.utils.now", return_value=datetime(2024, 1, 2, tzinfo=pytz.UTC)):
            tap_braintree.sync_transactions()

        self.assertIn("bookmarks", tap_braintree.STATE)
        tx_state = tap_braintree.STATE["bookmarks"]["transactions"]
        self.assertIn("updated_at", tx_state)
        self.assertIn("latest_updated_at", tx_state)
        self.assertIn("latest_disbursement_date", tx_state)
        mock_write_state.assert_called_once()


if __name__ == "__main__":
    unittest.main()
