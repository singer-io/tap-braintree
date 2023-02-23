import pytz
import unittest
from datetime import datetime
from unittest.mock import patch, call
from tap_braintree import _sync
from tap_braintree.streams import MerchantAccount, AddOn, SettlementBatchSummary, Transaction, Subscription
from parameterized import parameterized

class Mocked:
    
    def __init__(self, stream=None, created_at=None, updated_at=None, disbursement_date=None):
        self.stream = stream
        self.schema = Mocked
        self.created_at = created_at
        self.updated_at = updated_at
        self.disbursement_details = self if disbursement_date else None
        self.disbursement_date = disbursement_date
        
    def get_selected_streams(state):
        return (Mocked(i) for i in ["add_ons", "customers"])
    
    def get_stream(stream_name):
        return Mocked()
    
    def to_dict():
        return {}

class TestSyncMode(unittest.TestCase):
    @patch("tap_braintree.LOGGER.info")
    @patch("tap_braintree.streams.FullTableSync.sync", return_value=10)
    @patch("tap_braintree.streams.SyncWithWindow.sync", return_value=10)
    @patch("tap_braintree.streams.SyncWithoutWindow.sync", return_value=10)
    def test_sync(self, mocked_sync_without_window, mocked_sync_with_window, mocked_sync_full_table, mocked_LOGGER):
        """
        Test to verify that sync function run properly based on its logger calling
        """
        
        _sync("test_gateway", {}, Mocked, {})
        expected = [
            call('Starting Sync'),
            call("selected_streams: ['add_ons', 'customers']"),
            call('stream: add_ons, parent: None'),
            call('stream: customers, parent: None'),
            call("Sync Streams: ['add_ons', 'customers']"),
            call('START Syncing: add_ons'),
            call('FINISHED Syncing: add_ons, total_records: 10'),
            call('START Syncing: customers'),
            call('FINISHED Syncing: customers, total_records: 10'),
            call('discounts: Skipping - not selected'),
            call('plans: Skipping - not selected'),
            call('transactions: Skipping - not selected'),
            call('Finished sync')
        ]
        self.assertEqual(mocked_LOGGER.mock_calls,  expected, "Logger calls are not as expected")
        
    # @patch("tap_braintree.streams.MerchantAccount.sdk_call", return_value=["test", "test", "test"])
    # @patch("tap_braintree.streams.transform_row", return_value={'currency_iso_code': 'USD', 'default': True, 'id': 'cds', 'status': 'active'})
    # def test_full_table_sync(self, mocked_transform_row, mocked_sdk_call):
    #     """
    #     Test to verify that syncing return expected number of records for FULL_TABLE streams
    #     """
        
    #     stream_obj = MerchantAccount()
    #     record_counts = stream_obj.sync(
    #         gateway = "test",
    #         config = {"start_date": ""},
    #         schema = {},
    #         state = {},
    #         selected_streams = ["merchant_accounts"]
    #     )
        
    #     self.assertEqual(mocked_transform_row.call_count, 3, "Not getting expected number of calls")
    #     self.assertEqual(record_counts, 3, "Not getting expected number of records")
    
    @parameterized.expand([
        ['with_state', {"bookmarks": {"add_ons": {"updated_at": "2022-06-28T00:00:00.000000Z"}}}, None],
        ['without_state', {}, None],
    ])   
    @patch("tap_braintree.streams.AddOn.sdk_call", return_value=[Mocked(None, datetime(2022, 5, 29, 11, 46, 12)), Mocked(None, datetime(2022, 6, 29, 11, 46, 12)), Mocked(None, datetime(2022, 6, 29, 11, 46, 12), datetime(2022, 6, 29, 11, 46, 12))])
    @patch("tap_braintree.streams.transform_row", return_value="test_data")
    def test_sync_without_window(self, name, test_data, expected_data, mocked_transform_row, mocked_sdk_call):
        """
        Test to verify that syncing without date window return expected number of records for INCREMENTAL streams
        """
        
        stream_obj = AddOn()
        record_counts = stream_obj.sync(
            gateway = "test",
            config = {"start_date": "2022-06-25T00:00:00Z"},
            schema = {},
            state = test_data,
            selected_streams = ["add_ons"]
        )
        
        self.assertEqual(record_counts, 2, "Not getting expected number of the records")
        self.assertEqual(mocked_transform_row.call_count, 2, "Not getting expected number of calls")
    
    @patch("tap_braintree.streams.utils.now", return_value=datetime(2022,6,30,00,00,00).replace(tzinfo=pytz.UTC))   
    @patch("tap_braintree.streams.transform_row", return_value="test_data")
    @patch("tap_braintree.streams.Transaction.sdk_call", return_value=[Mocked(None, datetime(2022, 5, 29, 11, 46, 12)), Mocked(None, datetime(2022, 6, 29, 11, 46, 12)), Mocked(None, datetime(2022, 6, 29, 11, 46, 12), datetime(2022, 6, 29, 11, 46, 12), datetime(2022, 6, 29, 11, 46, 12))])
    def test_sync_with_window_for_transactions(self, mocked_sdk_call, mocked_transform_row, mocked_utils_now):
        """
        Test to verify that syncing with date window return expected number of records for INCREMENTAL streams
        """
        
        stream_obj = Transaction()
        record_counts = stream_obj.sync(
            gateway = "test",
            config = {"start_date": "2022-06-25T00:00:00Z"},
            schema = {},
            state = {
                "bookmarks": {
                    "transactions": {
                        "latest_updated_at": "2022-06-18T07:13:21.000000Z",
                        "latest_disbursement_date": "2022-06-18T00:00:00.000000Z",
                        "created_at": "2022-05-28T11:58:25.385256Z"
                    }
                },
            },
            selected_streams = ["transactions"]
        )
        
        self.assertEqual(record_counts, 2, "Not getting expected number of the records")
        self.assertEqual(mocked_transform_row.call_count, 192, "Not getting expected number of calls")
