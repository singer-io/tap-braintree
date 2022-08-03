import unittest
from unittest import mock
from parameterized import parameterized
from braintree.exceptions.too_many_requests_error import TooManyRequestsError
from braintree.exceptions.server_error import ServerError
from braintree.exceptions.service_unavailable_error import ServiceUnavailableError
from braintree.exceptions.gateway_timeout_error import GatewayTimeoutError
from tap_braintree.streams import AddOn, Transaction


class TestBackoff(unittest.TestCase):
    '''
    Test that backoff logic works properly
    '''

    gateway = "test"
    config = {"start_date":"2022-08-01T00:00:00Z"}
    schema = {}
    state = {}

    @parameterized.expand([
        ['connection_error', ConnectionError, 5],
        ['too_many_requests_error', TooManyRequestsError, 5],
        ['server_error', ServerError, 5],
        ['service_unavailable_error', ServiceUnavailableError, 5],
        ['gateway_timeout_error', GatewayTimeoutError, 5]
        ])
    @mock.patch("tap_braintree.streams.AddOn.sdk_call")
    @mock.patch("time.sleep")
    def test_backoff_for_sync_without_window(self, name, test_exception, expected_count, mocked_time, mocked_sdk_call):
        '''Test function to verify working of backoff for sync of incremental streams without window'''

        stream_obj = AddOn()
        mocked_sdk_call.side_effect = test_exception('exception')

        with self.assertRaises(test_exception):
            stream_obj.sync(self.gateway, self.config, self.schema, self.state, ["add_ons"])
        self.assertEqual(mocked_sdk_call.call_count, expected_count)

    @parameterized.expand([
        ['connection_error', ConnectionError, 5],
        ['too_many_requests_error', TooManyRequestsError, 5],
        ['server_error', ServerError, 5],
        ['service_unavailable_error', ServiceUnavailableError, 5],
        ['gateway_timeout_error', GatewayTimeoutError, 5]
        ])
    @mock.patch("tap_braintree.streams.Transaction.sdk_call")
    @mock.patch("time.sleep")
    def test_backoff_for_sync_with_window(self, name, test_exception, expected_count, mocked_time, mocked_sdk_call):
        '''Test function to verify working of backoff for sync of incremental streams with window'''

        stream_obj = Transaction()
        mocked_sdk_call.side_effect = test_exception('exception')

        with self.assertRaises(test_exception):
            stream_obj.GetRecords(self.gateway, self.config["start_date"], self.config["start_date"])
        self.assertEqual(mocked_sdk_call.call_count, expected_count)
