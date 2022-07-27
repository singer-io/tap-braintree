import unittest
from unittest import mock
from parameterized import parameterized
from braintree.exceptions.upgrade_required_error import UpgradeRequiredError
from braintree.exceptions.too_many_requests_error import TooManyRequestsError
from braintree.exceptions.server_error import ServerError
from braintree.exceptions.down_for_maintenance_error import DownForMaintenanceError
from tap_braintree.streams import MerchantAccount



class TestBraintreeAPIResponseException(unittest.TestCase):
    '''Test case to verify if errors are getting raised properly or not'''

    @parameterized.expand([
        ['upgrade_required_error', [{'error':'upgrade required error'}, UpgradeRequiredError, 426]],
        ['too_many_requests_error', [{'error':'signature cannot be blank'}, TooManyRequestsError, 429]],
        ['server_error', [{'error':'signature cannot be blank'}, ServerError, 500]],
        ['service_unavailable_error', [{'error':'signature cannot be blank'}, DownForMaintenanceError, 503]],
        ])
    @mock.patch("tap_braintree.streams.MerchantAccount.sdk_call")
    def test_raised_error(self, name, actual, mocked_sdk_call):
        '''Test function to test whether correct errors are getting raised or not'''

        mocked_sdk_call.side_effect = actual[1]
        stream_obj = MerchantAccount()
        with self.assertRaises(actual[1]) as e:
            stream_obj.sync(
                gateway = "test",
                config = {"start_date": ""},
                schema = {},
                state = {},
                selected_streams = ["merchant_accounts"]
            )
        
        self.assertEqual(type(e.exception), actual[1])
