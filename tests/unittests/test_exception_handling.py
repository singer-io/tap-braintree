import unittest
from unittest import mock
from parameterized import parameterized
from braintree.exceptions.upgrade_required_error import UpgradeRequiredError
from braintree.exceptions.too_many_requests_error import TooManyRequestsError
from braintree.exceptions.server_error import ServerError
from braintree.exceptions.service_unavailable_error import ServiceUnavailableError
from braintree.exceptions.gateway_timeout_error import GatewayTimeoutError
from tap_braintree.streams import AddOn



class TestBraintreeAPIResponseException(unittest.TestCase):
    '''Test case to verify if errors are getting raised properly or not'''

    @parameterized.expand([
        ['too_many_requests_error', TooManyRequestsError],   #Error with status code 429
        ['server_error', ServerError],    #Error with status code 500
        ['service_unavailable_error', ServiceUnavailableError],    #Error with status code 503
        ['gateway_timeout_error', GatewayTimeoutError]     #Error with status code 504
        ])
    @mock.patch("tap_braintree.streams.AddOn.sdk_call")
    def test_raised_error(self, name, actual, mocked_sdk_call):
        '''Test function to test whether correct errors are getting raised or not'''

        mocked_sdk_call.side_effect = actual
        stream_obj = AddOn()
        with self.assertRaises(actual) as e:
            stream_obj.sync(
                gateway = "test",
                config = {"start_date": ""},
                schema = {},
                state = {},
                selected_streams = ["add_ons"]
            )

        self.assertEqual(type(e.exception), actual)
