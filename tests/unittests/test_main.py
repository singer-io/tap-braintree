import unittest
from unittest import mock
from parameterized import parameterized
from braintree.exceptions.authentication_error import AuthenticationError
from tap_braintree import main

class Mocked():
    config = {"merchant_id": "test", "public_key": "test", "private_key": "test"}
    state = {}
    catalog = {}
    
    def __init__(self, *args):
        pass
        
        
@mock.patch("tap_braintree.json.dump")
@mock.patch("tap_braintree.braintree.BraintreeGateway")
@mock.patch("tap_braintree.utils.parse_args" )
class TestMain(unittest.TestCase):
    @parameterized.expand([
        ["Main function for sync mode", Mocked, None],
        ["Main function for authentication error in sync", AuthenticationError, None]
    ])
    @mock.patch("tap_braintree._sync")
    def test_main_for_sync(self, test_name, test_value, expected_value, mocked_sync, mocked_parse_args, mocked_gateway, mocked_json):
        """
        Test to verify that main function execute properly when sync mode execute
        """
        Mocked.discover = False
        mocked_parse_args.return_value = Mocked
        mocked_sync.side_effect = test_value
        main()
        call_count = mocked_sync.call_count
        self.assertEqual(call_count, 1)

    @mock.patch("tap_braintree.discover")
    def test_main_for_discover(self, mocked_discover, mocked_parse_args,  mocked_gateway, mocked_json):
        """
        Test to verify that main function execute properly when discover mode execute
        """
        Mocked.discover = True
        mocked_parse_args.return_value = Mocked
        main()
        call_count = mocked_discover.call_count
        self.assertEqual(call_count, 1)
