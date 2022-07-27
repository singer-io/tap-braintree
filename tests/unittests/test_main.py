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
        
        
class TestMain(unittest.TestCase):
    @parameterized.expand([
        ["Main function for discover", [True, Mocked], None],
        ["Main function for sync mode", [False, Mocked], None],
        ["Main function for authentication error in sync", [False, AuthenticationError], None]
    ])
    @mock.patch("tap_braintree.json.dump")
    @mock.patch("tap_braintree._sync")
    @mock.patch("tap_braintree.discover")
    @mock.patch("tap_braintree.braintree.BraintreeGateway")
    @mock.patch("tap_braintree.utils.parse_args" ) 
    def test_main(self, test_name, test_value, expected_value, mocked_parse_args, mocked_gateway, mocked_discover, mocked_sync, mocked_json_dumps):
        """
        Test to verify that main function execute properly
        """
        Mocked.discover = test_value[0]
        mocked_parse_args.return_value = Mocked
        mocked_sync.side_effect = test_value[1]
        
        main()
        
        if test_value[0]:
            call_count = mocked_discover.call_count
        else:
            call_count = mocked_sync.call_count
            
        self.assertEqual(call_count, 1)
        