import unittest
from unittest import mock
from parameterized import parameterized
from tap_braintree import main


class Mocked():
    ''' Class to initialize required variables'''
    def __init__(self):
        self.config = {"merchant_id": "test", "public_key": "test", "private_key": "test"}
        self.state = {}
        self.catalog = {}
        self.discover = True
        self.environment = "Sandbox"


@mock.patch("tap_braintree.braintree.Environment")
@mock.patch("tap_braintree.do_discover")
@mock.patch("tap_braintree.braintree.Configuration")
@mock.patch("tap_braintree.utils.parse_args")
class TestTimeout(unittest.TestCase):
    '''
    Test class to validate fetched timeout value
    '''
    @parameterized.expand([
        ['invalid_string_value_in_config', {"request_timeout" : "abc", "environment" : "Sandbox"}, None],
        ['integer_type_zero_value_in_config', {"request_timeout" : 0, "environment" : "Sandbox"}, None],
        ['string_type_zero_value_in_config', {"request_timeout" : "0", "environment" : "Sandbox"}, None]
    ])
    def test_request_timeout_invalid_value(self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment, name, test_value, expected_value):
        '''
        Test function to verify that when invalid value is provide as timeout value
        in config then error is raised
        '''
        mocked_obj = Mocked()
        mocked_obj.config = mocked_obj.config | test_value
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(str(e.exception), "Please provide a value greater than 0 for the request_timeout parameter in config")

    @parameterized.expand([
        ['timeout_value_not_in_config', {"environment" : "Sandbox"}, 300.0],
        ['string_type_timeout_value_in_config', {"request_timeout" : "200", "environment" : "Sandbox"}, 200.0],
        ['integer_type_timeout_value_in_config', {"request_timeout" : 200, "environment" : "Sandbox"}, 200.0]
    ])
    def test_request_timeout_valid_value(self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment, name, test_value, expected_value, ):
        """
        Test function to verify when valid timeout value is provided in config and when
        no value is provided in config
        """
        mocked_obj = Mocked()
        mocked_obj.config = mocked_obj.config | test_value
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        main()
        mocked_configure.assert_called_with(mocked_environment.Sandbox, merchant_id='test', public_key='test', private_key='test',timeout=expected_value)
