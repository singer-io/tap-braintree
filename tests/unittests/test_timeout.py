import unittest
from unittest import mock
from tap_braintree import main


class Mocked():
    ''' Class to initialize required variables'''
    def __init__(self):
        self.config = {
            "merchant_id": "test",
            "public_key": "test",
            "private_key": "test",
            "start_date": "2017-01-17T20:32:05Z"
        }
        self.state = {}
        self.catalog = {}
        self.discover = True
        self.environment = "Sandbox"


@mock.patch("tap_braintree.braintree.Environment")
@mock.patch("tap_braintree.do_discover")
@mock.patch("tap_braintree.braintree.Configuration.configure")
@mock.patch("tap_braintree.utils.parse_args")
class TestTimeout(unittest.TestCase):
    '''
    Test class to validate fetched timeout value
    '''

    def test_timeout_invalid_value_zero_integer(self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment):
        '''
        Test zero integer timeout should raise ValueError
        '''
        mocked_obj = Mocked()
        mocked_obj.config.update({"timeout": 0})
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        with self.assertRaises(ValueError) as e:
            main()

        self.assertEqual(str(e.exception), "Please provide a value greater than 0 for the timeout parameter in config")

    def test_timeout_invalid_value_string_zero(self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment):
        mocked_obj = Mocked()
        mocked_obj.config.update({"timeout": "0"})
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj
        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(str(e.exception), "Please provide a value greater than 0 for the timeout parameter in config")

    def test_timeout_invalid_value_invalid_string(self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment):
        mocked_obj = Mocked()
        mocked_obj.config.update({"timeout": "abc"})
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj
        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(str(e.exception), "Please provide a value greater than 0 for the timeout parameter in config")

    def test_timeout_valid_string_type_value_in_config(self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment):
        """
        Test when timeout is provided as a string in config
        """
        mocked_obj = Mocked()
        mocked_obj.config.update({"timeout": "200", "environment": "Sandbox"})

        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        main()

        mocked_configure.assert_called_with(
            mocked_environment.Sandbox,
            merchant_id="test",
            public_key="test",
            private_key="test",
            timeout=200.0
        )

    def test_timeout_valid_integer_type_value_in_config(self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment):
        """
        Test when timeout is provided as an integer in config
        """
        mocked_obj = Mocked()
        mocked_obj.config.update({"timeout": 200, "environment": "Sandbox"})

        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        main()

        mocked_configure.assert_called_with(
            mocked_environment.Sandbox,
            merchant_id="test",
            public_key="test",
            private_key="test",
            timeout=200.0
        )

    def test_timeout_no_value_in_config(self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment):
        """
        Test when timeout is not provided in config
        """
        mocked_obj = Mocked()
        mocked_obj.config.update({"environment": "Sandbox"})

        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        main()

        mocked_configure.assert_called_with(
            mocked_environment.Sandbox,
            merchant_id="test",
            public_key="test",
            private_key="test",
            timeout=300.0
        )


if __name__ == '__main__':
    unittest.main()
