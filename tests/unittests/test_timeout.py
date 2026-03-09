import unittest
from unittest import mock
from tap_braintree import main


class Mocked:
    """Class to initialize required variables"""

    def __init__(self):
        self.config = {
            "merchant_id": "test",
            "public_key": "test",
            "private_key": "test",
            "start_date": "2017-01-17T20:32:05Z",
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
    """
    Test class to validate fetched request_timeout value
    """

    def test_timeout_invalid_value_zero_integer(
        self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment
    ):
        """
        Test zero integer request_timeout should fallback on default value
        """
        mocked_obj = Mocked()
        mocked_obj.config.update({"request_timeout": 0, "environment": "Sandbox"})
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        main()

        mocked_configure.assert_called_with(
            mocked_environment.Sandbox,
            merchant_id="test",
            public_key="test",
            private_key="test",
            timeout=300,
        )

    def test_timeout_invalid_value_string_zero(
        self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment
    ):
        mocked_obj = Mocked()
        mocked_obj.config.update({"request_timeout": "0", "environment": "Sandbox"})
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        main()

        mocked_configure.assert_called_with(
            mocked_environment.Sandbox,
            merchant_id="test",
            public_key="test",
            private_key="test",
            timeout=300,
        )

    def test_timeout_invalid_value_invalid_string(
        self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment
    ):
        mocked_obj = Mocked()
        mocked_obj.config.update({"request_timeout": "abc"})
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj
        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(
            str(e.exception),
            "Please provide a positive number for `request_timeout`",
        )

    def test_timeout_no_value_in_config(
        self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment
    ):
        """
        Test when request_timeout is not provided in config
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
            timeout=300.0,
        )

    def test_timeout_negative_string(
        self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment
    ):
        """
        Test request_timeout as negative string falls back to default.
        """
        mocked_obj = Mocked()
        mocked_obj.config.update({"request_timeout": "-5", "environment": "Sandbox"})
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(
            str(e.exception),
            "Please provide a positive number for `request_timeout`",
        )

    def test_timeout_negative_integer(
        self, mocked_parse_args, mocked_configure, mocked_discover, mocked_environment
    ):
        """
        Test request_timeout as negative integer falls back to default.
        """
        mocked_obj = Mocked()
        mocked_obj.config.update({"request_timeout": -5, "environment": "Sandbox"})
        mocked_parse_args.return_value = mocked_obj
        mocked_environment.return_value = mocked_obj

        with self.assertRaises(ValueError) as e:
            main()
        self.assertEqual(
            str(e.exception),
            "Please provide a positive number for `request_timeout`",
        )
