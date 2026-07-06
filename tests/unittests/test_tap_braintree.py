import unittest
from unittest import mock

import tap_braintree
import pytz

from datetime import datetime, timedelta

from braintree.exceptions.authentication_error import AuthenticationError
from braintree.exceptions.authorization_error import AuthorizationError


class TestDateRangeUtility(unittest.TestCase):

    def test_daterange_normal(self):
        """
        When given two dates 7 days apart, function should return
        generator that iterates 8 sets of tuples where the second
        value equals the next day's first.

        The last iteration should be the same as 
        (end_date, end_date + timedelta(1)), where the time portion
        of the date has been set to 0:00.
        """

        start_date = datetime(2018, 1, 1)
        end_date = start_date + timedelta(7)

        self.assertEqual(
            list(tap_braintree.daterange(start_date, end_date)),

            [
                 (datetime(2018, 1, 1, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 2, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 2, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 3, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 3, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 4, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 4, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 5, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 5, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 6, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 6, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 7, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 7, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 8, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 8, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 9, 0, 0, tzinfo=pytz.UTC))
            ]
        )

    def test_daterange_different_times(self):
        """
        When given two dates, 7 days apart, with random times within
        the day, generator should function in the same way as it would
        have if all the times were 0:00
        """

        start_date = datetime(2018, 1, 1, 10, 54, 23)
        end_date = datetime(2018, 1, 8, 2, 12, 45)

        self.assertEqual(
            list(tap_braintree.daterange(start_date, end_date)),

            [
                 (datetime(2018, 1, 1, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 2, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 2, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 3, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 3, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 4, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 4, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 5, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 5, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 6, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 6, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 7, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 7, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 8, 0, 0, tzinfo=pytz.UTC))
                ,(datetime(2018, 1, 8, 0, 0, tzinfo=pytz.UTC), datetime(2018, 1, 9, 0, 0, tzinfo=pytz.UTC))
            ]
        )


if __name__ == '__main__':
    unittest.main()


class TestDoDiscover(unittest.TestCase):

    def test_authentication_error_propagates_from_do_discover(self):
        """AuthenticationError raised by ClientToken.generate() must not be
        swallowed — it should propagate so main() can log it as critical."""
        with mock.patch(
            "braintree.ClientToken.generate",
            side_effect=AuthenticationError(),
        ):
            with self.assertRaises(AuthenticationError):
                tap_braintree.do_discover()

    def test_unexpected_error_wrapped_in_do_discover(self):
        """Non-auth exceptions from ClientToken.generate() are wrapped in a
        generic Exception with an informative message."""
        with mock.patch(
            "braintree.ClientToken.generate",
            side_effect=RuntimeError("boom"),
        ):
            with self.assertRaises(Exception) as ctx:
                tap_braintree.do_discover()
        self.assertIn("Unexpected error", str(ctx.exception))

    def test_valid_credentials_proceeds_to_discover(self):
        """When ClientToken.generate() succeeds, do_discover() completes without error."""
        # discover() is bound in __init__.py at import time, so we mock at the
        # lower level: braintree.Transaction.search to avoid real network calls.
        with mock.patch("braintree.ClientToken.generate"), \
             mock.patch("braintree.Transaction.search"):
            # Should not raise
            tap_braintree.do_discover()


class TestMainAuthErrors(unittest.TestCase):

    def _base_args(self):
        args = mock.MagicMock()
        args.config = {
            "merchant_id": "m",
            "public_key": "p",
            "private_key": "k",
            "start_date": "2020-01-01T00:00:00Z",
        }
        args.state = {}
        args.catalog = None
        args.discover = True
        return args

    def test_authentication_error_logged_as_critical(self):
        """AuthenticationError from Braintree triggers logger.critical with
        credential-check message."""
        with mock.patch("singer.utils.parse_args", return_value=self._base_args()), \
             mock.patch("braintree.Configuration.configure"), \
             mock.patch(
                 "tap_braintree.do_discover",
                 side_effect=AuthenticationError(),
             ), \
             mock.patch.object(tap_braintree.logger, "critical") as mock_crit:
            tap_braintree.main()
        mock_crit.assert_called_once()
        logged_msg = mock_crit.call_args[0][0]
        self.assertIn("merchant_id", logged_msg)

    def test_authorization_error_logged_as_critical(self):
        """AuthorizationError (HTTP 403) from Braintree triggers logger.critical
        with a 403/permission message."""
        with mock.patch("singer.utils.parse_args", return_value=self._base_args()), \
             mock.patch("braintree.Configuration.configure"), \
             mock.patch(
                 "tap_braintree.do_discover",
                 side_effect=AuthorizationError(),
             ), \
             mock.patch.object(tap_braintree.logger, "critical") as mock_crit:
            tap_braintree.main()
        mock_crit.assert_called_once()
        logged_msg = mock_crit.call_args[0][0]
        self.assertIn("403", logged_msg)
