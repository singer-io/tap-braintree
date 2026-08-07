import unittest
import io
import tap_braintree
import pytz
import braintree

from unittest import mock
from singer.catalog import Catalog, CatalogEntry, Schema

from datetime import datetime, timedelta


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


class TestTapBraintreeHelpers(unittest.TestCase):
    def setUp(self):
        self.previous_catalog = tap_braintree.CATALOG

    def tearDown(self):
        tap_braintree.CATALOG = self.previous_catalog

    def test_get_stream_metadata_map_when_catalog_missing(self):
        tap_braintree.CATALOG = None
        self.assertIsNone(tap_braintree.get_stream_metadata_map("transactions"))

    def test_get_stream_metadata_map_when_stream_not_found(self):
        schema = Schema.from_dict({"type": "object", "properties": {"id": {"type": "string"}}})
        entry = CatalogEntry(
            stream="other",
            tap_stream_id="other",
            key_properties=["id"],
            schema=schema,
            metadata=[{"breadcrumb": [], "metadata": {}}],
        )
        tap_braintree.CATALOG = Catalog([entry])
        self.assertIsNone(tap_braintree.get_stream_metadata_map("transactions"))

    @mock.patch("tap_braintree.json.dump")
    @mock.patch("tap_braintree.discover")
    @mock.patch("tap_braintree.braintree.ClientToken.generate")
    def test_do_discover_happy_path(self, mock_generate, mock_discover, mock_json_dump):
        mock_generate.return_value = "token"
        mock_discover.return_value = mock.MagicMock(to_dict=lambda: {"streams": []})

        with mock.patch("tap_braintree.sys.stdout", new=io.StringIO()):
            tap_braintree.do_discover()

        mock_generate.assert_called_once()
        mock_json_dump.assert_called_once()

    @mock.patch(
        "tap_braintree.braintree.ClientToken.generate",
        side_effect=braintree.exceptions.authentication_error.AuthenticationError(),
    )
    def test_do_discover_authentication_error(self, _mock_generate):
        with self.assertRaises(Exception) as error:
            tap_braintree.do_discover()
        self.assertIn("Authentication error", str(error.exception))

    @mock.patch(
        "tap_braintree.braintree.ClientToken.generate",
        side_effect=RuntimeError("boom"),
    )
    def test_do_discover_unexpected_error(self, _mock_generate):
        with self.assertRaises(Exception) as error:
            tap_braintree.do_discover()
        self.assertIn("Unexpected error", str(error.exception))

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

