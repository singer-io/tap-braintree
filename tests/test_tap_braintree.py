import unittest
import tap_braintree
import pytz

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

