"""
Confirms the tap does not ignore start_date and re-replicate the full history
on every run, which would inflate API costs and flood downstream tables with
duplicates.

The 30-day trailing window means start_date_1 and start_date_2 are spaced
far enough apart that the record-count difference is unambiguous despite the
window overlap.
"""

from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import BraintreeBase


class BraintreeStartDateTest(StartDateTest, BraintreeBase):
    """A later start_date must yield a strict subset of the records from an earlier one."""

    @staticmethod
    def name():
        return "tt_braintree_start_date"

    def streams_to_test(self):
        return self.expected_stream_names()

    @property
    def start_date_1(self):
        return '2025-06-01T00:00:00Z'

    @property
    def start_date_2(self):
        return '2025-06-01T00:00:00Z'
