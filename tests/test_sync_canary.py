"""
Early-warning signal that catches environment or dependency breakage before
the heavier test suite runs — a canary that fails fast on obvious regressions.
"""

from tap_tester.base_suite_tests.sync_canary_test import SyncCanaryTest

from base import BraintreeBase


class BraintreeSyncCanaryTest(SyncCanaryTest, BraintreeBase):
    """The tap must produce at least one record to confirm end-to-end connectivity."""

    @staticmethod
    def name():
        return "tt_braintree_sync"

    def streams_to_test(self):
        return self.expected_stream_names()

    def setUp(self):  # pylint: disable=invalid-name
        self.start_date = '2025-06-01T00:00:00Z'
        super().setUp()
