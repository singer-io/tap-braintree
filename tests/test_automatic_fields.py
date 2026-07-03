from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import BraintreeBase


class BraintreeMinimumSelectionTest(MinimumSelectionTest, BraintreeBase):
    """
    Only true automatic fields (primary and replication keys) must be present
    when syncing with minimum selection.
    """

    @staticmethod
    def name():
        return "tap_tester_braintree_automatic_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
