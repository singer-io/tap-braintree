"""
Guards against regression where deselecting fields accidentally strips primary
or replication keys, which would break incremental state tracking entirely.
"""

from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import BraintreeBase


class BraintreeMinimumSelectionTest(MinimumSelectionTest, BraintreeBase):
    """
    id, created_at, and updated_at must survive any field-selection change
    because losing them silently corrupts incremental sync state.
    """

    @staticmethod
    def name():
        return "tt_braintree_auto"

    def streams_to_test(self):
        return self.expected_stream_names()
