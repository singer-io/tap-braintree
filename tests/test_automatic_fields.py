"""
Guards against regression where deselecting fields accidentally strips primary
or replication keys, which would break incremental state tracking entirely.

NOTE: Since do_discover() marks every field inclusion: automatic, deselecting
fields has no practical effect — this test effectively verifies all fields
are emitted, not just the minimum set.
"""

from tap_tester.base_suite_tests.automatic_fields_test import MinimumSelectionTest

from base import BraintreeBase


class BraintreeMinimumSelectionTest(MinimumSelectionTest, BraintreeBase):
    """
    All fields are automatic because do_discover() stamps every property with
    inclusion: automatic. Deselecting fields therefore has no practical effect,
    but the test still confirms the mechanism works end-to-end.
    """

    @staticmethod
    def name():
        return "tt_braintree_auto"

    def streams_to_test(self):
        return self.expected_stream_names()
