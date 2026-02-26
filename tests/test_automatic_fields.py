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
    MISSING_FIELDS = {
        "transactions": {"paypal_details"},
    }

    def test_only_automatic_fields_replicated(self):
        """Override to subtract MISSING_FIELDS before comparing.

        paypal_details is in the schema (and therefore in expected_automatic_fields)
        but is only populated for PayPal-funded transactions which the sandbox cannot
        produce.  We exclude it from the expected set here just as AllFieldsTest does
        via MISSING_FIELDS.
        """
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                expected = (
                    self.expected_automatic_fields(stream)
                    - self.MISSING_FIELDS.get(stream, set())
                )
                fields_replicated = set(self.actual_field.get(stream, []))
                self.assertSetEqual(
                    fields_replicated, expected,
                    logging="verify only automatic fields are replicated")

    @staticmethod
    def name():
        return "tt_braintree_auto"

    def streams_to_test(self):
        return self.expected_stream_names()
