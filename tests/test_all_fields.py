"""
Catches field-level omissions: if the tap silently drops a field during sync,
consumers lose data with no visible error until they notice missing columns.

NOTE: Discovery (schema.py) marks only the replication key as automatic and
sync_transactions() filters fields via singer.Transformer, so selection is
honoured. This test selects everything; test_automatic_fields.py selects the
minimum — together they bound the field-selection behaviour.
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import BraintreeBase


class BraintreeAllFieldsTest(AllFieldsTest, BraintreeBase):
    """Test that with all fields selected, all fields are replicated"""

    MISSING_FIELDS = {
        "transactions": {"paypal_details"},
    }

    @staticmethod
    def name():
        return "tap_tester_braintree_all_fields_test"

    def streams_to_test(self):
        return self.expected_stream_names()
