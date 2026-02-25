"""
Catches field-level omissions: if the tap silently drops a field during sync,
consumers lose data with no visible error until they notice missing columns.

NOTE: Since do_discover() marks every field inclusion: automatic, all fields
are replicated unconditionally. This test confirms the full field set arrives;
test_automatic_fields.py covers the same surface from the selection angle.
"""

from tap_tester.base_suite_tests.all_fields_test import AllFieldsTest

from base import BraintreeBase


class BraintreeAllFieldsTest(AllFieldsTest, BraintreeBase):
    """Test that with all fields selected, all fields are replicated"""

    selected_fields = AllFieldsTest.selected_fields

    @staticmethod
    def name():
        return "tt_braintree_all_fields"

    def streams_to_test(self):
        return self.expected_stream_names()
