"""
Verifies the transactions stream spans more than one page and does not
duplicate records while iterating across the Braintree SDK search results.
"""

from tap_tester.base_suite_tests.pagination_test import PaginationTest

from base import BraintreeBase


class BraintreePaginationTest(PaginationTest, BraintreeBase):
    """Pagination coverage for the only paginated Braintree stream."""

    PAGE_SIZE = 100

    @staticmethod
    def name():
        return "tap_tester_braintree_pagination_test"

    def streams_to_test(self):
        return self.expected_stream_names()
