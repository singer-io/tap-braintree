"""
Validates that catalog entries produced by discovery match the expected schema,
replication method, and key metadata so downstream consumers can rely on them
without inspecting the tap source.
"""

from tap_tester.base_suite_tests.discovery_test import DiscoveryTest

from base import BraintreeBase


class BraintreeDiscoveryTest(DiscoveryTest, BraintreeBase):
    """Standard Discovery Test"""

    expected_replication_keys = BraintreeBase.expected_replication_keys

    @staticmethod
    def name():
        return "tap_tester_braintree_discovery_test"

    def streams_to_test(self):
        return self.expected_stream_names()
