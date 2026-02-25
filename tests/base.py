"""
Centralises tap connection config and stream expectations so individual test
classes stay DRY and comparable across the suite.
"""
import os

from tap_tester.base_suite_tests.base_case import BaseCase


class BraintreeBase(BaseCase):
    """
    Single source of truth for tap identity, credentials, and stream contracts.
    Keeping these here means a schema change only needs to be fixed in one place.
    """

    # Chosen to coincide with a period that has known sandbox transactions.
    start_date = '2024-01-01T00:00:00Z'

    @staticmethod
    def tap_name():
        return "tap-braintree"

    @staticmethod
    def get_type():
        return "platform.braintree"

    def get_properties(self):
        return {
            'start_date': self.start_date,
            # Sandbox isolates test runs from production data and avoids
            # incurring real transaction costs during CI.
            'environment': 'Sandbox',
        }

    @staticmethod
    def get_credentials():
        return {
            'merchant_id': os.getenv('TAP_BRAINTREE_MERCHANT_ID'),
            'public_key': os.getenv('TAP_BRAINTREE_PUBLIC_KEY'),
            'private_key': os.getenv('TAP_BRAINTREE_PRIVATE_KEY'),
        }

    @staticmethod
    def expected_metadata():
        """
        The 30-day trailing window (TRAILING_DAYS) is intentional: it lets the
        tap catch late-settling disbursements without re-syncing all history.
        RESPECTS_START_DATE is True because records outside
        (start_date - TRAILING_DAYS, now] are never emitted.
        """
        return {
            'transactions': {
                BaseCase.PRIMARY_KEYS: {'id'},
                BaseCase.REPLICATION_METHOD: BaseCase.INCREMENTAL,
                BaseCase.REPLICATION_KEYS: {'updated_at'},
                BaseCase.RESPECTS_START_DATE: True,
            },
        }

    @staticmethod
    def expected_automatic_fields(stream=None):
        """
        Without id, records cannot be deduplicated downstream.
        Without created_at and updated_at, the tap cannot bound its query
        window or filter already-seen records, breaking incremental sync.
        """
        automatic_fields = {
            'transactions': {'id', 'created_at', 'updated_at'},
        }
        if stream:
            return automatic_fields[stream]
        return automatic_fields

    @classmethod
    def setUpClass(cls, logging="Ensuring environment variables are sourced."):  # pylint: disable=invalid-name
        super().setUpClass(logging=logging)
        missing_envs = [
            x for x in [
                'TAP_BRAINTREE_MERCHANT_ID',
                'TAP_BRAINTREE_PUBLIC_KEY',
                'TAP_BRAINTREE_PRIVATE_KEY',
            ] if os.getenv(x) is None
        ]

        if len(missing_envs) != 0:
            raise ValueError(f"Missing environment variables: {missing_envs}")
