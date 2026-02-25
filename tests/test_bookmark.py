"""
Verifies the tap resumes from its previous position rather than re-syncing
from the start, preventing duplicate records and unnecessary Braintree API usage.

The tap maintains three state keys instead of a single Singer bookmark because
updated_at is non-monotonic (settlements can update an old transaction) and
disbursement_date arrives out-of-order; tracking each independently avoids
missing late-settling records while still bounding the query window.
"""

from base import BraintreeBase
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class BraintreeBookmarkTest(BookmarkTest, BraintreeBase):
    """A pre-seeded state must cause the second sync to skip already-seen records."""

    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    # All three keys must be present because the tap only skips records that fall
    # below every threshold; an absent key is treated as epoch and defeats the test.
    initial_bookmarks = {
        'bookmarks': {
            'transactions': '2025-03-01T00:00:00.000000Z',
        },
        'latest_updated_at': '2025-03-01T00:00:00.000000Z',
        'latest_disbursement_date': '2025-03-01T00:00:00.000000Z',
    }

    @staticmethod
    def name():
        return "tt_braintree_bookmark"

    def streams_to_test(self):
        return self.expected_stream_names()
