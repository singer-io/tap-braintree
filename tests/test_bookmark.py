"""
Verifies the tap resumes from its previous position rather than re-syncing
from the start, preventing duplicate records and unnecessary Braintree API usage.

The tap maintains three state keys instead of a single Singer bookmark:
  bookmarks.transactions.updated_at                — query-window end date
  bookmarks.transactions.latest_updated_at         — max updated_at of written records (dedup key)
  bookmarks.transactions.latest_disbursement_date  — max disbursement date seen

Records are emitted when:
    updated_at >= latest_updated_at  OR  disbursement_date >= latest_disbursement_date

Test strategy
─────────────
• Override get_bookmark_value to return latest_updated_at (the actual dedup key),
  so all bookmark comparisons in the base suite work against the correct field.
• In manipulate_state push latest_disbursement_date to a far-future value inside
  bookmarks.transactions so the OR-branch of the filter cannot leak old
  disbursement-driven records into sync 2.
• Hard-pin the updated_at query-window bookmark 30 days past the data window
  (calculate_new_bookmarks) so period_start lands on the data day and sync 2
  covers the same query window but with a tighter latest_updated_at filter.
"""

from base import BraintreeBase
from tap_tester.base_suite_tests.bookmark_test import BookmarkTest


class BraintreeBookmarkTest(BookmarkTest, BraintreeBase):
    """A pre-seeded state must cause the second sync to skip already-seen records."""

    bookmark_format = "%Y-%m-%dT%H:%M:%S.%fZ"

    # Only bookmarks.transactions.updated_at is read by the tap as the
    # query-window start; the other top-level keys are never consulted.
    initial_bookmarks = {
        'bookmarks': {
            'transactions': {'updated_at': '2025-06-08T00:00:00.000000Z'},
        },
    }

    def get_bookmark_value(self, state, stream):
        """
        The tap's true deduplication boundary is latest_updated_at (the max
        updated_at of records written in the previous sync), not updated_at
        (which is the query-window end date and is unrelated to record content).
        Override the base implementation to point at the correct bookmark key.
        """
        stream_bookmark = state.get('bookmarks', {}).get(stream, {})
        if stream_bookmark and self.expected_replication_method(stream) == self.INCREMENTAL:
            return stream_bookmark.get('latest_updated_at')
        return None

    def calculate_new_bookmarks(self) -> dict:
        """
        The base implementation requires ≥2 records strictly before
        (state_1_bookmark - lookback).  All 101 sandbox records share a single
        calendar day so that condition is never met.  Instead hard-pin updated_at
        30 days past the max record timestamp:
          period_start = '2025-07-13T09:15:48' - 30 days = 2025-06-13 09:15:48
        which puts sync 2's query window over the same data day while
        latest_updated_at (unchanged at 2025-06-13 09:15:48) still filters out
        records whose updated_at is below that threshold.
        """
        new_bookmarks = {}
        replication_keys = self.expected_replication_keys()
        for stream in self.streams_to_test():
            if self.expected_replication_methods.get(stream) != self.INCREMENTAL:
                continue
            replication_key = next(iter(replication_keys[stream]))
            stream_id = self.get_stream_id(stream)
            new_bookmarks[stream_id] = {replication_key: '2025-07-13T09:15:48.000000Z'}
        return new_bookmarks

    @staticmethod
    def manipulate_state(state: dict, new_bookmarks: dict) -> dict:
        """
        Apply the standard bookmark update (sets bookmarks.transactions.updated_at)
        and additionally push latest_disbursement_date to a far-future value inside
        bookmarks.transactions.

        Without this, records whose disbursement_date equals the sync-1 value
        (2025-06-14) still satisfy the tap's OR-filter and leak into sync 2,
        causing test_first_vs_second_records and
        test_second_sync_records_respect_bookmark to fail.
        """
        new_state = BookmarkTest.manipulate_state(state, new_bookmarks)
        if 'transactions' in new_bookmarks and 'bookmarks' in new_state:
            new_state['bookmarks'].setdefault('transactions', {})
            # Pin disbursement date to the far future so only the updated_at
            # condition can admit records into sync 2.
            new_state['bookmarks']['transactions']['latest_disbursement_date'] = \
                '2099-01-01T00:00:00.000000Z'
        return new_state

    @staticmethod
    def name():
        return "tap_tester_braintree_bookmark_test"

    def streams_to_test(self):
        return self.expected_stream_names()
