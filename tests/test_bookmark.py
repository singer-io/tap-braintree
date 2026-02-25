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

    initial_bookmarks = {
        'bookmarks': {
            'transactions': {'updated_at': '2025-06-08T00:00:00.000000Z'},
        },
        'transactions': '2025-06-01T00:00:00.000000Z',
        'latest_updated_at': '2025-03-01T00:00:00.000000Z',
        'latest_disbursment_date': '2025-03-01T00:00:00.000000Z',
    }

    def calculate_new_bookmarks(self) -> dict:
        """
        The base implementation requires ≥2 records with distinct updated_at values
        strictly before (state_1_bookmark - lookback), which is rarely guaranteed over
        a 14-day sandbox window.  Instead we hard-pin the bookmark to the mid-point of
        the test window so sync 2 always has records to replay.
        """
        new_bookmarks = {}
        replication_keys = self.expected_replication_keys()
        for stream in self.streams_to_test():
            if self.expected_replication_methods.get(stream) != self.INCREMENTAL:
                continue
            replication_key = next(iter(replication_keys[stream]))
            stream_id = self.get_stream_id(stream)
            # Set bookmark to mid-window so sync 2 re-plays the second half of June.
            new_bookmarks[stream_id] = {replication_key: '2025-06-08T00:00:00.000000Z'}
        return new_bookmarks

    @staticmethod
    def manipulate_state(state: dict, new_bookmarks: dict) -> dict:
        """
        Delegate to the base implementation (which updates bookmarks.*) and then
        mirror the new updated_at value into the tap's own top-level state keys so
        that sync 2 actually starts from the reduced bookmark position.
        """
        new_state = BookmarkTest.manipulate_state(state, new_bookmarks)
        if 'transactions' in new_bookmarks:
            updated_at = new_bookmarks['transactions'].get('updated_at')
            if updated_at:
                new_state['latest_updated_at'] = updated_at
                # The tap reads this key with the historic typo ('disbursment')
                new_state['latest_disbursment_date'] = updated_at
        return new_state

    @staticmethod
    def name():
        return "tt_braintree_bookmark"

    def streams_to_test(self):
        return self.expected_stream_names()

    def _assert_bookmark_equals_max_updated_at(self, synced_records, state, label):
        replication_key = next(iter(self.expected_replication_keys('transactions')))
        records = [
            r['data'] for r in
            synced_records.get('transactions', {}).get('messages', [])
            if r.get('action') == 'upsert']
        if not records:
            return
        max_value = max(self.parse_date(r[replication_key]) for r in records)
        bookmark = self.parse_date(state.get('latest_updated_at'))
        self.assertGreaterEqual(
            bookmark, max_value,
            msg=f"{label}: latest_updated_at ({bookmark}) should be >= max {replication_key} ({max_value})")

    def test_first_sync_bookmark(self):
        self._assert_bookmark_equals_max_updated_at(
            self.synced_records_1, self.state_1, "sync 1")

    def test_second_sync_bookmark(self):
        self._assert_bookmark_equals_max_updated_at(
            self.synced_records_2, self.state_2, "sync 2")
