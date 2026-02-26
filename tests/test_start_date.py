"""
Confirms the tap does not ignore start_date and re-replicate the full history
on every run, which would inflate API costs and flood downstream tables with
duplicates.

The 30-day trailing window means start_date_1 and start_date_2 are spaced
far enough apart that the record-count difference is unambiguous despite the
window overlap.
"""

from datetime import timedelta

from tap_tester.base_suite_tests.start_date_test import StartDateTest

from base import BraintreeBase

# Must match TRAILING_DAYS in tap_braintree/__init__.py
_TRAILING_DAYS = timedelta(days=30)


class BraintreeStartDateTest(StartDateTest, BraintreeBase):
    """A later start_date must yield a strict subset of the records from an earlier one."""

    @staticmethod
    def name():
        return "tt_braintree_start_date"

    def streams_to_test(self):
        return self.expected_stream_names()

    @property
    def start_date_1(self):
        return '2025-06-01T00:00:00Z'

    @property
    def start_date_2(self):
        return '2025-07-10T00:00:00Z'

    # -------------------------------------------------------------------------
    # Test overrides
    #
    # The base assertions compare record replication-key values directly against
    # start_date, but this tap queries [start_date - 30d, end_date], so records
    # older than start_date are legitimately present.  We override both
    # assertions to use the effective window start (start_date - TRAILING_DAYS).
    # -------------------------------------------------------------------------

    def _effective_start(self, start_date_str: str):
        """Return start_date shifted back by TRAILING_DAYS as a datetime."""
        return self.parse_date(start_date_str) - _TRAILING_DAYS

    def test_replication_key_values(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                replication_key = next(iter(self.expected_replication_keys(stream)))
                effective_start_1 = self._effective_start(self.start_date_1)
                effective_start_2 = self._effective_start(self.start_date_2)

                for sync_num, messages, effective_start in [
                    (1, StartDateTest.synced_messages_by_stream_1, effective_start_1),
                    (2, StartDateTest.synced_messages_by_stream_2, effective_start_2),
                ]:
                    for record in messages.get(stream, {}).get('messages', []):
                        if record.get('action') != 'upsert':
                            continue
                        record_date = self.parse_date(record['data'].get(replication_key))
                        with self.subTest(sync=f"sync{sync_num}", record_date=record_date):
                            self.assertGreaterEqual(
                                record_date, effective_start,
                                msg=f"sync {sync_num}: record {record_date} is before "
                                    f"effective window start {effective_start} "
                                    f"(start_date - {_TRAILING_DAYS.days}d)")

    def test_replicated_records(self):
        for stream in self.streams_to_test():
            with self.subTest(stream=stream):
                replication_key = next(iter(self.expected_replication_keys(stream)))
                primary_keys = self.expected_primary_keys(stream)
                effective_start_2 = self._effective_start(self.start_date_2)

                count_1 = StartDateTest.record_count_by_stream_1.get(stream, 0)
                count_2 = StartDateTest.record_count_by_stream_2.get(stream, 0)
                # Use assertGreaterEqual rather than assertGreater: if all sandbox
                # transactions happen to fall inside sync 2's effective window
                # (start_date_2 - 30d, end_date], both syncs return the same records
                # and strict inequality would be a false failure.  The pk assertSetEqual
                # below is the meaningful assertion.
                self.assertGreaterEqual(count_1, count_2,
                                   msg=f"sync 1 ({count_1}) should have at least as many records "
                                       f"as sync 2 ({count_2}) for stream {stream}")

                max_sync_1_date = max(
                    self.parse_date(r['data'][replication_key])
                    for r in StartDateTest.synced_messages_by_stream_1.get(
                        stream, {}).get('messages', [])
                    if r.get('action') == 'upsert')

                # Sync 1 records that fall inside sync 2's effective window
                pks_sync_1 = {
                    tuple(r['data'][pk] for pk in primary_keys)
                    for r in StartDateTest.synced_messages_by_stream_1.get(
                        stream, {}).get('messages', [])
                    if r.get('action') == 'upsert'
                    and self.parse_date(r['data'][replication_key]) >= effective_start_2}

                # Sync 2 records, excluding any added after sync 1 finished
                pks_sync_2 = {
                    tuple(r['data'][pk] for pk in primary_keys)
                    for r in StartDateTest.synced_messages_by_stream_2.get(
                        stream, {}).get('messages', [])
                    if r.get('action') == 'upsert'
                    and self.parse_date(r['data'][replication_key]) <= max_sync_1_date}

                self.assertSetEqual(
                    pks_sync_1, pks_sync_2,
                    msg=f"Records in sync 2 should match sync 1 records "
                        f"from effective window start {effective_start_2}")
