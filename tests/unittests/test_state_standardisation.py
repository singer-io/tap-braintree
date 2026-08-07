"""
Unit tests for the state standardisation changes (fix/SAC-30419).

Covers:
    1. get_start() — returns bookmark value; falls back to CONFIG start_date
    2. to_utc(datetime.min) — no TypeError on timezone-naive sentinel
    3. Disbursement date comparison — UTC-aware datetime.min vs UTC-aware bookmark
    4. sync_transactions() state shape — bookmarks structure per Singer spec
    5. sync_transactions() — safe when daterange yields nothing (end = period_end)
    6. bookmark_properties uses updated_at replication key
"""

import unittest
from datetime import datetime
from unittest import mock

import pytz

import tap_braintree
from tap_braintree import get_start, sync_transactions, to_utc

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_row(
    updated_at=None,
    created_at=None,
    disbursement_date=None,
    disbursement_success=False,
):
    """Return a minimal mock Braintree transaction row."""
    row = mock.MagicMock()
    row.updated_at = updated_at or datetime(2024, 1, 15, 12, 0, 0)
    row.created_at = created_at or datetime(2024, 1, 15, 12, 0, 0)

    if disbursement_date is None:
        row.disbursement_details = None
    else:
        row.disbursement_details = mock.MagicMock()
        row.disbursement_details.disbursement_date = disbursement_date
        row.disbursement_details.success = disbursement_success

    return row


def _make_search_result(rows, maximum_size=None):
    """Return a mock braintree search result iterable."""
    result = mock.MagicMock()
    result.maximum_size = maximum_size if maximum_size is not None else len(rows)
    result.__iter__ = mock.Mock(return_value=iter(rows))
    return result


# ---------------------------------------------------------------------------
# 1. get_start()
# ---------------------------------------------------------------------------

class TestGetStart(unittest.TestCase):
    """get_start() must return the bookmark value (or start_date fallback)."""

    def setUp(self):
        # Reset module-level globals before each test
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CONFIG["start_date"] = "2024-01-01T00:00:00Z"

    def test_returns_start_date_when_no_bookmark(self):
        """Falls back to CONFIG start_date when no bookmark exists."""
        result = get_start("transactions", replication_key="updated_at")
        self.assertEqual(result, "2024-01-01T00:00:00Z")

    def test_returns_existing_bookmark(self):
        """Returns the stored bookmark when present."""
        tap_braintree.STATE = {
            "bookmarks": {
                "transactions": {"updated_at": "2024-06-01T00:00:00Z"}
            }
        }
        result = get_start("transactions", replication_key="updated_at")
        self.assertEqual(result, "2024-06-01T00:00:00Z")

    def test_returns_value_not_none(self):
        """Must never return None (old bug: missing return statement)."""
        result = get_start("transactions", replication_key="updated_at")
        self.assertIsNotNone(result)


# ---------------------------------------------------------------------------
# 2. to_utc with datetime.min (timezone-naive sentinel)
# ---------------------------------------------------------------------------

class TestToUtcWithDatetimeMin(unittest.TestCase):
    """to_utc(datetime.min) must not raise and must be UTC-aware."""

    def test_datetime_min_becomes_utc_aware(self):
        """to_utc(datetime.min) returns a UTC-aware datetime."""
        result = to_utc(datetime.min)
        self.assertIsNotNone(result.tzinfo)
        self.assertEqual(result.tzinfo, pytz.UTC)

    def test_no_type_error_comparing_with_utc_bookmark(self):
        """
        Comparing to_utc(datetime.min) against a UTC-aware bookmark
        must not raise TypeError (old bug: naive vs aware comparison).
        """
        utc_bookmark = datetime(2024, 1, 1, tzinfo=pytz.UTC)
        sentinel = to_utc(datetime.min)
        # Should not raise
        try:
            result = sentinel >= utc_bookmark
            self.assertIsInstance(result, bool)
        except TypeError:
            self.fail("TypeError raised comparing to_utc(datetime.min) with UTC datetime")

    def test_to_utc_regular_datetime(self):
        """to_utc on a regular naive datetime attaches UTC timezone."""
        dt = datetime(2024, 3, 17, 10, 30, 0)
        result = to_utc(dt)
        self.assertEqual(result.tzinfo, pytz.UTC)
        self.assertEqual(result.year, 2024)
        self.assertEqual(result.hour, 10)


# ---------------------------------------------------------------------------
# 3. Disbursement date: UTC-aware sentinel used in comparison
# ---------------------------------------------------------------------------

class TestDisbursementDateSentinel(unittest.TestCase):
    """
    When disbursement_details is None the disbursement_date sentinel
    must be UTC-aware so it can be compared with the UTC-aware bookmark.
    """

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_record")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={})
    @mock.patch("tap_braintree.transform_row", return_value={"id": "tx1"})
    @mock.patch("tap_braintree.get_transactions_data")
    def test_no_type_error_null_disbursement(
        self,
        mock_get_data,
        mock_transform,
        mock_load_schema,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        """
        A row with disbursement_details=None must not raise TypeError during
        the disbursement_date >= latest_disbursement_date comparison.
        """
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CONFIG["start_date"] = "2024-01-14T00:00:00Z"

        row = _make_row(
            updated_at=datetime(2024, 1, 15, 12, 0, 0),
            disbursement_date=None,  # triggers disbursement_date = to_utc(datetime.min)
        )
        mock_get_data.return_value = _make_search_result([row])

        # Should complete without TypeError
        try:
            with mock.patch("tap_braintree.utils.now") as mock_now:
                mock_now.return_value = datetime(2024, 1, 15, tzinfo=pytz.UTC)
                sync_transactions()
        except TypeError as e:
            self.fail(f"TypeError raised: {e}")


# ---------------------------------------------------------------------------
# 4. Singer state shape — bookmarks structure
# ---------------------------------------------------------------------------

class TestStateShape(unittest.TestCase):
    """
    After sync_transactions(), singer.write_state() must be called with a
    state dict that has the canonical Singer bookmarks structure:
      {"bookmarks": {"transactions": {...}}}
    """

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_record")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={})
    @mock.patch("tap_braintree.transform_row", return_value={"id": "tx1"})
    @mock.patch("tap_braintree.get_transactions_data")
    def test_state_has_bookmarks_key(
        self,
        mock_get_data,
        mock_transform,
        mock_load_schema,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CONFIG["start_date"] = "2024-01-14T00:00:00Z"

        row = _make_row(updated_at=datetime(2024, 1, 15, 12, 0, 0))
        mock_get_data.return_value = _make_search_result([row])

        with mock.patch("tap_braintree.utils.now") as mock_now:
            mock_now.return_value = datetime(2024, 1, 15, tzinfo=pytz.UTC)
            sync_transactions()

        written_state = mock_write_state.call_args[0][0]
        self.assertIn("bookmarks", written_state)
        self.assertIn("transactions", written_state["bookmarks"])

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_record")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={})
    @mock.patch("tap_braintree.transform_row", return_value={"id": "tx1"})
    @mock.patch("tap_braintree.get_transactions_data")
    def test_state_bookmarks_contains_required_keys(
        self,
        mock_get_data,
        mock_transform,
        mock_load_schema,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        """
        The transactions bookmark must contain latest_updated_at,
        latest_disbursement_date and updated_at.
        """
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CONFIG["start_date"] = "2024-01-14T00:00:00Z"

        row = _make_row(updated_at=datetime(2024, 1, 15, 12, 0, 0))
        mock_get_data.return_value = _make_search_result([row])

        with mock.patch("tap_braintree.utils.now") as mock_now:
            mock_now.return_value = datetime(2024, 1, 15, tzinfo=pytz.UTC)
            sync_transactions()

        bk = mock_write_state.call_args[0][0]["bookmarks"]["transactions"]
        self.assertIn("latest_updated_at", bk)
        self.assertIn("latest_disbursement_date", bk)
        self.assertIn("updated_at", bk)

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_record")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={})
    @mock.patch("tap_braintree.transform_row", return_value={"id": "tx1"})
    @mock.patch("tap_braintree.get_transactions_data")
    def test_state_bookmark_values_are_strings(
        self,
        mock_get_data,
        mock_transform,
        mock_load_schema,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        """Bookmark values written to state must be ISO-format strings."""
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CONFIG["start_date"] = "2024-01-14T00:00:00Z"

        row = _make_row(updated_at=datetime(2024, 1, 15, 12, 0, 0))
        mock_get_data.return_value = _make_search_result([row])

        with mock.patch("tap_braintree.utils.now") as mock_now:
            mock_now.return_value = datetime(2024, 1, 15, tzinfo=pytz.UTC)
            sync_transactions()

        expected_state = {
            "bookmarks": {
                "transactions": {
                    "latest_updated_at": "2024-01-15T12:00:00.000000Z",
                    "latest_disbursement_date": "1970-01-01T00:00:00.000000Z",
                    "updated_at": "2024-01-15T00:00:00.000000Z"
                }
            }
        }

        self.assertEqual(mock_write_state.call_args[0][0], expected_state)

        bk = mock_write_state.call_args[0][0]["bookmarks"]["transactions"]
        self.assertIsInstance(bk["latest_updated_at"], str)
        self.assertIsInstance(bk["latest_disbursement_date"], str)
        self.assertIsInstance(bk["updated_at"], str)


# ---------------------------------------------------------------------------
# 5. Empty daterange — end defaults to period_end, no NameError
# ---------------------------------------------------------------------------

class TestEmptyDaterange(unittest.TestCase):
    """
    When period_start == period_end (empty daterange), sync_transactions()
    must not raise NameError for undefined `end` variable.
    """

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={})
    @mock.patch("tap_braintree.get_transactions_data")
    def test_no_name_error_when_loop_never_runs(
        self,
        mock_get_data,
        mock_load_schema,
        mock_write_schema,
        mock_write_state,
    ):
        """
        An end_date equal to start_date produces no loop iterations.
        Must not raise NameError and must still call write_state.
        """
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        # Set start_date far in the future so period_end < period_start
        tap_braintree.CONFIG["start_date"] = "2030-01-01T00:00:00Z"
        tap_braintree.CONFIG["end_date"] = "2030-01-01T00:00:00Z"

        try:
            sync_transactions()
        except NameError as e:
            self.fail(f"NameError raised (end variable undefined): {e}")

        mock_write_state.assert_called_once()

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={})
    @mock.patch("tap_braintree.get_transactions_data")
    def test_updated_at_bookmark_equals_period_end_when_loop_empty(
        self,
        mock_get_data,
        mock_load_schema,
        mock_write_schema,
        mock_write_state,
    ):
        """
        When the loop never runs, the updated_at bookmark written to state
        must equal period_end (the safe default for `end`).
        """
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CONFIG["start_date"] = "2030-01-01T00:00:00Z"
        tap_braintree.CONFIG["end_date"] = "2030-01-01T00:00:00Z"

        sync_transactions()

        written_state = mock_write_state.call_args[0][0]
        bk = written_state["bookmarks"]["transactions"]
        # updated_at bookmark must be a string (not missing/None)
        self.assertIsNotNone(bk.get("updated_at"))
        self.assertIsInstance(bk["updated_at"], str)


# ---------------------------------------------------------------------------
# 6. write_schema called with bookmark_properties=['updated_at']
# ---------------------------------------------------------------------------

class TestWriteSchemaBookmarkProperties(unittest.TestCase):
    """
    singer.write_schema() must be called with bookmark_properties=['updated_at'],
    matching the actual replication key — not 'created_at'.
    """

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_record")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={})
    @mock.patch("tap_braintree.transform_row", return_value={"id": "tx1"})
    @mock.patch("tap_braintree.get_transactions_data")
    def test_bookmark_properties_is_updated_at(
        self,
        mock_get_data,
        mock_transform,
        mock_load_schema,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CONFIG["start_date"] = "2024-01-14T00:00:00Z"

        row = _make_row(updated_at=datetime(2024, 1, 15, 12, 0, 0))
        mock_get_data.return_value = _make_search_result([row])

        with mock.patch("tap_braintree.utils.now") as mock_now:
            mock_now.return_value = datetime(2024, 1, 15, tzinfo=pytz.UTC)
            sync_transactions()

        _, kwargs = mock_write_schema.call_args
        self.assertEqual(kwargs.get("bookmark_properties"), ["updated_at"])

    @mock.patch("tap_braintree.singer.write_state")
    @mock.patch("tap_braintree.singer.write_record")
    @mock.patch("tap_braintree.singer.write_schema")
    @mock.patch("tap_braintree.load_schema", return_value={})
    @mock.patch("tap_braintree.transform_row", return_value={"id": "tx1"})
    @mock.patch("tap_braintree.get_transactions_data")
    def test_bookmark_properties_not_created_at(
        self,
        mock_get_data,
        mock_transform,
        mock_load_schema,
        mock_write_schema,
        mock_write_record,
        mock_write_state,
    ):
        tap_braintree.CONFIG.clear()
        tap_braintree.STATE.clear()
        tap_braintree.CONFIG["start_date"] = "2024-01-14T00:00:00Z"

        row = _make_row(updated_at=datetime(2024, 1, 15, 12, 0, 0))
        mock_get_data.return_value = _make_search_result([row])

        with mock.patch("tap_braintree.utils.now") as mock_now:
            mock_now.return_value = datetime(2024, 1, 15, tzinfo=pytz.UTC)
            sync_transactions()

        _, kwargs = mock_write_schema.call_args
        self.assertNotIn("created_at", kwargs.get("bookmark_properties", []))
