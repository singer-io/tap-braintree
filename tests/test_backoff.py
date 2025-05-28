import unittest
from unittest import mock
from tap_braintree import get_transactions_data
from braintree.exceptions import ServerError


class TestGetTransactionsDataBackoff(unittest.TestCase):
    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_retries_on_server_error(
        self,
        mock_transaction_search
    ):
        # Configure the mock to raise ServerError on the first two calls, then succeed
        mock_transaction_search.side_effect = [ServerError(), ServerError(), "success"]
        result = get_transactions_data("2021-01-01", "2021-01-31")

        self.assertEqual(result, "success")
        self.assertEqual(mock_transaction_search.call_count, 3)

    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_max_retries_reached(
        self,
        mock_transaction_search
    ):
        # Configure the mock to always raise ServerError
        mock_transaction_search.side_effect = ServerError()

        with self.assertRaises(ServerError):
            get_transactions_data("2021-01-01", "2021-01-31")

        # Assuming the backoff decorator is set to max_tries=5
        self.assertEqual(mock_transaction_search.call_count, 5)
