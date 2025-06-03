import unittest
from unittest import mock
from tap_braintree import get_transactions_data
from braintree.exceptions import ServerError


class TestGetTransactionsDataBackoff(unittest.TestCase):
    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_retries_on_server_error(self, mock_transaction_search):
        """
        Test that get_transactions_data retries on transient ServerError and succeeds after retries.
        """
        mock_transaction_search.side_effect = [ServerError(), ServerError(), "success"]
        result = get_transactions_data("2021-01-01", "2021-01-31")

        self.assertEqual(result, "success")
        self.assertEqual(mock_transaction_search.call_count, 3)

    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_max_retries_reached(self, mock_transaction_search):
        """
        Test that get_transactions_data raises ServerError after exceeding maximum retry attempts.
        """
        mock_transaction_search.side_effect = ServerError()

        with self.assertRaises(ServerError):
            get_transactions_data("2021-01-01", "2021-01-31")

        self.assertEqual(mock_transaction_search.call_count, 5)
