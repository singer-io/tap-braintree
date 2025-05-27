import unittest
from unittest import mock
from tap_braintree import get_transactions_data
from braintree.exceptions import ServerError


class TestGetTransactionsDataBackoff(unittest.TestCase):
    # @mock.patch("tap_braintree.__init__.get_transactions_data")
    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_retries_on_server_error(
        self,
        mock_transaction_search,
        # mock_get_transactions_data
    ):
        # Configure the mock to raise ServerError on the first two calls, then succeed
        mock_transaction_search.side_effect = [ServerError(), ServerError(), "success"]

        result = get_transactions_data("2021-01-01", "2021-01-31")

        self.assertEqual(result, "success")
        # self.assertEqual(mock_get_transactions_data.call_count, 3)
