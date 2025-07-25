import unittest
from unittest import mock
from tap_braintree import get_transactions_data

from braintree.exceptions.too_many_requests_error import TooManyRequestsError
from braintree.exceptions import ServerError
from braintree.exceptions.service_unavailable_error import ServiceUnavailableError
from braintree.exceptions.gateway_timeout_error import GatewayTimeoutError


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

    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_retries_on_connection_error(self, mock_transaction_search):
        """
        Test retries on ConnectionError and succeeds after retry.
        """
        mock_transaction_search.side_effect = [ConnectionError(), "success"]
        result = get_transactions_data("2021-01-01", "2021-01-31")
        self.assertEqual(result, "success")
        self.assertEqual(mock_transaction_search.call_count, 2)

    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_retries_on_too_many_requests_error(self, mock_transaction_search):
        """
        Test retries on TooManyRequestsError and succeeds.
        """
        mock_transaction_search.side_effect = [TooManyRequestsError(), "success"]
        result = get_transactions_data("2021-01-01", "2021-01-31")
        self.assertEqual(result, "success")
        self.assertEqual(mock_transaction_search.call_count, 2)

    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_retries_on_service_unavailable_error(self, mock_transaction_search):
        """
        Test retries on ServiceUnavailableError.
        """
        mock_transaction_search.side_effect = [ServiceUnavailableError(), "success"]
        result = get_transactions_data("2021-01-01", "2021-01-31")
        self.assertEqual(result, "success")
        self.assertEqual(mock_transaction_search.call_count, 2)

    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_retries_on_gateway_timeout_error(self, mock_transaction_search):
        """
        Test retries on GatewayTimeoutError and returns result.
        """
        mock_transaction_search.side_effect = [GatewayTimeoutError(), "success"]
        result = get_transactions_data("2021-01-01", "2021-01-31")
        self.assertEqual(result, "success")
        self.assertEqual(mock_transaction_search.call_count, 2)

    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_does_not_retry_on_unlisted_exception(self, mock_transaction_search):
        """
        Test that an unlisted exception does not trigger retry.
        """
        class CustomException(Exception):
            pass

        mock_transaction_search.side_effect = CustomException("unexpected")

        with self.assertRaises(CustomException):
            get_transactions_data("2021-01-01", "2021-01-31")

        self.assertEqual(mock_transaction_search.call_count, 1)

    @mock.patch("tap_braintree.braintree.Transaction.search")
    def test_success_without_error(self, mock_transaction_search):
        """
        Test function succeeds without any exception.
        """
        mock_transaction_search.return_value = "success"

        result = get_transactions_data("2021-01-01", "2021-01-31")
        self.assertEqual(result, "success")
        self.assertEqual(mock_transaction_search.call_count, 1)
