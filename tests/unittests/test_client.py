import unittest
from unittest import mock

import braintree

import tap_braintree


class TestClientAuth(unittest.TestCase):
    @mock.patch("tap_braintree.braintree.ClientToken.generate", return_value="token")
    @mock.patch("tap_braintree.discover")
    @mock.patch("tap_braintree.json.dump")
    def test_do_discover_success(self, mock_json_dump, mock_discover, _mock_generate):
        mock_catalog = mock.MagicMock()
        mock_catalog.to_dict.return_value = {"streams": []}
        mock_discover.return_value = mock_catalog

        tap_braintree.do_discover()

        mock_discover.assert_called_once()
        mock_json_dump.assert_called_once()

    @mock.patch(
        "tap_braintree.braintree.ClientToken.generate",
        side_effect=braintree.exceptions.authentication_error.AuthenticationError(),
    )
    def test_do_discover_authentication_error(self, _mock_generate):
        with self.assertRaises(Exception) as ctx:
            tap_braintree.do_discover()
        self.assertIn("Authentication error", str(ctx.exception))

    @mock.patch(
        "tap_braintree.braintree.ClientToken.generate",
        side_effect=RuntimeError("boom"),
    )
    def test_do_discover_unexpected_error(self, _mock_generate):
        with self.assertRaises(Exception) as ctx:
            tap_braintree.do_discover()
        self.assertIn("Unexpected error", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
