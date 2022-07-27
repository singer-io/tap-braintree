import unittest
from tap_braintree import schema
from unittest import mock

class TestSchema(unittest.TestCase):

    def test_get_schemas(self):
        '''
        This test function verifies that proper schemas and metadata are returned by the get_schemas function
        '''

        return_schemas, return_metadata = schema.get_schemas()
        self.assertTrue(type(return_schemas), dict)
        self.assertTrue(type(return_metadata), dict)


    @mock.patch("os.path.join")
    def test_get_abs_path(self, mock_os_path):
        '''
        This test function is used to verify that we get proper path for given file
        '''

        mock_os_path.return_value = "abc/xyz"
        assert schema.get_abs_path("ab") == "abc/xyz"   #here "ab" is the name of the file