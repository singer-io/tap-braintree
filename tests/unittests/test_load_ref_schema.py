import unittest
from tap_braintree import schema

class TestClass(unittest.TestCase):
    '''Test class for verifying working of load_shared_schema_refs function'''

    def test_load_shared_schema_refs(self):
        '''
        This test function verifies that schemas for references are resolved properly
        '''

        return_schema_refs = schema.load_shared_schema_refs()
        self.assertTrue(type(return_schema_refs), dict)
