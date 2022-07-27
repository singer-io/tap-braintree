import unittest
from tap_braintree import discover

class TestDiscover(unittest.TestCase):
    
    def test_discover(self):
        '''
        This test is used to verify that proper catalog is returned by discover function
        '''
        
        return_catalog = discover()
        self.assertTrue(type(return_catalog), dict)
    