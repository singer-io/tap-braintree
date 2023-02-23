import unittest
import datetime
from parameterized import parameterized
from tap_braintree.transform import InvalidData
from tap_braintree import transform


class Mocked:
    '''Mocked class to set values for testing'''
    def __init__(self):
        self.string_data = "test"
        self.int_data = 123
        self.num_data = 12.3
        self.bool_data = True
        self.dt_data = datetime.datetime(2022, 1, 1, 00, 00, 00)

class TestTransform(unittest.TestCase):
    '''Test cases to verify proper working of transform functions'''

    def test_transform_row(self):
        '''Test case to verify proper transformation of row'''

        schema = {'properties':{},'type': ['null', 'string']}
        return_row = transform.transform_row(Mocked(), schema)
        self.assertTrue(type(return_row), dict)


    def test_invalid_data_error(self):
        '''Test case to verify only valid data gets transformed '''

        type_schema = ['string']
        with self.assertRaises(InvalidData) as e:
            transform._type_transform({}, type_schema)
        self.assertEqual(str(e.exception), "{} doesn\'t match any of ['string']")



    def test_object(self):
        '''Test case to verify proper transformation of object type data'''
        
        properties_schema = {'properties':{},'type': ['null', 'string']}
        return_data = transform._object(Mocked(), properties_schema)
        self.assertTrue(type(return_data), dict)


    def test_type_transform(self):
        '''Test case to verify proper transformation of data of different datatypes'''
        
        # Verify proper transformation of data of type string
        obj = Mocked()
        return_type = transform._type_transform(obj.string_data, 'string')
        self.assertTrue(type(return_type), str)

        # Verify proper transformation of data of type integer
        return_type = transform._type_transform(obj.int_data, 'integer')
        self.assertTrue(type(return_type), int)

        # Verify proper transformation of data of type float i.e. number
        return_type = transform._type_transform(obj.num_data, 'number')    
        self.assertTrue(type(return_type), float)
     
        # Verify proper transformation of data of type boolean
        return_type = transform._type_transform(obj.bool_data, 'boolean')    
        self.assertTrue(type(return_type), bool)


    @parameterized.expand([
        ['array_type',[[Mocked()],{"items": {"type":["null", "string"]}, "type": ["null", "array"]}],list],
        ['object_type',[Mocked(), {'properties':{},'type': ['null', 'object']}],dict],
        ])
    def test_transform_field(self, name, actual, expected): 
        '''Test case to verify proper transformation of fields within type object and array'''
       
        resp = transform._transform_field(actual[0], actual[1])
        self.assertEqual(type(resp),expected)

               

    