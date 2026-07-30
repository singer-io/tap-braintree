import datetime
import unittest
from unittest import mock

from tap_braintree import transform as transform_module


class TestTransformModule(unittest.TestCase):
    def test_transform_row_maps_present_attributes(self):
        class Row:
            id = "123"
            amount = "4.5"

        row = Row()
        schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "amount": {"type": "number"},
                "missing": {"type": "string"},
            }
        }

        result = transform_module.transform_row(row, schema)
        self.assertEqual({"id": "123", "amount": 4.5}, result)

    def test_anyof_uses_first_matching_schema(self):
        value = transform_module._transform_field(
            "12", {"anyOf": [{"type": "integer"}, {"type": "string"}]}
        )
        self.assertEqual(value, 12)

    def test_anyof_raises_for_non_matching_data(self):
        with self.assertRaises(transform_module.InvalidData):
            transform_module._transform_field(
                "abc", {"anyOf": [{"type": "integer"}, {"type": "number"}]}
            )

    def test_array_and_object_transform(self):
        array_value = transform_module._transform_field(
            ["1", "2"], {"type": "array", "items": {"type": "integer"}}
        )
        self.assertEqual(array_value, [1, 2])

        class Row:
            foo = "3"

        row = Row()
        result = transform_module._transform_field(
            row,
            {
                "type": "object",
                "properties": {
                    "foo": {"type": "integer"},
                    "missing": {"type": "string"},
                },
            },
        )
        self.assertEqual(result, {"foo": 3})

    def test_type_transform_list_and_null_behaviors(self):
        self.assertEqual(transform_module._type_transform("5", ["integer", "string"]), 5)
        self.assertIsNone(transform_module._type_transform(None, "null"))
        with self.assertRaises(transform_module.InvalidData):
            transform_module._type_transform(None, "string")

    def test_type_transform_list_raises_when_all_types_fail(self):
        with self.assertRaises(transform_module.InvalidData):
            transform_module._type_transform("abc", ["integer", "number"])

    def test_type_transform_number_boolean_and_unknown(self):
        self.assertEqual(transform_module._type_transform("2.5", "number"), 2.5)
        self.assertTrue(transform_module._type_transform("any", "boolean"))
        with self.assertRaises(transform_module.InvalidData):
            transform_module._type_transform("value", "unknown")

    @mock.patch("tap_braintree.transform.utils.strftime", return_value="formatted")
    def test_transform_datetime_and_date_values(self, mock_strftime):
        dt_val = datetime.datetime(2024, 1, 1, 12, 30, 0)
        out_dt = transform_module._transform_field(dt_val, {"type": "string"})
        self.assertEqual(out_dt, "formatted")

        d_val = datetime.date(2024, 1, 2)
        out_date = transform_module._transform_field(d_val, {"type": "string"})
        self.assertEqual(out_date, "formatted")
        self.assertEqual(mock_strftime.call_count, 2)
