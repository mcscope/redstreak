import unittest
from unittest.mock import patch
import redstreak.sort as sort
import random


def make_sort_data(amount):
    letters = "qwertyuiopasdfghjklzxcvbnm"
    random.seed = 'outofcore'
    raw = [
        {
            'value': "".join([random.choice(letters) for _ in range(10)]),
            "other_attribute": "constant",
        }
        for _ in range(amount)]
    return raw


class TestSortNode(unittest.TestCase):

    def test_sort_key(self):
        raw = ["a", "abracadbra", "bd", "test", "testimony", ]
        raw = [{'value': val} for val in raw]

        expected = ["a", "abracadbra", "bd", "test", "testimony", ]
        expected = [{'value': val} for val in expected]

        self.assertEqual(list(sort.Sort('value', raw)),
                         expected)

    @patch('redstreak.sort.MAX_RECORD_LEN', 5)
    def test_small_out_of_core_sort(self):
        raw = make_sort_data(10)

        def expected_sort_key(item):
            return item['value']

        expected = sorted(raw, key=expected_sort_key)
        self.assertEqual(list(sort.Sort('value', raw)),
                         expected)

    @patch('redstreak.sort.MAX_RECORD_LEN', 45)
    def test_medium_out_of_core_sort(self):
        raw = make_sort_data(500)

        def expected_sort_key(item):
            return item['value']

        expected = sorted(raw, key=expected_sort_key)
        self.assertEqual(list(sort.Sort('value', raw)),
                         expected)

    @patch('redstreak.sort.MAX_RECORD_LEN', 1000)
    def test_huge_out_of_core_sort(self):
        """Wew"""
        raw = make_sort_data(100000)

        def expected_sort_key(item):
            return item['value']

        expected = sorted(raw, key=expected_sort_key)
        self.assertEqual(list(sort.Sort('value', raw)),
                         expected)


if __name__ == '__main__':
    unittest.main()
