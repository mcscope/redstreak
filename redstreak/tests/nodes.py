import unittest
import itertools
import csv
import os
import redstreak.nodes as nodes
import redstreak.sort as sort
from redstreak.tests.shared import make_ratings


class TestRedStreakNodes(unittest.TestCase):

    def test_scan_str(self):
        example_str = 'test'
        cut = nodes.Scan(example_str)
        self.assertEqual("".join(cut), example_str)

    def test_selection_str(self):
        example_str = 'test string'

        def predicate(char):
            return char in "esng"

        cut = nodes.Selection(predicate, example_str)
        self.assertEqual("".join(cut), "essng")

    def test_selection_scan_str(self):
        example_str = 'test string'

        def predicate(char):
            return char in "esng"

        cut = nodes.Selection(predicate, nodes.Scan(example_str))
        self.assertEqual("".join(cut), "essng")

    def test_selection_recursive(self):

        def divis_2(num):
            return num % 2 == 0

        def divis_3(num):
            return num % 3 == 0

        cut = nodes.Selection(
            divis_2,
            nodes.Selection(
                divis_3,
                range(40)
            ))
        self.assertEqual(list(cut), [0, 6, 12, 18, 24, 30, 36])

    def test_scan_csv(self):
        cut = nodes.Scan(self.get_csv())

        got = list(itertools.islice(cut, 3))
        expected = [
            ["movieId", "title", "genres"],
            ["1",
             "Toy Story (1995)",
             "Adventure|Animation|Children|Comedy|Fantasy"],
            ["2", "Jumanji (1995)", "Adventure|Children|Fantasy"],
        ]
        self.assertEqual(got, expected)

    def test_selection_csv(self):
        def is_fantasy(row):
            return 'Fantasy' in row[2]

        cut = nodes.Selection(is_fantasy, self.get_csv())

        got = list(itertools.islice(cut, 3))
        expected = [
            ["1",
             "Toy Story (1995)",
             "Adventure|Animation|Children|Comedy|Fantasy"],
            ["2",
             "Jumanji (1995)",
             "Adventure|Children|Fantasy"],
            ["29",
             "City of Lost Children, The (Cité des enfants perdus, La) (1995)",
             "Adventure|Drama|Fantasy|Mystery|Sci-Fi"],
        ]

        self.assertEqual(got, expected)

    def test_sort(self):
        raw = iter([1, 50, 3, 2, 53, 23])
        self.assertEqual(list(sort.Sort(None, raw)),
                         [1, 2, 3, 23, 50, 53])

    def test_sort_key(self):
        raw = ["a", "abracadbra", "bd", "test", "testimony", ]
        expected = ["bd", "a", "test", "testimony", "abracadbra"]

        def vowel_count(string):
            count = 0
            for char in string:
                if char in 'aeiou':
                    count += 1
            return count

        self.assertEqual(list(sort.Sort(vowel_count, raw)),
                         expected)

    def test_limit(self):
        self.assertEqual(
            list(nodes.Limit(
                10,
                range(100))),
            list(range(10))
        )

    # def test_sum(self):
    #     self.assertEqual(
    #         list(nodes.Sum(range(101))),
    #         [5050])

    def test_field_sum(self):
        self.assert_drain(nodes.Sum('value',
                                    self.aggregate_node()),
                          {"Sum": 5050})

    def test_count(self):

        self.assert_drain(nodes.Count('value',
                                      self.aggregate_node()),
                          {"Count": 101})

    def test_mean(self):
        self.assert_drain(nodes.Mean('value',
                                     self.aggregate_node()),
                          {"Mean": 50.0})

    def assert_drain(self, node, value):
        self.assertEqual(list(node), [value])

    def aggregate_node(self):
        return ({'value': x} for x in range(101))

    def test_group_by_sum(self):
        test_values = [{"value": x, "operation": "solo"}
                       for x in range(11)]
        test_values += [{"value": x * 2, "operation": "double"}
                        for x in range(11)]
        test_values += [{"value": x * 3, "operation": "triple"}
                        for x in range(11)]
        expected = [
            {'Sum': 55, 'operation': "solo", },
            {'Sum': 110, 'operation': "double", },
            {'Sum': 165, 'operation': "triple", }
        ]

        result = nodes.Sum('value', test_values, group_by="operation")

        self.assertEqual(list(result), expected)

    def get_csv(self):
        test_dir = os.path.dirname(__file__)
        hotelpath = os.path.join(test_dir, "data", "movies.csv")
        with open(hotelpath, newline='') as csvfile:
            # Let's pull this all into memory so we can return the csv reader
            # cleanly
            fullcsv = csvfile.readlines()
            return csv.reader(fullcsv)

    def test_count_named_tuple(self):

        self.assert_drain(nodes.Count('value',
                                      make_ratings(count=101)),
                          {"Count": 101})

if __name__ == '__main__':

    unittest.main()
