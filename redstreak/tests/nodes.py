import unittest
import itertools
import csv
import os
import redstreak.nodes as nodes


class TestStringMethods(unittest.TestCase):

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

    def test_order(self):
        raw = [1, 50, 3, 2, 53, 23]
        self.assertEqual(list(nodes.Order(None, raw)),
                         [1, 2, 3, 23, 50, 53])

    def test_order_key(self):
        raw = ["a", "abracadbra", "bd", "test", "testimony", ]
        expected = ["bd", "a", "test", "testimony", "abracadbra"]

        def vowel_count(string):
            count = 0
            for char in string:
                if char in 'aeiou':
                    count += 1
            return count

        self.assertEqual(list(nodes.Order(vowel_count, raw)),
                         expected)

    def test_limit(self):
        self.assertEqual(
            list(nodes.Limit(
                10,
                range(100))),
            list(range(10))
        )

    def get_csv(self):
        test_dir = os.path.dirname(__file__)
        hotelpath = os.path.join(test_dir, "data", "movies.csv")
        with open(hotelpath, newline='') as csvfile:
            # Let's pull this all into memory so we can return the csv reader
            # cleanly
            fullcsv = csvfile.readlines()
            return csv.reader(fullcsv)


if __name__ == '__main__':

    unittest.main()
