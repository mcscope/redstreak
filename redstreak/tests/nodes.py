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

        cut = nodes.Selection(example_str, predicate)
        self.assertEqual("".join(cut), "essng")

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

    @unittest.skip
    def test_selection_csv(self):
        def is_fantasy(row):
            return 'Fantasy' in row[2]

        cut = nodes.Selection(self.get_csv(), is_fantasy)

        got = list(itertools.islice(cut, 3))
        expected = [
            ["1",
             "Toy Story (1995)",
             "Adventure|Animation|Children|Comedy|Fantasy"],
            ["2",
             "Jumanji (1995)",
             "Adventure|Children|Fantasy"],
            [29,
             "City of Lost Children, The (Cité des enfants perdus, La) (1995)",
             "Adventure|Drama|Fantasy|Mystery|Sci-Fi"],
        ]

        self.assertEqual(got, expected)

    def get_csv(self):
        test_dir = os.path.dirname(__file__)
        hotelpath = os.path.join(test_dir, "data", "hotel.csv")
        with open(hotelpath, newline='') as csvfile:
            # Let's pull this all into memory so we can return the csv reader
            # cleanly
            fullcsv = csvfile.readlines()
            return csv.reader(fullcsv)


if __name__ == '__main__':

    unittest.main()
