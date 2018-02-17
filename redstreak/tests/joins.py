import unittest
from redstreak.io import readtups, writetups
import random


class TestIO(unittest.TestCase):

    def test_nested_list(self):
        pass

    def test_hash_join(self):
        pass

    def test_sort_merge_join(self):
        pass

    def test_sort_merge_join_w_dupes(self):
        pass

        # letters = "qwertyuiopasdfghjklzxcvbnm"
        # random.seed = 'outofcore'
        # raw = [
        #     {
        #         'value': "".join([random.choice(letters) for _ in range(10)]),
        #         "other_attribute": "constant",
        #     }
        #     for _ in range(10)]

        # tmpfile = writetups(raw)
        # got_tups = list(readtups(tmpfile))
        # tmpfile.close()
        # self.assertEqual((got_tups), raw)


if __name__ == '__main__':
    unittest.main()
