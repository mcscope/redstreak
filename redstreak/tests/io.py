import unittest
from redstreak.io import readtups, writetups
import random


class TestIO(unittest.TestCase):

    def test_write_then_read(self):
        letters = "qwertyuiopasdfghjklzxcvbnm"
        random.seed = 'outofcore'
        raw = [
            {
                'value': "".join([random.choice(letters) for _ in range(10)]),
                "other_attribute": "constant",
            }
            for _ in range(10)]

        tmpfile = writetups(raw)
        got_tups = list(readtups(tmpfile))
        tmpfile.close()
        self.assertEqual((got_tups), raw)


if __name__ == '__main__':
    unittest.main()
