import unittest
import redstreak.joins as joins
import redstreak.sort as sorts


class TestJoins(unittest.TestCase):
    def math_join_setup(self):

        left = ({"value": x, 'double': 2 * x}
                for x in range(0, 10))
        right = [{"value": x, 'triple': 3 * x}
                 for x in range(5, 15)]
        right.append({"value": 7, "quadruple": 7 * 4})
        expected = [
            {"value": 5, 'double': 10, "triple": 15},
            {"value": 6, 'double': 12, "triple": 18},
            {"value": 7, 'double': 14, "triple": 21},
            {"value": 7, 'double': 14, "quadruple": 28},
            {"value": 8, 'double': 16, "triple": 24},
            {"value": 9, 'double': 18, "triple": 27},
        ]
        return left, right, expected

    def test_nested_list(self):
        left, right, expected = self.math_join_setup()
        self.assertEqual(
            list(joins.NestedLoopJoin('value', left, right)),
            expected)

    def test_hash_join(self):
        left, right, expected = self.math_join_setup()
        self.assertEqual(
            list(joins.HashJoin('value', left, right)),
            expected)

    def test_sort_merge_join(self):
        left, right, expected = self.math_join_setup()
        got = list(joins.SortMergeJoin('value',
                                       sorts.Sort("value", left),
                                       sorts.Sort("value", right)))
        self.assertEqual(got, expected)

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
