import unittest
import redstreak.joins as joins
import redstreak.sort as sort
import redstreak.nodes as nodes


class TestJoins(unittest.TestCase):
    def math_join_setup(self):

        left = [{"value": x, 'double': 2 * x}
                for x in range(0, 10)]
        right = [{"value": x, 'triple': 3 * x}
                 for x in range(5, 15)]
        right.append({"value": 7, "quadruple": 7 * 4})
        left.append({"value": 7, "sextuple": 7 * 6})
        expected = [
            {"value": 5, 'double': 10, "triple": 15},
            {"value": 6, 'double': 12, "triple": 18},
            {"value": 7, 'double': 14, "triple": 21},
            {"value": 7, 'sextuple': 42, "triple": 21},
            {"value": 7, 'double': 14, "quadruple": 28},
            {"value": 7, 'sextuple': 42, "quadruple": 28},
            {"value": 8, 'double': 16, "triple": 24},
            {"value": 9, 'double': 18, "triple": 27},
        ]
        return left, right, expected

    def assert_sorted_equal(self, node, expected):
        listednode = list(node)
        # Comparing two lists of dictionaries in python to make
        # sure they have the same dicts is surprisingly tricky!
        self.assertEqual(
            sorted([sorted(d.items()) for d in listednode]),
            sorted([sorted(d.items()) for d in expected]),)

    def test_nested_list(self):
        left, right, expected = self.math_join_setup()
        self.assert_sorted_equal(
            joins.NestedLoopJoin('value', nodes.Scan(left), nodes.Scan(right)),
            expected)

    def test_hash_join(self):
        left, right, expected = self.math_join_setup()

        self.assert_sorted_equal(
            joins.HashJoin('value', left, right),
            expected)

    def test_sort_merge_join(self):
        left, right, expected = self.math_join_setup()
        self.assert_sorted_equal(
            joins.SortMergeJoin('value',
                                sort.Sort("value", left),
                                sort.Sort("value", right)),
            expected)


if __name__ == '__main__':
    unittest.main()
