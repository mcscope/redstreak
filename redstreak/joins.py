import attr
from nodes import Scan


@attr.s
class HashJoin(Scan):
    """
    TODO edit
    For this Hash Join join
    (written on the first day of class before the Join unit)
    (Turns out I wrote a nice hash join)
    We make a new record assuming that the
    left and right(data) match on a field exactly

    This relies on being able to hold the entirety of the left table in memory

    This join is nice because it's performance is O(M + N) lookups
    Which is awesome. LINEAR time!
    """
    field = attr.ib()
    left = attr.ib()
    data = attr.ib()

    def join(self):
        left = defaultdict(list)
        for item in self.left:
            left[item[self.field]].append(item)

        for item in self.data:
            matches = left[item[self.field]]
            for match in matches:
                new_record = item.copy()
                new_record.update(match)
                yield new_record

    def __iter__(self):
        return self.join()

    def additional_explain(self):
        return f"\n{_sub_explain(self.left)}"


@attr.s
class NestedLoopJoin(Scan):
    """
    This is a simple nested loops join.

    it has performance of (M * N) lookups, which is rotten
    There's plenty of performance improvements to be made by
    going a page/chunk at a time, but this one is nieve

    """
    field = attr.ib()
    left = attr.ib()
    data = attr.ib()

    def join(self):
        for left_record in self.left:
            for right_record in self.data:
                if left_record[self.field] == right_record[self.field]:
                    new_record = left_record.copy()
                    new_record.update(right_record)
                    yield new_record

            self.data.reset()

    def __iter__(self):
        return self.join()

    def additional_explain(self):
        return f"\n{_sub_explain(self.left)}"

# TODO sort merge join