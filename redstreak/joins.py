import attr
from collections import defaultdict
from redstreak.nodes import Scan


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


STOP_SIGNAL = object()
START_SIGNAL = object()


@attr.s
class BufferedIter:
    _iter = attr.ib()
    field = attr.ib()
    head = attr.ib(default=START_SIGNAL)
    matches = attr.ib(default=attr.Factory(list))
    matchfield = attr.ib(default=None)

    def __attrs_post_init__(self):
        self.fetch()

    def fetch(self):

        if self.head == STOP_SIGNAL:
            raise StopIteration
        if self.head == START_SIGNAL:
            self.head = next(self._iter)

        self.matches = []

        # TODO catch
        self.matchfield = self.head[self.field]
        while self.head[self.field] == self.matchfield:
            self.matches.append(self.head)
            try:
                self.head = next(self._iter)
            except StopIteration:
                self.head = STOP_SIGNAL
                return
            # todo catch StopIteration

        # TODO write to file if necessary


@attr.s
class SortMergeJoin(Scan):
    """
    The input to both of these must be sorted!
    That's the responsibility of the query planner!
    """
    REQUIRES_ORDERED_INPUT = True
    field = attr.ib()
    left = attr.ib()
    data = attr.ib()

    def join(self):

        try:
            left = BufferedIter(self.left, self.field)
            right = BufferedIter(self.data, self.field)
            while True:
                if right.matchfield == left.matchfield:
                    for left_record in left.matches:
                        for right_record in right.matches:
                            new_record = left_record.copy()
                            new_record.update(right_record)
                            yield new_record
                    right.fetch()
                    left.fetch()

                elif right.matchfield < left.matchfield:
                    right.fetch()
                else:
                    left.fetch()
        except StopIteration:
            # python 3.6ism. Generators return, don't stopiter
            return

    def __iter__(self):
        return self.join()

    def additional_explain(self):
        return f"\n{_sub_explain(self.left)}"
