import attr
import inspect
from textwrap import indent
from collections import defaultdict


def _sub_explain(data):
    if hasattr(data, 'explain'):
        substr = data.explain()
    else:
        substr = str(data)
    sublines = substr.splitlines()
    sublines[0] = "---> " + sublines[0]
    sublines = [sublines[0]] + ["\t" + line
                                for line in filter(None, sublines[1:])]

    return "\n".join(sublines)


@attr.s
class Scan:
    REQUIRES_ORDERED_INPUT = False
    data = attr.ib()
    _iter = attr.ib(init=False)

    def __attrs_post_init__(self):
        self._iter = iter(self.data)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iter)

    def reset(self):
        try:
            self.data.reset()
        except AttributeError:
            self._iter = iter(self.data)

    def additional_explain(self):
        return ""

    def explain(self):
        indented_sub = _sub_explain(self.data)
        return f"""{self.__class__.__name__}({indent(self.additional_explain(), "|  ")}
{indented_sub}
) """
# The jury is still out over which is more readable
# Node(args, data) or Node(data, args)
# The former is more readable e.g. limit 50 (select )
# The latter is more consistent.
# I'm leaning towards (args, data) so each subclass must redefine data
# in order to keep that ordering


@attr.s
class Selection(Scan):
    predicate = attr.ib()
    data = attr.ib()

    def __next__(self):
        while True:
            # We DONT catch notimplemented so that it will pass up
            item = next(self._iter)
            if self.predicate(item):
                return item

    def additional_explain(self):
        return "\nFilter:\n" + inspect.getsource(self.predicate)


@attr.s
class Limit(Scan):
    limit = attr.ib()
    data = attr.ib()
    count = attr.ib(default=0)

    def __next__(self):
        if self.count == self.limit:
            raise StopIteration
        self.count += 1
        return next(self._iter)

    def additional_explain(self):
        return f"\nlimit: {self.limit}"


SENTINAL_NO_PREVIOUS_TUPLE = "SENTINAL_NO_PREVIOUS_TUPLE"


@attr.s
class Aggregate(Scan):
    REQUIRES_ORDERED_INPUT = True
    field = attr.ib()
    data = attr.ib()
    group_by = attr.ib(default=None)

    _done = attr.ib(default=False, init=False)
    _drew = attr.ib(default=SENTINAL_NO_PREVIOUS_TUPLE, init=False)

    def aggregate_field(self, data):
        # todo this float conversion is wrong here. we should have good typing
        # all these aggregate fields are kinda dicy
        return self.aggregate([float(d[self.field]) for d in data])

    def __next__(self):
        if self._done:
            # we do this explicitly to keep track of counting 0
            #  vs having already counted everything
            raise StopIteration

        if not self.group_by:
            # TODO what to return
            self._done = True
            return {self.__class__.__name__: self.aggregate_field(self._iter)}

        else:
            # We required ordered, so draw until we get to a non-groupby
            buf = []
            buf_group_val = None

            try:
                # we may have a cached value from last round
                if self._drew == SENTINAL_NO_PREVIOUS_TUPLE:
                    self._drew = next(self._iter)
                buf_group_val = self._drew[self.group_by]

                while self._drew[self.group_by] == buf_group_val:
                    buf.append(self._drew)
                    self._drew = next(self._iter)
            except StopIteration:
                self._done = True

            # we've drawn all we can. time for math
            return {
                self.group_by: buf_group_val,
                self.__class__.__name__: self.aggregate_field(buf)
            }


@attr.s
class Sum(Aggregate):

    def aggregate(self, records):
        return sum(records)

    def additional_explain(self):
        return f"\nfield: {self.field}"


@attr.s
class Count(Aggregate):
    def aggregate(self, records):
        return len(records)


@attr.s
class Mean(Aggregate):
    def aggregate(self, records):
        count = 0
        total = 0
        for r in records:
            count += 1
            total += r
        if not count:
            raise StopIteration
        return total / count
