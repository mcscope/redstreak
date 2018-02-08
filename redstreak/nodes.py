import attr


@attr.s
class Scan:
    data = attr.ib()
    _iter = attr.ib(init=False)

    def __attrs_post_init__(self):
        self._iter = iter(self.data)

    def __iter__(self):
        return self

    def __next__(self):
        return next(self._iter)


@attr.s
class Selection(Scan):
    predicate = attr.ib()

    def __next__(self):
        while True:
            # We DONT catch notimplemented so that it will pass up
            item = next(self._iter)
            if self.predicate(item):
                return item


@attr.s
class Order(Scan):
    key = attr.ib()

    def __attrs_post_init__(self):
        # I think ideally this is some kind of streaming sorting algorithm
        # but that seems quite hard right now. this will exhaust the data nodes
        if self.key:
            self._iter = iter(sorted(self.data, key=self.key))
        else:
            self._iter = iter(sorted(self.data))


@attr.s
class Limit(Scan):
    limit = attr.ib()
    count = attr.ib(default=0)

    def __next__(self):
        if self.count == self.limit:
            raise StopIteration
        self.count += 1
        return next(self._iter)
