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
