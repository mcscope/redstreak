"""
An abstraction layer over our record reading/writing

Write now these are CSV's but soon enough they will be namedtuples that
write/read from disk in a binary format
"""

import csv
import tempfile


def readtups(file):
    file.seek(0)
    #  read a stream of tuples out of this
    return csv.DictReader(file)


def writetups(tups):
    file = tempfile.TemporaryFile(mode='w+')

    writer = csv.DictWriter(file, fieldnames=tups[0].keys())
    writer.writeheader()
    for tup in tups:
        writer.writerow(tup)
    return file


class Writer:
    def __init__(self):
        self.file = tempfile.TemporaryFile(mode='w+')
        self._writer = None

    def write(self, tups):
        if not self._writer:
            self._writer = csv.DictWriter(
                self.file,
                fieldnames=tups[0].keys())
            self._writer.writeheader()

        for tup in tups:
            self._writer.writerow(tup)
        tups.clear()
