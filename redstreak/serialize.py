"""
An abstraction layer over our record reading/writing

Write now these are CSV's but soon enough they will be namedtuples that
write/read from disk in a binary format
"""

import csv
import tempfile
import attr

from construct import Struct, Int16ub, Float32b, Int32ub, Rebuild, Byte, this
from construct import NamedTuple, Tell, Array, StreamError, FormatFieldError
from construct import CString
from redstreak.nodes import Scan

MAX_PAGE_BYTES = 4096


class FatPageException(Exception):
    pass


def readtups(file):
    file.seek(0)
    #  read a stream of tuples out of this
    return csv.DictReader(file)


def writetups(tups):

    raise NotImplementedError

    file = tempfile.TemporaryFile(mode='w+')

    writer = csv.DictWriter(file, fieldnames=tups[0].keys())
    writer.writeheader()
    for tup in tups:
        writer.writerow(tup)
    return file


class Writer:
    def __init__(self):
        raise NotImplementedError

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


def make_page_for_record_construct(record_construct):
    RecordHeader = Struct(
        "transaction" / Int16ub,
        "bitfield" / Int16ub,
    )

    RecordWrapper = Struct(
        "start" / Tell,
        "header" / RecordHeader,
        "record" / record_construct,
        "end" / Tell
    )

    # THIS LIBRARY IS VERY COMPLEX. MAKE ANY CHANGES HERE WITH CAUTION
    return Struct(
        # TODO these could probably be a nested/embedded substruct,
        # that might be useful for index parsing
        "count" / Rebuild(Int16ub, lambda this: len(this['records'])),
        "records" / Array(this.count, RecordWrapper),
        "padding_size" / Rebuild(
            Int16ub,
            lambda this: (
                4096 - (2 + 4 * this.count) - this['records'][-1]["end"])),

        Rebuild(Array(this.padding_size, Byte),
                lambda this: [0 for _ in range(this.padding_size)]),
        'line_pointers' / Rebuild(Array(this.count, Int16ub >> Int16ub),
                                  lambda this: [
                                  [r['start'], r['end'] - r['start']]
                                  for r in this.records]),
    )


RatingConstruct = NamedTuple("Rating",
                             "userId movieId rating timestamp",
                             Int32ub >> Int32ub >> Float32b >> Int32ub
                             )

MovieConstruct = NamedTuple("Movie",
                            "movieId title genres",
                            Int32ub >> CString("utf8") >> CString("utf8")
                            )


# SCHEMA TABLE
#  RIGHT NOW THESE ARE MANUALLY DEFINED NAMEDTUPLE CONSTRUCTS
#  We make a page/record/recordheader etc blah for each type
#  keep in a schema dictionary keyed like this
#  recordtype (the namedtuple): Page
#  we can get the corrusponding page by doing
#  PAGES[type(record)]

# These are rough estimates
ROW_HEADER_BYTES = 8
PAGE_HEADER_BYTES = 8

SCHEMA_CONSTRUCTS = [RatingConstruct, MovieConstruct]
SCHEMA_CONSTRUCT_MAP = {record_construct.factory: record_construct
                        for record_construct in SCHEMA_CONSTRUCTS}


@attr.s
class Table:
    factory = attr.ib()
    name = attr.ib()
    homefile = attr.ib()


TABLES = {
    record_construct.factory.__name__: Table(
        factory=record_construct.factory,
        name=record_construct.factory.__name__,
        homefile=record_construct.factory.__name__ + ".db"
    ) for record_construct in SCHEMA_CONSTRUCTS
}


SCHEMA = {record_construct.factory.__name__: record_construct.factory
          for record_construct in SCHEMA_CONSTRUCTS}

PAGES = {
    record_construct.factory: make_page_for_record_construct(record_construct)
    for record_construct in SCHEMA_CONSTRUCTS}


def get_page_struct(record):
    return PAGES[type(record)]


def read_records_from_page(buf, record_type):
    """
    reads a page from the stream. returns records as a list
    We need record type passed in cause we can't tell binary data apart
    """
    Page = PAGES[record_type]
    try:
        return [wrap.record
                for wrap in Page.parse_stream(buf).records]
    except StreamError:
        return []


def read_page(buf, record_type):
    """
    reads a page from the stream. returns records as a list
    We need record type passed in cause we can't tell binary data apart
    """
    Page = PAGES[record_type]
    try:
        return Page.parse_stream(buf)
    except StreamError:
        return None


def write_page(buf, records):
    """
    Builds a page of records right into the write - buf you gave us
    If you pass records from different tables, I kill u
    """
    # These will be used later
    Page = get_page_struct(records[0])
    ratings_w_headers = []
    for raw in records:
        ratings_w_headers.append({
            "header": {"transaction": 3, "bitfield": 0},
            "record": raw,
        })
    try:
        raw = Page.build({"records": ratings_w_headers})
    except FormatFieldError:
        # If it's too big for a page, we'll get a negative padding value.
        # Let's call that a Fat Page
        raise FatPageException()

    if len(raw) > MAX_PAGE_BYTES:
        # This is a safeguard although I expect the try/except will catch
        raise FatPageException(len(raw))
    buf.write(raw)


def write_row_to_existing_file(buf, row):
    """
    Inserts a new row in an existing table.
    This tries to fit it in the last page of a table.
    If it doesn't fit, it makes a new page.
    This may leave some pages partially full
    (say, insert bunch of small records
    then a GIANT one, then more small ones... the first page
    could probably have fit more of the small ones that came later)

    However, checking all the pages from scratch has a pretty high IO cost, and
    is unlikely to find much space if we've discarded those pages as 'full'
    one time. So therefore, optimize by only checking hte last page.
    We can compact with a vacuum if we ever write one

    # TODO does this/ should this preserve order of records?
    """
    buf.seek(-4096, 2)
    new_size = _estimate_row_size(row)
    page = read_page(buf, type(row))

    if page and new_size < page.padding_size:
        # We can fit it in this one!!
        buf.seek(-MAX_PAGE_BYTES, 1)
        records = [wrap.record for wrap in page.records] + [row]
    else:
        # make a new page
        records = [row]

    write_page(buf, records)


def _estimate_row_size(row):
    construct = SCHEMA_CONSTRUCT_MAP[type(row)]
    return len(construct.build(row)) + ROW_HEADER_BYTES


def write_fresh_table(buf, rows):
    """
    Writes a new table from a lot of rows.
    This is mostly designed for like, imports. Maybe for spilling?

    Takes many records, serialize them and get their length
    Start the file.
    Add records from the list unitl the next one exceeds the bin
    write that page to file
    keep going until record list is empty
    """

    i_rows = iter(rows)

    cur_page_size = 0
    selected_rows = []

    while True:
        try:
            new_row = next(i_rows)
            new_size = _estimate_row_size(new_row)
        except StopIteration:
            if selected_rows:
                write_page(buf, selected_rows)
            return
        if _can_fit_another_row(cur_page_size, new_size):
            cur_page_size += new_size
            selected_rows.append(new_row)
        else:
            write_page(buf, selected_rows)
            cur_page_size = new_size
            selected_rows = [new_row]


def _can_fit_another_row(cur_size, new_size):
    return (cur_size + new_size + PAGE_HEADER_BYTES) < MAX_PAGE_BYTES


def read_all_pages(buf, record_type, close=False):
    """
    Read all rows out of all pages (into memory)
    This may not fit for large tables!
    """
    buf.seek(0)

    got_records = read_records_from_page(buf, record_type)
    while got_records:
        yield from got_records
        got_records = read_records_from_page(buf, record_type)
        if not got_records:
            if close:
                # TODO what if we have an exception
                buf.close()
            return


class FileScan(Scan):
    table = attr.ib(init=False)

    def __attrs_post_init__(self):
        # Data here is a string of a table!
        # Let's look that sucker up!
        self.table = TABLES[self.data]
        buf = open(self.table.homefile, "rb")
        self._iter = read_all_pages(buf, self.table.factory, close=True)
