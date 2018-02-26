import unittest
import random
import io
from itertools import count, islice

from redstreak.serialize import readtups, writetups, SCHEMA
from redstreak.serialize import write_page, read_records_from_page, MAX_PAGE_BYTES
from redstreak.serialize import FatPageException, write_row_to_existing_file
from redstreak.serialize import read_all_pages, write_fresh_table, write_row_to_existing_file_try_fail


Rating = SCHEMA["Rating"]


class TestIO(unittest.TestCase):

    def assert_records_match(self, recordsa, recordsb):
        self.assertEqual(len(recordsa), len(recordsb))
        movie_ids = {rec.movieId for rec in recordsa}
        for rec in recordsb:
            # raise keyerror if empty, nice!
            movie_ids.remove(rec.movieId)
        self.assertFalse(movie_ids)

    @unittest.skip("converting to binary")
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

    def make_page(self):
        buf = io.BytesIO()
        records = self.make_ratings()
        write_page(buf, records)
        return records, buf

    def test_write_page(self):
        records, buf = self.make_page()
        written = buf.getvalue()
        self.assertEqual(len(written), MAX_PAGE_BYTES)

    def test_write_then_read_records_from_page(self):
        records, buf = self.make_page()
        buf.seek(0)
        got_records = read_records_from_page(buf, Rating)
        self.assert_records_match(records, got_records)

    def test_write_fat_page(self):
        buf = io.BytesIO()
        records = self.make_ratings()
        records = records * 100
        self.assertRaises(FatPageException, lambda: write_page(buf, records))

    def test_read_records_from_page_from_record_type(self):
        records, buf = self.make_page()
        buf.seek(0)
        got_records = read_records_from_page(buf, type(records[0]))
        self.assertEqual(len(records), len(got_records))

    def test_row_to_existing_table_space(self):
        records, buf = self.make_page()
        new_record = self.make_single_rating()
        write_row_to_existing_file(buf, new_record)
        buf.seek(0)
        got_records = read_records_from_page(buf, Rating)
        self.assert_records_match(records + [new_record], got_records)

    @unittest.skip("This is just hella long")
    def test_row_to_existing_table_space_many(self):
        # please don't make your tables this way.
        # But this is a nice robust test that you can keep adding rows to a
        # table and the format remains consistent
        # This test takes a LOT of CPU, mostly cause this is a dumb
        # way to write your table...
        buf = io.BytesIO()
        # This should go to 4 pages
        many_records = self.make_ratings(count=700)
        for record in many_records:
            write_row_to_existing_file(buf, record)

        got_records = read_all_pages(buf, Rating)
        self.assert_records_match(got_records, many_records)
        self.assertTrue(len(buf.getvalue()) > MAX_PAGE_BYTES)
        self.assertTrue(len(buf.getvalue()) % MAX_PAGE_BYTES == 0)
        self.assertTrue(len(buf.getvalue()) == 4 * MAX_PAGE_BYTES)

    def test_write_fresh_table(self):
        """
        Test write_fresh_table, the prpoer way to write a table from scratch!
        """

        buf = io.BytesIO()
        many_records = self.make_ratings(count=1000)
        write_fresh_table(buf, many_records)

        got_records = read_all_pages(buf, Rating)

        self.assert_records_match(got_records, many_records)
        self.assertTrue(len(buf.getvalue()) > MAX_PAGE_BYTES)
        self.assertTrue(len(buf.getvalue()) % MAX_PAGE_BYTES == 0)

    def _make_ratings(self):
        field_counter = (count(0), count(3), count(5.9), count(1112484727))
        fields = ["userId", "movieId", "rating", "timestamp"]
        while True:
            yield Rating(**dict(zip(fields, (map(next, field_counter)))))

    def make_ratings(self, count=5):
        return list(islice(self._make_ratings(), 0, count))

    def make_single_rating(self):
        fields = ["userId", "movieId", "rating", "timestamp"]
        rating_fields = (99, 99, 99.9, 99999)
        return Rating(**dict(zip(fields, rating_fields)))


if __name__ == '__main__':
    unittest.main()
    # real  0m14.640s
# user    0m14.564s
# sys 0m0.044s
