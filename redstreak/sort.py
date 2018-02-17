
import attr
import inspect

from redstreak.nodes import Scan
from redstreak.io import readtups, writetups, Writer

MAX_MEMORY_SIZE_BYTES = 1000 * 1000 * 10  # Ten mbs
# Sampled from ratings as Dicts, probably less as NamedTuples
# TODO redo when move off dicts
EST_RECORD_SIZE = 900

MAX_RECORD_LEN = MAX_MEMORY_SIZE_BYTES / EST_RECORD_SIZE


class InTooDeepException(Exception):
    pass


@attr.s
class Sort(Scan):
    key = attr.ib()
    data = attr.ib()
    _iter = attr.ib(default=None, init=False)

    def __attrs_post_init__(self):
        # I think ideally this is some kind of streaming sorting algorithm
        # but that seems quite hard right now. this will exhaust the data nodes
        def sort_key(item):
            return item[self.key]
        self.sort_key = sort_key
        self._files_to_cleanup = []

    def __next__(self):

        if not self._iter:
            self._iter = iter(self.sort())
        try:
            return next(self._iter)
        except StopIteration:
            for file in self._files_to_cleanup:
                file.close()
            raise

    def additional_explain(self):
        return "\nfield:\n" + inspect.getsource(self.field)

    def sort(self):
        """
        Potentially out-of-core mergesort.

         pull until one of two things
             we hit max memory
             we hit end

         if max memory
             sort and write to disk
             record location in filelist
             delete buffer
             pull again

         if end:
             if no filelist:
                 sort (in åmemory) and return
             if filelist:
               sort and write to disk
                out of core sort!


         """

        # INITIAL PASS
        buf = []
        files_by_pass = {0: []}
        data_iterator = iter(self.data)
        # This is going to start with a len, but it should become an actual
        # memory size estimate
        # If you pass me an infinite iterator I will kill us both
        # How can I sort what I cannot comprehend!

        while True:
            try:
                buf.append(next(data_iterator))
            except StopIteration:
                if files_by_pass[0]:
                    # We gotta do an out-of-core sort, oh goody!
                    new_filename = self.sort_write_clear(buf)
                    files_by_pass[0].append(new_filename)
                    return self.binary_out_of_core_sort(files_by_pass)
                else:
                    buf.sort(key=self.sort_key)
                    return iter(buf)

            if len(buf) > MAX_RECORD_LEN:
                # We gotta write to disk!
                new_filename = self.sort_write_clear(buf)
                files_by_pass[0].append(new_filename)

    def sort_write_clear(self, buf):
        buf.sort(key=self.sort_key)
        file = writetups(buf)
        self._files_to_cleanup.append(file)
        buf.clear()
        return file

    def binary_out_of_core_sort(self, files_by_pass, prev_pass=0):
        """
        This is a simple out of core sort that just sorts 2 each time
        instead of n. Yes, it's bad.
        The reason I'm doing this is that it's a stepping stone, and also
        with csv dictreader under the hood, I can't estimate my input buffer
        sizes. So I'm playing it safe

        increase passcounter
        if only 1 file:
             open file and return an iterator over it
        while >1 file:
            if 2 files left:

                pop 2 files
                open 2 files, read in each a bit
                mergesort and write result to new file
                add new file to filelist

        if one file left:
            move file to next pass

        return recur
        """
        if prev_pass > 10:
            raise InTooDeepException("I'm in too deep!")

        cur_pass = prev_pass + 1
        runs_to_merge = files_by_pass[prev_pass]
        files_by_pass[cur_pass] = []

        if len(runs_to_merge) == 1:
            return readtups(runs_to_merge[0])
        while len(runs_to_merge) > 1:
            filea = runs_to_merge.pop()
            fileb = runs_to_merge.pop()
            merged_result = self.binary_merge_to_file(filea, fileb)
            files_by_pass[cur_pass].append(merged_result)

        # Done with this pass. We may have 1 leftover if odd starter.
        if runs_to_merge:
            files_by_pass[cur_pass].extend(runs_to_merge)
        return self.binary_out_of_core_sort(files_by_pass, cur_pass)

    def binary_merge_to_file(self, filea, fileb):
        itera, iterb = readtups(filea), readtups(fileb)
        a_head, b_head = next(itera), next(iterb)
        output_buf = []
        output_writer = Writer()
        FINISH_SENTINAL = object()

        while (a_head, b_head) != (FINISH_SENTINAL, FINISH_SENTINAL):
            if a_head == FINISH_SENTINAL:
                a_wins = False
                #  b wins
            elif b_head == FINISH_SENTINAL:
                a_wins = True
                # a wins
            elif a_head[self.key] < b_head[self.key]:
                a_wins = True
                # a wins
            else:
                a_wins = False
                # b wins

            if a_wins:
                output_buf.append(a_head)
                try:
                    a_head = next(itera)
                except StopIteration:
                    a_head = FINISH_SENTINAL
            else:
                output_buf.append(b_head)
                try:
                    b_head = next(iterb)
                except StopIteration:
                    b_head = FINISH_SENTINAL

            # TODO tune this when we have control over reading/writing
            if len(output_buf) > MAX_RECORD_LEN:
                output_writer.write(output_buf)

        output_writer.write(output_buf)
        self._files_to_cleanup.append(output_writer.file)
        return output_writer.file
