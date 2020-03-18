from os import path, SEEK_END
from struct import unpack, pack
from typing import Tuple

from lstore.config import *
from lstore.page import Page


class DiskHelper:
    def __init__(self, appendix: str):
        self.appendix = appendix
        self.opened_files = {}  # maps a file_name to its file object

    def read_page(self, range_type: str, page_index: int, column: int) -> Page:
        file, size = self.__open_file(column, range_type)

        # checks if the file needs to be enlarged
        if page_index >= size // PAGESIZE:
            file.seek(0, SEEK_END)
            new_page = Page(range_type=range_type)
            # doubles the size of the file
            new_bytes = new_page.data * (page_index + 1)
            file.write(new_bytes)

        # reads specified page
        file.seek(page_index * PAGESIZE)
        return Page(data=file.read(PAGESIZE))

    def __open_file(self, column: int, range_type: str) -> Tuple:
        filename = '_'.join([self.appendix, range_type[0], str(column)])

        # checks if file has already been opened
        if filename not in self.opened_files \
                or self.opened_files[filename].closed:
            # not opened yet, open the file and stores file object
            self.opened_files[filename] = open(filename, 'rb+')

        # opened already
        file = self.opened_files[filename]
        size = path.getsize(filename)
        return file, size

    def write_page(self, range_type: str, page_index: int, column: int,
                   to_write: Page) -> None:
        file, size = self.__open_file(column, range_type)

        if page_index >= size // PAGESIZE:
            raise ValueError('Write Page Out of Bound')

        file.seek(page_index * PAGESIZE, 0)
        file.write(to_write.data)

    def get_last_rids(self) -> Tuple[bytes, bytes]:
        range_types = ['base', 'tail']
        rids = []
        for range_type in range_types:
            file, size = self.__open_file(0, range_type)

            if size % PAGESIZE != 0:
                raise ValueError('File size is not a multiple of PAGESIZE')

            if size == 0:
                last_page_index = 0
                byte_offset = 8 if range_type == 'base' else 0
            else:
                last_page_index = size // PAGESIZE - 1
                # num_record is stored as the first entry
                file.seek(last_page_index * PAGESIZE, 0)
                num_records = unpack('Q', file.read(WORDSIZE))[0]
                byte_offset = num_records * WORDSIZE

            rids.append(pack('II', last_page_index, byte_offset))

        return rids[0], rids[1]

    def __del__(self):
        for file in self.opened_files.values():
            file.close()