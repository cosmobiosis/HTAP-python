from collections import OrderedDict
from copy import copy
from struct import pack_into
from threading import Lock

from lstore.disk_helper import *
from lstore.page import Page


class Cache:
    def __init__(self, size: int, appendix: str):
        """
        :param size:
        Attributes:
            cache: OrderedDict[page_key: Tuple[bool dirty, Page]]
                page_key = base|tail-(page_index)-(column_index)
        """
        self.size = size
        self.cache = OrderedDict()
        self.last_rid = {}
        self.appendix = appendix  # In the form of 'ECS165/Grades'
        self.disk_helper = DiskHelper(appendix)

        last_base_rid, last_tail_rid = self.disk_helper.get_last_rids()
        self.last_rid['base'] = last_base_rid
        self.last_rid['tail'] = last_tail_rid

        # Use Page Latch to ensure atomic R/W operations
        # Two latches are only used in static methods __get_page and __set_page
        # One extra latch locks up the last rid
        self.set_latch = Lock()
        self.get_latch = Lock()
        self.rid_latch = Lock()

    # Public APIs

    def get_new_rid(self, range_type: str) -> bytes:
        self.rid_latch.acquire()

        page_index, byte_offset = unpack('II', self.last_rid[range_type])
        entry_offset = byte_offset // WORDSIZE

        entry_offset += 1
        if entry_offset == PAGESIZE // WORDSIZE:
            page_index += 1
            # Skips num_record in tail pages, num_record and TPS in base pages
            if range_type == 'base':
                entry_offset = 2
            elif range_type == 'tail':
                entry_offset = 1
            else:
                raise ValueError("Range Type Error")
        new_rid = pack('II', page_index, entry_offset * WORDSIZE)
        self.last_rid[range_type] = new_rid

        self.rid_latch.release()
        return new_rid

    def __del__(self):
        for key, (is_dirty, page) in self.cache.items():
            range_type, page_index, column = self.__parse_key(key)
            # No need to flush the clean pages
            if not is_dirty:
                continue
            self.disk_helper.write_page(range_type, page_index, column, page)

    def get_page(self, range_type: str, page_index: int, column: int) -> Page:
        """Interface to get a Page"""
        return self.__get_page(self.__generate_key(range_type, page_index,
                                                   column))

    def get_entry(self, range_type: str, rid: bytes, query_column: int) \
            -> bytes:
        """
        Returns the single data entry in the table.
        Args:
            range_type: 'base' | 'tail', the page range to read
            rid: the target RID of the record to read
            query_column: the index of the column to select
        Returns:
            each byte of the data in the entry
        """
        page_index, page_offset = unpack('II', rid)
        target_page = self.get_page(range_type, page_index, query_column)
        return target_page.read_field(page_offset)

    def set_page(self, range_type: str, page_index: int, column: int,
                 new_page: Page):
        """ Interface to set a Page """
        self.__set_page(self.__generate_key(range_type, page_index, column),
                        new_page)

    def set_entry(self, range_type: str, rid: bytes, query_column: int,
                  data: bytes = None, is_append: bool = False) -> None:
        """
        Return: Set entry return page to keep reference in table layer
        Sets one data entry with specified data, increments the counter of
        the page if is_append is set to True
        Args:
            range_type: 'base' | 'tail', the page range to write
            rid: the target RID of the record to write
            query_column: the index of the column to select
            data: the new data to be written, if None, then only incrementing
            counter of the page without calling Page.write_field
        """
        page_index, page_offset = unpack('II', rid)
        target_page = copy(self.get_page(range_type, page_index, query_column))
        key = self.__generate_key(range_type, page_index, query_column)

        if is_append:
            target_page.num_records += 1
            pack_into('Q', target_page.data, 0, target_page.num_records)

        if data is None:
            return

        target_page.write_field(page_offset, data)
        self.__set_page(key, target_page)
        return

    def last_page_index(self, range_type: str) -> int:
        if range_type == 'base':
            last_base_rid = self.last_rid['base']
        elif range_type == 'tail':
            last_base_rid = self.last_rid['tail']
        else:
            raise ValueError('unrecognized range_type, '
                             'expecting \'base\' or \'tail\', but got {}'
                             .format(range_type))

        last_page_index, _ = unpack('II', last_base_rid)
        return last_page_index

    # Private APIs

    def __set_page(self, key: str, val: Page):
        self.set_latch.acquire()

        if key in self.cache:
            del self.cache[key]

        self.cache[key] = (True, val)
        if len(self.cache) > self.size:
            key, (is_dirty, page) = self.cache.popitem(last=False)
            # print("Evicting: Dirty?", is_dirty)
            if not is_dirty:
                self.set_latch.release()
                return  # not writing clean pages to disks

            range_type, page_index, column = self.__parse_key(key)
            self.disk_helper.write_page(range_type, page_index, column, page)

        self.set_latch.release()

    def __get_page(self, key: str) -> Page:
        self.get_latch.acquire()

        range_type, page_index, column = self.__parse_key(key)
        if key not in self.cache:
            target_page = self.disk_helper.read_page(
                range_type, page_index, column)
            self.__set_page(key, target_page)
            self.cache[key] = (False, target_page)
            ret_page = target_page
        else:
            self.cache.move_to_end(key)
            ret_page = self.cache[key][1]

        self.get_latch.release()

        return ret_page

    @staticmethod
    def __parse_key(key: str) -> Tuple[str, int, int]:
        parsed_key = key.split('-')
        return parsed_key[0], int(parsed_key[1]), int(parsed_key[2])

    @staticmethod
    def __generate_key(range_type: str, page_index: int, column: int) -> str:
        return '-'.join((range_type, str(page_index), str(column)))
