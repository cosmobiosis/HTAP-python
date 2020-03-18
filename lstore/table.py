import threading
from collections import deque
from copy import copy
from struct import *
from time import time
from typing import Iterable, List

from lstore.cache import Cache
from lstore.config import *
from lstore.index import *
from lstore.lock_manager import *

# internal columns maintained by Table class
RID_COLUMN = 0
# base record: RID of the latest update (tail record)
# tail record: RID of its previous update
INDIRECTION_COLUMN = 1
# bit-vector to indicate which column(s) have been updated
SCHEMA_ENCODING_COLUMN = 2
# number of seconds since the epoch * 1,000,000
TIMESTAMP_COLUMN = 3
# number of meta data columns
NUM_INTERNAL_COLUMN = 4

INVALID_RID = pack('Q', 0xFFFFFFFFFFFFFFFF)


class Record:
    """
    Record is the standardized object used in the interfaces of Table class
    """

    def __init__(self, rid: bytes, key: int,
                 columns: Iterable[int], range_type: str):
        """
        Args:
            rid: rid of the next record
                first 4 bytes (unsigned int): page index, i,
                in self.(base/tail)_range[][i]
                last 4 bytes (unsigned int): entry starting byte offset
                in current page
            key: the value of the key for the record
            columns: the values for all columns for the record
            range_type: 'base' | 'tail', indicating whether it's
                a base or tail record
        """
        self.rid = rid
        self.key = key
        self.columns = columns
        self.range_type = range_type

    def __str__(self):
        return self.columns.__str__()


class Table:
    def __init__(self, name: str, num_columns: int, key_index: int):
        """
        Args:
            name: table name
            num_columns: number of features
            key_index: index of table key in columns
        Attributes:
            appendix (str): table name
            key_index (int): index of primary_key in columns
            num_columns (int): number of features; all attributes are integer
            base_range (List[List[Page]]): base page range contains columns
            of list of Pages
            tail_range (List[List[Page]]): tail page range contains columns
            of list of Pages
        """
        self.appendix = name
        self.key_index = key_index
        self.num_columns = num_columns
        self.index = Index(self, num_columns, key_index)
        self.cache = Cache(CACHE_SIZE, name)
        self.lock_manager = LockManager()

        # self.BASERID_COLUMN = self.num_columns + NUM_INTERNAL_COLUMN
        """
        Each base page has a merge queue
        merge queue is a deque 
        with element (BaseRid, TailRid) for every update
        """
        self.merge_queue_matrix = []
        num_base_page = self.cache.last_page_index('base') + 1
        for feature_index in range(num_columns):
            self.merge_queue_matrix.append([])
            for _ in range(num_base_page):
                self.merge_queue_matrix[feature_index].append(deque())
        self.merge_counter = deque()

        self.closed = False
        self.merge_trigger = threading.Event()
        self.merge_thread = threading.Thread(name='merge', target=self.merge)
        self.merge_thread.start()

    def get_new_rid(self, range_type: str) -> bytes:
        """
        Returns the next rid to be allocated in the specified range_type
        """
        # Pass the job to cache level
        new_rid = self.cache.get_new_rid(range_type)
        if range_type == 'base':
            # print(unpack('II', new_rid))
            # Expand Merge Queue Matrix if base range expanded
            _, byte_offset = unpack('II', new_rid)
            if byte_offset == 2 * WORDSIZE:  # BasePage starts with 16 = 2 word size
                for i in range(self.num_columns):
                    # Append a merge queue for all new base pages
                    self.merge_queue_matrix[i].append(deque())
        return new_rid

    def insert_record(self, new_record: Record):
        """Inserts new_record into the base page range"""
        range_type = new_record.range_type
        rid = new_record.rid

        # appends RID of the new record to RID column
        self.cache.set_entry(range_type, rid, query_column=RID_COLUMN,
                             data=rid, is_append=True)

        # initializes indirection column as invalid (no updates yet)
        self.cache.set_entry(range_type, rid, query_column=INDIRECTION_COLUMN,
                             data=INVALID_RID, is_append=True)

        # initializes schema encoding column as all clean (all 0's)
        self.cache.set_entry(range_type, rid, query_column=SCHEMA_ENCODING_COLUMN,
                             is_append=True)

        # initializes timestamp column as current time
        self.cache.set_entry(range_type, rid, query_column=TIMESTAMP_COLUMN,
                             data=pack('Q', int(time() * 1000000)), is_append=True)

        # initializes user defined columns
        for index, data in enumerate(new_record.columns):
            if data is not None:  # converts data to bytes if it exists
                data = pack('q', data)

            self.cache.set_entry(range_type, rid,
                                 query_column=index + NUM_INTERNAL_COLUMN,
                                 data=data, is_append=True)

    def update_record(self, base_rid: bytes, new_tail_record: Record) \
            -> None:
        """
        Appends a new record into the tail page range
        Args:
            base_rid: the RID of the base record to be updated
            new_tail_record: the update record to be appended
        """
        new_tail_rid = new_tail_record.rid
        # Incremented merge_counter
        _, byte_offset = unpack('II', new_tail_rid)
        # Increment merge counter if a new tail page has been expanded
        if byte_offset == WORDSIZE:  # TailPage starts with 8 = 1 word size
            self.merge_counter.append(1)
            if len(self.merge_counter) > MERGE_EPOCH:
                # begin merge when needed
                self.merge_trigger.set()

        base_indirection = self.cache.get_entry('base', base_rid, INDIRECTION_COLUMN)

        # rewires indirection columns
        if base_indirection == INVALID_RID:  # base record has not been updated
            # indirect to the new tail RID
            self.cache.set_entry('base', base_rid, INDIRECTION_COLUMN,
                                 data=new_tail_rid)
        else:  # base record has already been updated
            # redirect latest tail record's indirection column
            # to second latest tail record
            old_tail_rid = base_indirection

            # indirection points to (old tail) second latest RID in tail pages
            self.cache.set_entry('tail', new_tail_rid, INDIRECTION_COLUMN,
                                 data=old_tail_rid)

            # finally update base page indirection to new tail RID
            self.cache.set_entry('base', base_rid, INDIRECTION_COLUMN,
                                 data=new_tail_rid)

        # actually appends the new tail record into Table
        for index, data in enumerate(new_tail_record.columns):
            if data is not None:
                data = pack('q', data)  # converts data to bytes if it exists
                # updates schema encoding for both base pages and tail pages
                self.update_schema('base', index, base_rid)
                self.update_schema('tail', index, new_tail_rid)

                # append (BaseRid, TailRid) to merge queue for this base page
                base_page_index, _ = unpack('II', base_rid)
                self.merge_queue_matrix[index][base_page_index] \
                    .append((base_rid, new_tail_rid))

            self.cache.set_entry('tail', new_tail_rid,
                                 query_column=index + NUM_INTERNAL_COLUMN,
                                 data=data)

    def update_schema(self, range_type: str, feature_index: int, rid: bytes) \
            -> None:
        """
        Marks the (feature_index)th column of the record as dirty
        Args:
            range_type: 'base' or 'tail'
            feature_index: the client-side index of the column to update
            rid: specifies the record to be updated
        """
        schema = self.cache.get_entry(range_type, rid, SCHEMA_ENCODING_COLUMN)
        schema = unpack('Q', schema)[0]  # converts unsigned int
        schema |= 1 << feature_index  # flips the (feature_index)th bit to 1
        data = pack('Q', schema)  # converts int to bytes

        # writes back the new schema
        self.cache.set_entry(range_type, rid, SCHEMA_ENCODING_COLUMN, data)

    @staticmethod
    def is_updated(encoding: bytes, feature_index: int) -> int:
        """
        Returns 0 if the feature is not updated according to the encoding,
            non-zero otherwise
        """
        return unpack('Q', encoding)[0] & (1 << feature_index)

    def select_feature(self, base_rid: bytes, feature_index: int) -> int:
        # base_page_index, base_page_offset = unpack('II', base_rid)
        # encoding_page = self.base_range[SCHEMA_ENCODING_COLUMN][base_page_index]
        # encoding = encoding_page.data[base_page_offset: base_page_offset + WORDSIZE]
        encoding = self.cache.get_entry('base', base_rid, SCHEMA_ENCODING_COLUMN)
        # print(unpack('II', base_rid), encoding)
        """
        if base page is never updated before
        OR if base already merged, newer than the latest tail record
        """

        if self.base_up_to_date(base_rid, feature_index) \
                or not self.is_updated(encoding, feature_index):
            """
            if self.base_up_to_date(base_rid, feature_index):
                print("Selecting from merged base page")
            """
            # selected feature is not updated, directly returns it
            data_entry = self.cache.get_entry(
                'base', base_rid,
                query_column=feature_index + NUM_INTERNAL_COLUMN
            )
            return unpack('q', data_entry)[0]

        tail_rid = self.cache.get_entry('base', base_rid, INDIRECTION_COLUMN)
        encoding = self.cache.get_entry('tail', tail_rid, SCHEMA_ENCODING_COLUMN)

        # keeps looking for the tail record's ancestor if it does not contain
        # the latest value of the queried column
        while not self.is_updated(encoding, feature_index):
            tail_rid = self.cache.get_entry('tail', tail_rid, INDIRECTION_COLUMN)
            encoding = self.cache.get_entry('tail', tail_rid,
                                            SCHEMA_ENCODING_COLUMN)

        # now tail_rid is the current record containing the latest value
        # of the queried column
        data_entry = self.cache.get_entry(
            'tail', tail_rid, query_column=feature_index + NUM_INTERNAL_COLUMN)
        return unpack('q', data_entry)[0]

    def delete(self, rid: bytearray) -> None:
        """
        Invalidates a base record and its tail records by setting their RID
            column to INVALID_RID
        """
        latest_update = self.cache.get_entry('base', rid, INDIRECTION_COLUMN)
        self.cache.set_entry('base', rid, RID_COLUMN, INVALID_RID)
        while latest_update != INVALID_RID:
            self.cache.set_entry('tail', latest_update, RID_COLUMN, INVALID_RID)
            latest_update = \
                self.cache.get_entry('tail', latest_update, INDIRECTION_COLUMN)

    def merge(self):
        while not self.closed:
            # Wait until range expansion triggers the merge
            self.merge_trigger.wait()
            merge_range = self.cache.last_page_index('base')

            if self.closed:
                """
                If we're closing, we want to update all base pages;
                Including those are not full yet since they won't be accessed anymore
                """
                merge_range += 1

            # create a copy of base range
            for feature_index in range(self.num_columns):
                for page_index in range(merge_range):
                    # print("Merging Column", feature_index, ",Page", page_index)
                    # limit the load to only outdated columns
                    merge_queue = self.merge_queue_matrix[feature_index][page_index]
                    if len(merge_queue) == 0:
                        continue
                    column_index = NUM_INTERNAL_COLUMN + feature_index
                    new_base_page = copy(self.cache.get_page('base', page_index, column_index))
                    """
                    hash map seen_update
                    Always bring in tail rid of newest update
                    becasue of dictionary key replacement
                    """
                    seen_update = {}
                    lineage = new_base_page.read_field(WORDSIZE)
                    while len(merge_queue) != 0:
                        tuple = merge_queue.popleft()
                        base_rid = tuple[0]
                        tail_rid = tuple[1]
                        seen_update[base_rid] = tail_rid
                        lineage = tail_rid  # later popped will be new lineage
                    new_base_page.write_field(WORDSIZE, lineage)
                    """
                    In the 1-to-1 mapping of seen_update,
                    for every tail rid of newest update in hashmap,
                    we directly merge corresponding base rid
                    """
                    for base_rid, tail_rid in seen_update.items():
                        data_entry = self.cache.get_entry(
                            'tail', tail_rid, query_column=feature_index + NUM_INTERNAL_COLUMN)
                        # next, we merge our latest data entry to the base page
                        _, page_offset = unpack('II', base_rid)
                        new_base_page.write_field(page_offset, data_entry)

                    self.cache.set_page('base', page_index, column_index, new_base_page)

            # Break the loop if table closed during wait
            if self.closed:
                break
            # Refresh the counter
            for _ in range(MERGE_EPOCH):
                self.merge_counter.popleft()
            # Reset Merge Trigger Event
            self.merge_trigger.clear()
        return

    def base_up_to_date(self, base_rid: bytes, feature_index: int) -> bool:
        """
         Args:
            base_rid: we want to check whether a base record is merged up-to-date for select
            feature_index: the index to be checked for up to date or not
        """
        # first we get this page's lineage
        page_index, _ = unpack('II', base_rid)
        column_index = feature_index + NUM_INTERNAL_COLUMN
        cur_page = self.cache.get_page('base', page_index, column_index)
        # get the lineage from offset WORDSIZE
        lineage = cur_page.read_field(WORDSIZE)
        # then we get this base record's newest update tail rid
        latest_tail_rid = self.cache.get_entry('base', base_rid, INDIRECTION_COLUMN)
        # If our lineage includes the latest tail rid, then the base page is up-to-date
        # compare the total records using basic calculations

        # First compare whose page index is BIGGER!
        if unpack('II', lineage)[0] > unpack('II', latest_tail_rid)[0]:
            return True
        # If the same compare whose offset is BIGGER!
        if unpack('II', lineage)[0] == unpack('II', latest_tail_rid)[0]:
            return unpack('II', lineage)[1] > unpack('II', latest_tail_rid)[1]
        return False

    def close(self):
        self.closed = True
        self.merge_trigger.set()  # Releasing merge thread before closing

    def table_create_index(self, column_number):
        if self.index.index_created[column_number] == True:
            print("Index Already Created")
            return
        # create index on non-primary column
        self.index.index_created[column_number] = True
        primary_index = self.index.map_list[self.key_index]
        for _, base_rid_list in primary_index.items():
            base_rid = base_rid_list[0]
            new_duplicate_key = self.select_feature(base_rid, column_number)

            if new_duplicate_key in self.index.map_list[column_number]:
                # if in map, append new key to the rids list
                self.index.map_list[column_number][new_duplicate_key].append(base_rid)
            else:
                # initialize a list of rid(s) to current map and store the base rid
                self.index.map_list[column_number][new_duplicate_key] = [base_rid]
        return