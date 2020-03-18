from typing import List
from collections import deque
from lstore.table import Table, Record
from lstore.lock_manager import LockManager

class Query:
    def __init__(self, table: Table) -> None:
        """
        Initializes a Query object that can perform different queries on the
        specified table.

        Args:
            table: the Table for the Query to operate on
        """
        self.table = table

    @staticmethod
    def __print_error(msg: str) -> None:
        # Prints an error message to standard out.
        print('Query Error: "{}"'.format(msg))

    def delete(self, key: int) -> None:
        """
        Deletes a record with specified key.

        Args:
            key: the key to delete from self.table
        """
        rid = self.table.index.locate(key, 0)[0]
        self.table.delete(rid)
        self.table.index.map_delete(key)

    def insert(self, *columns: int) -> None:
        """
        Inserts a record with specified columns into the base page range

        Args:
            *columns: values of each column of the new record
        """
        if len(columns) != self.table.num_columns:
            self.__print_error('inserted record has incompatible dimensions'
                               ' with the table, insertion failed')
            return

        # instantiates the new Record
        base_rid = self.table.get_new_rid('base')
        key = columns[self.table.key_index]

        new_record = Record(base_rid, key, columns, 'base')
        self.table.index.map_insert(new_record)

        # inserts the new Record into self.table
        self.table.insert_record(new_record)

    def select(self, key: int, key_index: int, query_columns: List[int]) \
            -> List[Record]:
        """
        Selects the specified column(s) of the record with the specified key.

        Args:
            key: the key of the record to select
            key_index: which column does this key belong to
            query_columns: bit-vector to indicate which column(s) to select

        Returns:
            on success: list with records of specified columns
            on failure: empty list
        """
        if len(query_columns) != self.table.num_columns:
            self.__print_error(
                'query_columns has incompatible size with the table, '
                'expecting {} but got {}'
                    .format(self.table.num_columns, len(query_columns))
            )
            return []

        base_rids_list = self.table.index.locate(key, key_index)
        record_list = []  # list of records to return
        for base_rid in base_rids_list:
            query_result = []
            for i in range(self.table.num_columns):
                if query_columns[i] == 0:  # current column is not selected
                    query_result.append(None)
                    continue

                field = self.table.select_feature(base_rid, i)
                query_result.append(field)
            record = Record(base_rid, key, query_result, 'base')
            record_list.append(record)

        return record_list

    def update(self, key: int, *columns: int or None) -> bool:
        """
        Updates the record that has specified key to the specified columns.

        Args:
            key: the key of the record to be updated
            *columns: new values of the record,
                      None means not updating the column
        """
        if len(columns) != self.table.num_columns:
            self.__print_error(
                'columns has incompatible size with the table, '
                'expecting {} but got {}, update failed'
                    .format(self.table.num_columns, len(columns))
            )
            return False

        new_tail_rid = self.table.get_new_rid('tail')
        new_record = Record(new_tail_rid, key, columns, 'tail')
        # append the new Record into tail pages
        self.table.insert_record(new_record)

        # get the base rid before we do the map change
        base_rid = self.table.index.locate(key, 0)[0]
        query_column = [1 for _ in range(self.table.num_columns)]
        # Using the old primary key to get the only old_record with the old data
        old_features = self.select(key, 0, query_column)[0].columns
        new_features = columns
        # change the index of old data to new data
        self.table.index.map_change(base_rid, old_features, new_features)

        self.table.update_record(base_rid, new_record)
        return True

    def sum(self, start_range: int, end_range: int, aggregate_column_index: int) \
            -> int:
        """
        Sums up the values in the specified column for records with
        a the set of start and end keys (inclusive).

        Args:
            start_range: start of the key range to aggregate
            end_range: end of the key range to aggregate
            aggregate_column_index: index of desired column to aggregate
        """
        query_column = [0] * self.table.num_columns
        query_column[aggregate_column_index] = 1

        if start_range == end_range:
            return self.select(start_range, 0, query_column)[0] \
                .columns[aggregate_column_index]

        step = (end_range - start_range) // abs(end_range - start_range)
        sum_val = 0
        for i in range(start_range, end_range + step, step):
            if self.table.index.locate(i, 0) is None:
                continue  # skips current key if it does not exist

            sum_val += self.select(i, 0, query_column)[0] \
                .columns[aggregate_column_index]
        return sum_val

    def increment(self, key, column):
        r = self.select(key, self.table.key_index, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r.columns[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
