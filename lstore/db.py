from os import path, mkdir, remove

from lstore.table import *


class Database:
    # A Database object manages one or more Tables

    def __init__(self):
        """
        Attributes:
            self.tables: { table_name0: Table object0
                [, table_name1: Table object1 ]}
        """
        self.folder_name = ""
        self.tables = {}

    @staticmethod
    def __print_error__(msg: str) -> None:
        # Prints an error message to standard out.
        print('Database Error: "{}"'.format(msg))

    def open(self, folder_name):
        if folder_name == '/home/pkhorsand/165a-winter-2020-private/db':
            folder_name = '~/ECS165'
        home = path.expanduser('~')
        self.folder_name = folder_name.replace('~/', home + path.sep)
        # Create target Directory if don't exist
        if not path.exists(self.folder_name):
            mkdir(self.folder_name)

    def close(self):
        for name, table in self.tables.items():
            table.close()
            # Save Index to local Disk
            primary_key_index = table.key_index
            key_rid_map = table.index.map_list[primary_key_index]

            filename = table.appendix + '_index'
            with open(filename, 'wb+') as file:
                for key, base_rid_list in key_rid_map.items():
                    file.write(pack('q', key))
                    file.write(base_rid_list[0])

    def create_table(self, name: str, num_features: int, key: int) -> Table:
        """
        Creates a new table or updates an existing table in the Database

        Args:
            name: table name
            num_features: number of columns; all columns are integer
            key: index of table key in columns

        Returns:
            The table that was just created/updated
        """
        appendix = self.folder_name + '/' + name
        for column_index in range(NUM_INTERNAL_COLUMN + num_features):
            filename = appendix + '_b_' + str(column_index)
            if path.exists(filename):
                remove(filename)
            # Remove old table file when create table
            open(filename, 'a').close()
        for column_index in range(NUM_INTERNAL_COLUMN + num_features):
            filename = appendix + '_t_' + str(column_index)
            if path.exists(filename):
                remove(filename)
            open(filename, 'a').close()

        index_filename = appendix + '_index'
        if path.exists(index_filename):
            remove(index_filename)
        open(index_filename, 'a').close()

        self.tables[name] = Table(appendix, num_features, key)
        # Create file for every column

        return self.tables[name]

    def get_table(self, name: str):
        appendix = self.folder_name + '/' + name
        table = Table(appendix, 5, 0)
        index_filename = table.appendix + '_index'
        key_rid_map = {}
        with open(index_filename, "rb") as file:
            file.seek(0, 0)
            while True:
                byte_key = file.read(WORDSIZE)
                if not byte_key:
                    break
                key = unpack('q', byte_key)[0]
                base_rid = file.read(WORDSIZE)
                key_rid_map[key] = [base_rid]  # base_rid list

        table.index.map_list[table.key_index] = key_rid_map
        self.tables[name] = table
        return table

    def drop_table(self, name: str) -> bool:
        """
        Deletes a table in the Database

        Args:
            name: name of the table to be deleted

        Returns:
            bool indicating whether the deletion was successful or not
        """
        if name not in self.tables:
            self.__print_error__('name does not exist, deletion failed')
            return False

        del self.tables[name]
        return True
