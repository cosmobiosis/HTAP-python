from typing import List


class Index:

    def __init__(self, table, num_columns, primary_key_index: int):
        """
        Args:
            num_columns: number of columns for users
            primary_key_index: which column is primary key
        Attributes:
            map_list: List of Dict
            Dict: key -> List[Rids]  int to list mapping
            index created: boolean values for index that are created
        """
        self.table = table
        self.num_features = num_columns
        self.index_created = []
        for i in range(self.num_features):
            self.index_created.append(False)
        # Index is created for primary keys by default
        self.index_created[primary_key_index] = True

        self.map_list = []
        self.primary_key_index = primary_key_index
        for i in range(self.num_features):
            self.map_list.append({})
        return

    def map_insert(self, new_record):
        """
            Args:
                new_record: new records we insert
        """
        if len(new_record.columns) != self.num_features:
            raise ValueError('Index Error: "{}"'.format("Map Insert incompatible num columns"))

        base_rid = new_record.rid
        for feature_index in range(len(new_record.columns)):
            if self.index_created[feature_index] == False:
                continue
            new_key = new_record.columns[feature_index]
            if feature_index == self.primary_key_index:
                # if we're updating the primary key in our map
                if new_key in self.map_list[feature_index]:
                    # And if new primary key is already in the primary key map
                    raise ValueError("Cannot update to primary key", new_key, "that already exists!")
                else:  # if new primary key isn't in primary key map at all
                    self.map_list[feature_index][new_key] = [base_rid]

            else:  # if we're not updating the primary key at all
                # first remove old key from rid list
                if new_key in self.map_list[feature_index]:
                    # if in map, append new key to the rids list
                    self.map_list[feature_index][new_key].append(base_rid)
                else:
                    # initialize a list of rid(s) to current map and store the base rid
                    self.map_list[feature_index][new_key] = [base_rid]
        return

    def map_change(self, base_rid, old_feature: List, new_feature: List):
        """ Args:
            old_feature: old_feature we query select before query update
            new_feature: new_feature we want to do query update to the table
        """
        if len(old_feature) != self.num_features or len(new_feature) != self.num_features:
            print('Index Error: "{}"'.format("Map Insert incompatible num columns"))

        for feature_index in range(self.num_features):
            if self.index_created[feature_index] == False:
                continue
            old_key = old_feature[feature_index]
            new_key = new_feature[feature_index]
            if old_key not in self.map_list[feature_index]:
                raise ValueError('Index Error: old key not exist', old_key, 'new feature:', new_feature)
            if new_key is None or new_key == old_key:
                continue

            if feature_index == self.primary_key_index:
                # if we're updating the primary key in our map
                if new_key in self.map_list[feature_index]:
                    # And if new primary key is already in the primary key map
                    raise ValueError("Cannot update to primary key", new_key, "that already exists!")
                else:  # if new primary key isn't in primary key map at all
                    del self.map_list[feature_index][old_key]
                    self.map_list[feature_index][new_key] = [base_rid]

            else:  # if we're not updating the primary key at all
                # first remove old key from rid list
                self.map_list[feature_index][old_key].remove(base_rid)
                if new_key in self.map_list[feature_index]:
                    # if in map, append new key to the rids list
                    self.map_list[feature_index][new_key].append(base_rid)
                else:
                    # initialize a list of rid(s) to current map and store the base rid
                    self.map_list[feature_index][new_key] = [base_rid]
        return

    def map_delete(self, old_key):
        del self.map_list[self.primary_key_index][old_key]

    def locate(self, key: int, feature_index: int):
        if self.index_created[feature_index] == False:
            raise ValueError("Index hasn't been built for this column yet!")
        if key not in self.map_list[feature_index]:
            return None
        ans = self.map_list[feature_index][key]
        if len(ans) == 0:
            return None
        else:
            return ans

    def create_index(self, column_number):
        self.table.table_create_index(column_number)

    def drop_index(self, column_number):
        self.index_created[column_number] = False
        # Empty it out
        self.map_list[column_number] = {}
        return
