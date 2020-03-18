from inspect import getmembers
from typing import Iterable, List, Tuple

from lstore.table import *
from lstore.index import *
from lstore.query import *

class Transaction:
    """
    # Creates a transaction object.
    """
    def __init__(self):
        # Class Reference Init
        self.table = None
        self.lock_manager = None
        self.aborted = False
        """
        keys_to_be_locked contains information for all records with primary keys ready to be locked
        mapping: primary keys -> locktype
        lock type: "r" / "w"
        """
        self.keys_to_be_locked = {}
        # Save for all Queries details in this transaction
        self.queries = []
        # Save current locks to be released later
        self.locks = []
    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    """
    def add_query(self, query, *args):
        # to run the query:
        # query.method(*args)
        if self.table is None:
            query_class = getmembers(query, lambda member: isinstance(member, Query))[0][1]
            self.table = query_class.table
            self.lock_manager = self.table.lock_manager

        self.queries.append((query, args))
        return

    def run(self) -> int:
        self.preprocessing()  # Prepare keys_to_be_locked, do all inserts
        self.acquire_locks()
        if self.aborted:
            return 0
        for query, args in self.queries:
            query(*args)
        self.release_locks()
        return 1

    def abort(self):
        # print("ABORT")
        self.release_locks()
        self.aborted = True
        return

    def commit(self):
        return

    def acquire_locks(self):
        for pkey, lock_type in self.keys_to_be_locked.items():
            lock = self.lock_manager.acquire(pkey, lock_type)
            if lock is None:
                self.abort()
                return
            self.locks.append(lock)

    def release_locks(self):
        for lock in self.locks:
            self.lock_manager.release(lock)
        self.keys_to_be_locked = {} # empty it

    def preprocessing(self) -> None:
        """
        Prepare records locks information by the following:
        take note of pkeys and lock type details in all queries
        Preprocess locking procedure using self.keys_to_be_locked mapping: pkeys->lock type
        lock type: "r" / "w"
        """
        index_class = self.table.index
        pkey_index = self.table.key_index
        
        for query, args in self.queries:
            query_class = getmembers(query, lambda member: isinstance(member, Query))[0][1]
            
            lock_type = None
            query_pkeys = []

            if query == query_class.insert or query == query_class.increment:
                lock_type = 'w'
                query_pkeys.append(args[pkey_index])

            if query == query_class.select:
                lock_type = 'r'
                # select(key, key_index, query_columns)
                query_pkeys.append(args[0])
                if args[1] != pkey_index:
                    # if we're doing non-primary selections
                    query_pkeys = []
                    base_rid_list = index_class.locate(args[0], args[1])
                    pkey_rid_map = index_class.map_list[pkey_index]
                    rid_pkey_map = {v:k for k, v in pkey_rid_map.items()}
                    for base_rid in base_rid_list:
                        if base_rid in rid_pkey_map:
                            query_pkeys.append(rid_pkey_map[base_rid])

            elif query == query_class.update or query == query_class.delete:
                lock_type = 'w'
                query_pkeys.append(args[0])

            elif query == query_class.sum:
                lock_type = 'r'
                query_baserids = []
                # sum(start_range, end_range, aggregate_column_index)
                start_range = args[0]
                end_range = args[1]
                step = (end_range - start_range) // abs(end_range - start_range)

                for i in range(start_range, end_range + step, step):
                    base_rid_list = index_class.locate(i, pkey_index)
                    if base_rid_list is None:
                        continue  # skips current records doesn't exist
                    query_pkeys.append(i) # append selected primary keys to list

            if len(query_pkeys) == 0:
                continue # Proceed to next query

            for key in query_pkeys:
                # Update lock type for this query's baserids mappings
                self.keys_to_be_locked[key] = lock_type
        return