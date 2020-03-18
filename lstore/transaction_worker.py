from collections import deque

from lstore.table import Table, Record
from lstore.index import Index


class TransactionWorker:

    """
    # Creates a transaction worker object.
    """
    def __init__(self, transactions=None):
        self.result = 0
        self.stats_queue = deque()
        if transactions is None:
            self.transactions = []
            return
        self.transactions = transactions
        return

    """
    # Adds the given query to this transaction
    # Example:
    # q = Query(grades_table)
    # t = Transaction()
    # t.add_query(q.update, 0, *[None, 1, None, 2, None])
    # txn_worker = TransactionWorker([t])
    # th1 = threading.Thread(target=txn_worker.run)
    """

    def run(self):
        for txn in self.transactions:
            self.stats_queue.append(txn.run())
        self.result = len(list(filter(lambda x: x, self.stats_queue)))
        return

    def add_transaction(self, transaction):
        self.transactions.append(transaction)
