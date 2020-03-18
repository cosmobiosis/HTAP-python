from readerwriterlock import rwlock

class LockManager:

    def __init__(self):
        """
        HashMap: baserid -> RWLockFair
        every RWLockFair Entrance will be able to generate locks
        """
        self.locks_map = {}

    def acquire(self, pkey: int, lock_type: str):
        """
        :param primary key, lock_type "r"/"w"
        :return:
        Generated locks by corresponding rids
        None if failed to acquire
        """
        if pkey not in self.locks_map:
            self.locks_map[pkey] = rwlock.RWLockFair()
        # Create a new RWLock Entrance for this record
        if lock_type == 'r':
            rlock = self.locks_map[pkey].gen_rlock()
            success = rlock.acquire(blocking=False)
            if not success:
                return None
            return rlock
        elif lock_type == 'w':
            wlock = self.locks_map[pkey].gen_wlock()
            success = wlock.acquire(blocking=False)
            if not success:
                return None
            return wlock
        raise ValueError("Lock Type not recognized", lock_type)

    def release(self, lock) -> None:
        lock.release()
        return