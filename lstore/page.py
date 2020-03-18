from struct import pack, unpack_from

from lstore.config import *


class Page:

    def __init__(self, range_type: str = '', data: bytes = bytes(0)):
        # initialize the page with given data bytes
        if len(data) == PAGESIZE:
            self.data = bytearray(data)
            self.num_records = unpack_from('Q', data)[0]
            return

        # initialize a new page with range_type
        self.data = bytearray(PAGESIZE)
        # initializes num_record based on range_type
        if range_type == 'base':
            self.num_records = 2  # num_record and TPS
        elif range_type == 'tail':
            self.num_records = 1  # num_record
        else:
            raise ValueError('Unrecognized range_type: {}'.format(range_type))
        self.write_field(0, pack('Q', self.num_records))

    def write_field(self, start_offset: int, new_word: bytes) -> None:
        # Writes one word to the given start_offset
        if len(new_word) != WORDSIZE:
            raise ValueError('writing with incompatible word size: {}'
                             .format(len(new_word)))

        self.data[start_offset: start_offset + WORDSIZE] = new_word

    def read_field(self, start_offset: int) -> bytearray:
        # Reads one word from the page at given start_offset
        if start_offset < 0 or start_offset + WORDSIZE > PAGESIZE:
            raise ValueError('start offset {} is out of range'
                             .format(start_offset))

        return self.data[start_offset: start_offset + WORDSIZE]
