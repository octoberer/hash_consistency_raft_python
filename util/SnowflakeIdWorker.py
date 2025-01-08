import time

import time


class SnowflakeIdWorker:
    tw_epoch = 1288834974657  # Twitter's snowflake epoch
    max_worker_id = 31  # 5 bits
    max_datacenter_id = 31  # 5 bits
    sequence_mask = 4095  # 12 bits
    worker_id_shift = 12  # 12 bits for worker_id
    datacenter_id_shift = 17  # 5 bits for datacenter_id
    timestamp_shift = 22  # 41 bits for timestamp

    def __init__(self, datacenter_id=1, worker_id=1):
        if datacenter_id > self.max_datacenter_id or datacenter_id < 0:
            raise ValueError(f"datacenter_id must be between 0 and {self.max_datacenter_id}")
        if worker_id > self.max_worker_id or worker_id < 0:
            raise ValueError(f"worker_id must be between 0 and {self.max_worker_id}")

        self.datacenter_id = datacenter_id
        self.worker_id = worker_id
        self.sequence = 0
        self.last_timestamp = -1

    def time_gen(self):
        """Returns the current time in milliseconds."""
        return int(time.time() * 1000)

    def til_nextMillis(self, last_timestamp):
        """Wait until the next millisecond."""
        timestamp = self.time_gen()
        while timestamp <= last_timestamp:
            timestamp = self.time_gen()
        return timestamp

    def get_id(self):
        """Generate a unique ID based on the current time, worker ID, datacenter ID, and sequence."""
        timestamp = self.time_gen()

        if self.last_timestamp == timestamp:
            self.sequence = (self.sequence + 1) & self.sequence_mask  # Reset sequence if it exceeds the limit
            if self.sequence == 0:
                # Sequence overflow, need to wait for the next millisecond
                timestamp = self.til_nextMillis(self.last_timestamp)
        else:
            self.sequence = 0

        self.last_timestamp = timestamp

        # Shift bits to create the unique ID
        return ((timestamp - self.tw_epoch) << self.timestamp_shift) | \
            (self.datacenter_id << self.datacenter_id_shift) | \
            (self.worker_id << self.worker_id_shift) | \
            self.sequence
