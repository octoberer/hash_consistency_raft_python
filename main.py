
import os
import time

class SnowflakeIdWorker:
    tw_epoch = 1288834974657
    def __init__(self, datacenter_id, worker_id):
        self.datacenter_id = datacenter_id
        self.worker_id = worker_id
        self.sequence = 0
        self.last_timestamp = -1

    def time_gen(self):
        return int(time.time() * 1000)

    def til_nextMillis(self, last_timestamp):
        timestamp = self.time_gen()
        while timestamp <= last_timestamp:
            timestamp = self.time_gen()
        return timestamp

    def get_id(self):
        timestamp = self.time_gen()
        if self.last_timestamp == timestamp:
            self.sequence = self.sequence + 1
            if self.sequence > 4095:
                timestamp = self.til_nextMillis(self.last_timestamp)
        else:
            self.sequence = 0
        self.last_timestamp = timestamp
        return ((timestamp - self.tw_epoch) << 22) | (self.datacenter_id << 12) | (self.worker_id << 6) | self.sequence

def saveFileId(file_path,datacenter_id,worker_id):
        with open(file_path, 'rb') as file:
            file_content = file.read()
        file_name= os.path.basename(file_path)
        filer = SnowflakeIdWorker(datacenter_id, worker_id)
        file_uuid = filer.get_id()
        with open("file_ids.txt", "a") as file_ids:
            file_ids.write(f"{file_name}:   {file_uuid}\n")
        with open(f'{file_uuid}.py', "wb") as new_unique_file:
            new_unique_file.write(file_content)

if __name__ == '__main__':
    saveFileId('./main.py',0,0)

