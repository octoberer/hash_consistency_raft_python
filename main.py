
import os
import time



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

