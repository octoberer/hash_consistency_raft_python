import json
import os
import threading
import time
directory=''


class lfu_cache:
    def __init__(self,maxsize):
        self.maxsize=maxsize
        self.time_dict = {}
        self.frequency_cache_dict= {}
        self.interval=5
        self.min_frequency=0
    def visitFile(self,file_name):
        key = file_name
        # 更新time_dict
        current_timestamp = time.time()
        if key not in self.time_dict:
            self.time_dict[key]={"frequency":0}
        new_frequency = self.time_dict[key]["frequency"] + 1
        self.time_dict[key]["visit_time"] = current_timestamp
        self.time_dict[key]["frequency"] = new_frequency
        # 如果在内存里
        if key in self.frequency_cache_dict:
            # print(f'{key}被访问了，走的缓存，现在访问频率为：{new_frequency}')
            self.frequency_cache_dict[key]["frequency"]=new_frequency
            return self.frequency_cache_dict[key]['file_content']
        return False
    def visit_disk_file(self,file_name):
        with open(f'{file_name}', 'r') as file:
            file_content = file.read()
        return file_content

    def updateFile(self,file_path,properties_dict):
        try:
            with open(file_path, 'r') as file:
                # 假设文件是JSON格式，加载为字典
                file_content = json.loads(file.read())
        except FileNotFoundError:
            print(f"Error: The file {file_path} does not exist.")
            return
        except json.JSONDecodeError:
            print(f"Error: The file {file_path} is not a valid JSON file.")
            return
        file_content.update(properties_dict)
        with open(file_path,'w') as file:
            file.write(json.dumps(file_content))
            file.flush()  # 刷新缓冲区到磁盘
            os.fsync(file.fileno())  # 强制同步数据到磁盘
        if file_path in self.frequency_cache_dict:
            del self.frequency_cache_dict[file_path]
        if file_path in self.time_dict:
            del self.time_dict[file_path]
    def deleteFile(self,file_path):
        # 先删除磁盘
        os.remove(file_path)
        if file_path in self.frequency_cache_dict:
            del self.frequency_cache_dict[file_path]
        if file_path in self.time_dict:
            del self.time_dict[file_path]
    def deleteCache(self,file_path=None):
        if file_path is None:
            self.frequency_cache_dict.clear()
            self.time_dict.clear()
            return
        if file_path in self.frequency_cache_dict:
            del self.frequency_cache_dict[file_path]
        if file_path in self.time_dict:
            del self.time_dict[file_path]
    def deleteKey(self):
        last_current_timestamp = time.time()
        for key in list(self.time_dict.keys()):
            if last_current_timestamp - self.time_dict[key]['visit_time'] > self.m:
                # print(f"时间到了,{key}被移除time_dict内存了")
                del self.time_dict[key]
                if key in self.frequency_cache_dict:
                    del self.frequency_cache_dict[key]
                    # print(f"时间到了,{key}被移除frequency_cache_dict内存了")
        my_thread = threading.Timer(self.interval, self.deleteKey)
        my_thread.daemon = True
        my_thread.start()
    def schedule_exit(self, m: int):
        self.m=m
        self.deleteKey()

    def insert_in_dict(self,key, file_content):
        try:
            # print(f'55555555，{key}==》{self.time_dict}')
            if key not in self.time_dict:
                self.time_dict[key]={'frequency':1,'visit_time': time.time()}
            key_frequency = self.time_dict[key]["frequency"]
            value = {"frequency": key_frequency, "file_name": key, 'file_content': file_content}
            if key_frequency < self.min_frequency:
                return
            if len(self.frequency_cache_dict) == self.maxsize:
                min_freq_item = min(self.frequency_cache_dict, key=lambda x: self.frequency_cache_dict[x]['frequency'])
                self.min_frequency = min_freq_item
                # 删除frequency最小的项
                if self.frequency_cache_dict[min_freq_item]['frequency'] <= value['frequency']:
                    del self.frequency_cache_dict[min_freq_item]
            if len(self.frequency_cache_dict) < self.maxsize:
                self.frequency_cache_dict[key] = value
            self.min_frequency = value['frequency']
        except Exception as e:
            print(f'insert_in_dict里的错误：{e}')

# filenames=['file1.txt','file2.txt','file3.txt','file4.txt']
# k=2
# m=6
# my_lru_cache=lru_cache(k)
# my_lru_cache.schedule_exit(m)
# my_lru_cache.visitFile('file1.txt')
# my_lru_cache.visitFile('file2.txt')
# my_lru_cache.visitFile('file3.txt')
# my_lru_cache.visitFile('file4.txt')
# my_lru_cache.visitFile('file3.txt')
# my_lru_cache.visitFile('file4.txt')
# my_lru_cache.visitFile('file1.txt')
# my_lru_cache.visitFile('file1.txt')
# def temp():
#     my_lru_cache.visitFile('file3.txt')
# timer = threading.Timer(4,temp)
# timer.start()
# time.sleep(3)