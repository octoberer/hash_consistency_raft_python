
import numpy as np
import time
from lfu import lru_cache
# import multiprocessing
from multiprocessing import Pool



def gaussian_random(n, mean, std_dev):
    return np.random.normal(mean, std_dev, n)
def worker(data_chunk):
    # 每个进程处理一个数据块
    k = 200
    m = 60
    my_lru_cache = lru_cache(k)
    my_lru_cache.schedule_exit(m)
    print(len(data_chunk))
    for file_index in data_chunk:
        my_lru_cache.visitFile(f'file_{file_index}.txt')
def test_random_access(num_accesses):
    start_time = time.time()
    accesses = gaussian_random(num_accesses, 50000, 100).astype(int)
    accesses = np.clip(accesses, 0, 99999)
    # cpu_cores = multiprocessing.cpu_count()
    # print('cpu_cores',cpu_cores)
    chunk_size = len(accesses)//4
    chunks = [accesses[i:i + chunk_size] for i in range(0, len(accesses), chunk_size)]
    with Pool(4) as p:
        p.map(worker, chunks)
    # k = 200
    # m = 60
    # my_lru_cache = lru_cache(k)
    # my_lru_cache.schedule_exit(m)
    # for file_index in accesses:
    #     my_lru_cache.visitFile(f'file_{file_index}.txt')
    print(f'访问 {len(accesses)} 个文件用时 {time.time() - start_time:.2f} 秒')

test_cases = [200, 400, 600, 800, 1000, 2000, 4000, 8000, 20000, 50000, 100000, 200000, 500000, 1000000]
if __name__ == '__main__':
    test_random_access(1000000)
# for num_accesses in test_cases:
#     test_random_access(num_accesses,my_lru_cache)