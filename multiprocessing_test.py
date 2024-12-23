import random
from multiprocessing import Process
from operate import  schedual_operate
all_operation=['add','update','get','delete']
max_file_count=100
def worker(op):
    """线程工作函数"""
    first_names = ['Alice', 'Bob', 'Charlie', 'David', 'Eve']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones']
    if op=='add':
        for i in range(0,max_file_count):
            print(i)
            first = random.choice(first_names)
            last = random.choice(last_names)
            schedual_operate(f"add A.user.{str(i).zfill(3)} name={first} {last},age={random.randint(0, 100)}")
    elif op=='get':
        for i in range(0, 500):
            find_index=random.randint(0, max_file_count)
            print(i)
            schedual_operate(f"get A.user.{str(find_index).zfill(3)} name,age")
    elif op=='update':
        for i in range(0, 50):
            update_index = random.randint(0, max_file_count)
            first = random.choice(first_names)
            last = random.choice(last_names)
            schedual_operate(
                f"update A.user.{str(update_index).zfill(3)} name={first} {last},age={random.randint(0, 100)}")
    elif op=='delete':
        for i in range(0, 30):
            delete_index = random.randint(0, max_file_count)
            schedual_operate(f"delete A.user.{str(delete_index).zfill(3)}")


if __name__ == '__main__':
    # 创建Process对象列表
    processes = []
    for op in all_operation:
        p = Process(target=worker, args=(op,))
        processes.append(p)
        p.start()  # 启动进程

    for p in processes:
        p.join()  # 等待进程执行结束



