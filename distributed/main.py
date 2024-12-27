# 使用示例
import asyncio
import multiprocessing
import os

import psutil
from multiprocessing import Manager
from distributed.my_raft import RaftNode
from distributed.operate_file import serve_num
from distributed.serves.client import Client
from distributed.serves.load_balance import ConsistentHashLoadBalancer
from distributed.serves.mediator import Mediator


# ———————模拟创建五个服务器，并将其套上raft算法————————


def worker(nodes, index,sibling_pids_dict):
    pid = os.getpid()
    sibling_pids_dict[nodes[index].id]=pid
    # print('pid', pid, sibling_pids_dict)
    p = psutil.Process()  # 当前进程
    p.nice(psutil.HIGH_PRIORITY_CLASS)
    asyncio.run(nodes[index].run(sibling_pids_dict))

def client_do(client1):
    p = psutil.Process()  # 当前进程
    p.nice(psutil.HIGH_PRIORITY_CLASS)
    asyncio.run(client1.client_init())

def mediator_do(mediator,new_queue):
    p = psutil.Process()  # 当前进程
    p.nice(psutil.HIGH_PRIORITY_CLASS)
    asyncio.run(mediator.run(new_queue))

def main():
    # message_queue = multiprocessing.Queue()
    try:
        with Manager() as manager:
            queues_dict=manager.dict()
            queues_dict.update({'client_1': manager.Queue(), 'Mediator': manager.Queue()})
            queues_dict_1 = {'server' + str(node_id): manager.Queue() for node_id in range(serve_num)}
            queues_dict.update(queues_dict_1)
            new_queue=manager.Queue()
            # print('main',queues_dict)
            processes = []
            # 创建共享的节点实例
            nodes = [RaftNode(i,queues_dict) for i in range(serve_num)]
            # ———————创建一致性hash环————————
            balancer = ConsistentHashLoadBalancer(replicas_node_count=3)  # 假设每个服务器有100个虚拟节点

            # 添加服务器节点
            for node in nodes:
                balancer.add_server(node.id)
            # ———————添加balancer和raft节点的关联—————————
            for node in nodes:
                node.set_peers(nodes)
            sibling_pids_dict = manager.dict()
            for i in range(serve_num):
                p = multiprocessing.Process(target=worker, args=(nodes, i,sibling_pids_dict))
                p.start()
                processes.append(p)

            # 模拟一个客户端
            client_id = "client_1"
            client1 = Client(client_id,queues_dict)
            p = multiprocessing.Process(target=client_do, args=(client1,))
            processes.append(p)
            p.start()
            # 模拟一个中间层
            mediator = Mediator(queues_dict)
            mediator = multiprocessing.Process(target=mediator_do, args=(mediator,new_queue,))
            processes.append(mediator)
            mediator.start()

            # 运行一段时间后停止
            for p in processes:
                p.join()  # 等待所有进程完成
    except Exception as e:
        # 其他异常也可以处理
        print(f"'入口函数': {e}")


if __name__ == '__main__':
    main()
