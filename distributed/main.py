# 使用示例
import asyncio
import multiprocessing
import psutil
from multiprocessing import Manager
from distributed.my_raft import RaftNode
from distributed.operate_file import serve_num
from distributed.serves.client import Client
from distributed.serves.load_balance import ConsistentHashLoadBalancer



# ———————模拟创建五个服务器，并将其套上raft算法————————


def worker(nodes, index):
    # print(nodes[index])
    # os.sched_setaffinity(0, {index})
    p = psutil.Process()  # 当前进程
    p.nice(psutil.HIGH_PRIORITY_CLASS)
    asyncio.run(nodes[index].run())

def client_do(queues_dict):
    client_id="client_1"
    client1=Client(client_id,queues_dict)
    asyncio.run(client1.client_init())

def main():
    # message_queue = multiprocessing.Queue()
    with Manager() as manager:
        queues_dict = {'server' + str(node_id): manager.Queue() for node_id in range(serve_num)}
        # 创建共享的节点实例
        nodes = [RaftNode(i) for i in range(serve_num)]
        # ———————创建一致性hash环————————
        balancer = ConsistentHashLoadBalancer(replicas_node_count=3)  # 假设每个服务器有100个虚拟节点

        # 添加服务器节点
        for node in nodes:
            balancer.add_server(node.id)
        # ———————添加balancer和raft节点的关联—————————
        for node in nodes:
            node.set_peers(nodes)
            node.set_balancer(balancer)
            node.set_queues_dict(queues_dict)

        # 启动节点
        # 启动每个节点的进程
        processes = []

        for i in range(serve_num):
            p = multiprocessing.Process(target=worker, args=(nodes, i))
            p.start()
            processes.append(p)


        # 模拟一个客户端
        p = multiprocessing.Process(target=client_do, args=(queues_dict,))
        processes.append(p)
        p.start()
        # 运行一段时间后停止
        for p in processes:
            p.join()  # 等待所有进程完成




if __name__ == '__main__':
    main()



