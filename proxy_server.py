import asyncio
import multiprocessing
import time

import psutil

from communicate.message import MessageServer
from serves.my_raft import FakeNode, RaftNode
from util.SnowflakeIdWorker import SnowflakeIdWorker
from util.settings import serve_num, mediator_address, address, new_server_address
from util.load_balance import ConsistentHashLoadBalancer
from util.resolve_client import resolve_client

from enum import Enum

unique_obj = SnowflakeIdWorker()


def worker(node):
    p = psutil.Process()  # 当前进程
    p.nice(psutil.HIGH_PRIORITY_CLASS)
    asyncio.run(node.run({}, True))


class RequestStatus(Enum):
    WAITING = "waiting"
    PROCESSING = "processing"
    COMPLETED = "completed"


class Mediator():
    def __init__(self,server):
        self.server = server
        self.remove_node_id = None
        self.new_node_id = None
        self.pending_task_dict = {}
        self.leader_id = None
        self.send_msg_interval = 0.05
        self.nodes = [FakeNode(node_id) for node_id in address]
        self.leader_alive_timeout = 2 # 允许领导死掉两秒
        self.heartbeat_received = asyncio.Event()
        self.execute_pending_task_lock = asyncio.Lock()
        self.add_pending_task_lock = asyncio.Lock()
        self.init_balancer()

    def init_balancer(self):
        balancer = ConsistentHashLoadBalancer(replicas_node_count=10)  # 假设每个服务器有10个虚拟节点
        # 添加服务器节点
        for node in self.nodes:
            balancer.add_server(node.id)
        self.balancer = balancer

    async def send_msg(self, client_id, message, timestamp):
        # print('send_msg？？？')
        request_id = unique_obj.get_id()
        await self.add_pending_task((request_id, message, client_id, RequestStatus.WAITING, timestamp))

    def get_waiting_task(self):
        all_tasks = self.pending_task_dict.values()
        tasks = [task for task in all_tasks if task[3] == RequestStatus.WAITING]
        return tasks

    def get_processing_task(self):
        all_tasks = self.pending_task_dict.values()
        tasks = [task for task in all_tasks if task[3] == RequestStatus.PROCESSING]
        return tasks

    async def add_pending_task(self, message_tuple):
        await self.add_pending_task_lock.acquire()
        request_id = message_tuple[0]
        # print('request_id',request_id)
        if request_id in self.pending_task_dict:
            print('重复了request_id')
            return
        self.pending_task_dict[request_id] = message_tuple
        # print(f'add_pending_task啊啊啊:{len(self.pending_task_dict)}==={request_id}')
        self.add_pending_task_lock.release()
    async def execute_pending_task(self):
        # 执行队列里的没有执行的任务
        while True:
            # print('中间层活着')
            try:
                async with self.execute_pending_task_lock:
                    if self.leader_id and len(self.pending_task_dict) > 0:
                        waiting_task = self.get_waiting_task()
                        for one_waiting_task in waiting_task:
                            # print(f'待执行任务：{one_waiting_task}')
                            request_id = one_waiting_task[0]
                            temp_tup = self.pending_task_dict[request_id]
                            self.pending_task_dict[request_id] = temp_tup[:3] + (RequestStatus.PROCESSING,) + temp_tup[
                                                                                                              4:]
                            await self.send_waiting_msg(one_waiting_task)
            except Exception as e:
                print('更新self.leader_id的错误', e)
            await asyncio.sleep(self.send_msg_interval)

    async def run(self):
        try:
            loop = asyncio.get_event_loop()
            task1 = asyncio.create_task(self.get_message())
            task2 = asyncio.create_task(self.execute_pending_task())
            task3 = asyncio.create_task(self.judge_leader_exit())
            task4 = asyncio.create_task(self.update_server())
            await asyncio.gather(task1, task2, task3, task4)
        except Exception as e:
            print(f"Error in running tasks: {e}")

    async def judge_leader_exit(self):
        while True:
            # print('judge_leader_exit')
            try:
                await asyncio.wait_for(self.heartbeat_received.wait(), timeout=self.leader_alive_timeout)
                # print('replicate_logs_request===>中间层实现了心跳')
            except asyncio.TimeoutError:
                print('等待超时，将leader设置为空')
                self.leader_id = None

    async def get_message(self):
        while True:
            msg = await self.server.get_host_message()
            # print('get_message里的msg',msg)
            await self.process_message(msg)

    async def process_message(self, msg):
        msg_type = msg[0]
        if msg_type.startswith("server"):
            self.process_servers_message(msg)
        elif msg_type.startswith("client"):
            # print('啊啊 process_client_message', msg)
            await self.process_clients_message(msg)
        elif msg_type == 'replicate_logs_request':
            # print('收到了领导发来的心跳')
            try:
                self.heartbeat_received.set()
                self.heartbeat_received.clear()
                leader_term, leader_id, commit_index = msg[1], msg[2], msg[3]
                # print(leader_term, leader_id, commit_index,self.leader_id)
                if leader_id != self.leader_id:
                    print(f'中间层的leader变了___{leader_id}---{self.leader_id}')
                    if 'add_server' in self.pending_task_dict:
                        del self.pending_task_dict['add_server']
                        print('本次添加主机失败')
                    if 'remove_server' in self.pending_task_dict:
                        del self.pending_task_dict['remove_server']
                        print('本次移除主机失败')
                    print(f'更新self.leader_id:{leader_id}')
                    self.leader_id = leader_id
            except Exception as e:
                print('中间层replicate_logs_request的错误', e)

    async def send_waiting_msg(self, waiting_task):
        # print(f'waiting_task:{waiting_task}')
        try:
            request_id = waiting_task[0]
            if request_id == 'remove_server':
                node_id = waiting_task[1]
                await self.put_message_to_host(self.leader_id, ('leader_remove_server', request_id, node_id))
                return
            if request_id == 'add_server':
                node_id = waiting_task[1]
                await self.put_message_to_host(self.leader_id, ('leader_add_server', request_id, node_id))
                return
        except Exception as e:
            print(f'send_waiting_msg错误：{e}')
        request_id, msg, client_id, state, timestamp = waiting_task
        params = resolve_client(msg)
        op = params.get('op')
        file_path = params.get('file_path')
        if op == 'delete' or op == 'update':
            # 先删除所有节点的file_path的缓存
            for peer in self.nodes:
                # print(f'peer.id:{peer.id}')
                asyncio.create_task(self.put_message_to_host(peer.id, ('delete_cache', file_path)))
        if op == 'get':
            node_id = self.balancer.get_server(file_path)
            # node_id=self.leader_id
            # node_id=('127.0.0.1', 44444)
            print(f'{node_id},中间层get发送{msg},timestamp:{timestamp}')
            asyncio.create_task(self.put_message_to_host(node_id , ('mediator_send_msg_get', request_id, msg, client_id, timestamp, node_id)))
        else:
            print(f'中间层update发送:{op}')
            asyncio.create_task(self.put_message_to_host(self.leader_id, ('mediator_send_msg_modified', request_id, msg, client_id, timestamp, self.leader_id)))

    def process_servers_message(self, msg):
        msg_type = msg[0]
        request_id = msg[1]
        if msg_type == 'server_send_response':
            if request_id == 'add_server':
                try:
                    self.nodes.append(FakeNode(self.new_node_id))
                    print('中间层输出：节点添加成功')
                    self.balancer.add_server(self.new_node_id)
                    next_node_id = self.balancer.get_next_server(self.new_node_id)
                    print('next_node_id',next_node_id)
                    asyncio.create_task(self.put_message_to_host(next_node_id, ('delete_cache',)))
                    self.new_node_id = None
                except Exception as e:
                    print(f'add_server判断是否成功的错误：{e}')
            if request_id == 'remove_server':
                self.nodes = [node for node in self.nodes if node.id != self.remove_node_id]
                self.balancer.remove_server(self.remove_node_id)
                print('节点移除成功')
                self.remove_node_id = None

    async def process_clients_message(self, msg):
        try:
            msg_type = msg[0]
            if msg_type == 'client_send_msg':
                # print('中间层收到了一条消息')
                _, client_id, message, timestamp = msg
                await self.send_msg(client_id, message, timestamp)
        except Exception as e:
            print('process_clients_message的错误', e)

    async def add_server(self, new_node_id):
        try:
            if self.leader_id:
                node_exists = any(peer.id == new_node_id for peer in self.nodes)
                if node_exists:
                    print('已有该主机')
                    return
                self.new_node_id = new_node_id
                await self.add_pending_task(('add_server', new_node_id, '_', RequestStatus.WAITING, time.time()))
            else:
                print('暂无leader节点，请稍后add_server')
        except Exception as e:
                print('add_server的错误', e)

    async def remove_server(self, remove_node_id):
        if self.leader_id:
            node_exists = any(peer.id == remove_node_id for peer in self.nodes)
            if remove_node_id == self.leader_id:
                print('不可移除领导节点')
                return
            if not node_exists:
                print('节点并不存在')
                return
            self.remove_node_id = remove_node_id
            await self.add_pending_task(('remove_server', remove_node_id, '_', RequestStatus.WAITING, time.time()))
        else:
            print('暂无leader节点，请稍后remove_server')

    async def update_server(self):
        # pass
        await asyncio.sleep(5)
        # print('增加一台主机')
        # await self.add_server(new_server_address)
        # print('删掉一台主机，4444')
        # await self.remove_server(address[1])

    # def start_process(self, new_node_id):
    #     try:
    #         new_node = RaftNode(new_node_id)
    #         new_node.set_peers(self.nodes + [new_node])
    #         p = multiprocessing.Process(target=worker, args=(new_node,))
    #         p.start()
    #         # p.join()
    #     except Exception as e:
    #         print('start_process的错误', e)

    async def put_message_to_host(self, address_id, message_tuple):
        try:
            await self.server.send_host_message(address_id, message_tuple)
        except Exception as e:
            if message_tuple[0] == 'mediator_send_msg_get':
                await self.server.send_host_message(self.leader_id, message_tuple)

async def main():
    host, port = mediator_address
    server = MessageServer(host, port)
    # 启动服务端
    asyncio.create_task(server.start_server())
    mediator = Mediator(server)
    await mediator.run()
if __name__ == '__main__':
    asyncio.run(main())