import asyncio
import multiprocessing

import psutil

from distributed.my_raft import RaftNode
from distributed.serves.load_balance import ConsistentHashLoadBalancer
from distributed.serves.resolve_client import resolve_client

all_operation=['add','update','get','delete']


def generate_statement(method, data):
    # 检查必要的键是否存在
    required_keys = ['projectName', 'objectName', 'id']
    if not all(key in data for key in required_keys):
        raise ValueError("Missing required keys in data dictionary")

    # 构造基础语句部分
    base_statement = f"{method} {data['projectName']}.{data['objectName']}.{data['id']}"

    # 根据不同的method构造完整的语句
    if method == "get":
        if ('properties' not in data) or not isinstance(data['properties'],list) :
            raise ValueError("你要查询一个文件，但是properties定义错误")
        properties = data.get('properties', [])
        properties_str = ','.join(properties)
        return f"{base_statement} {properties_str}"
    elif method == "add":
        if 'properties' not in data or not isinstance(data['properties'],dict):
            raise ValueError("你要添加一个文件，但是properties定义错误")
        properties = data.get('properties')
        properties_str = ','.join(f"{k}={v}" for k, v in properties.items())
        return f"{base_statement} {properties_str}"
    elif method == "delete":
        return f"{base_statement}"
    elif method == "update":
        if 'properties' not in data or not isinstance(data['properties'],dict):
            raise ValueError("你要更新一个文件，但是properties定义错误")
        properties = data.get('properties')
        properties_str = ','.join(f"{k}={v}" for k, v in properties.items())
        return f"{base_statement} {properties_str}"
    else:
        raise ValueError("Unsupported method")
def worker(node):
    p = psutil.Process()  # 当前进程
    p.nice(psutil.HIGH_PRIORITY_CLASS)
    asyncio.run(node.run())

class Client:
    def __init__(self,client_id,queues_dict):
        self.pending_task = []
        self.client_id=client_id
        self.queues_dict = queues_dict
        self.lead_id=None
    def receive_msg(self,msg):
        print(f'{self.client_id}客户端收到了消息，消息说：{msg}')
    def send_msg(self,msg):
        print(f'{self.client_id}客户端发送了消息，消息是：{msg}')
        params = resolve_client(operate_str=msg)
        operational = params.get("operational", False)
        if not operational:
            self.receive_msg(params.get("tip", "暂无提示"))
        if self.lead_id:
            self.queues_dict[self.lead_id].put(('client_send',self,msg))
        else:
            self.receive_msg('暂无leader节点，请稍后')
    def get_pending_task(self):
        return self.pending_task
    def add_pending_task(self,message):
        self.pending_task.append(message)

    async def client_do_core(self):
        await asyncio.sleep(5)
        print('客户端发起请求')
        my_client_str1 = generate_statement('add', {'projectName': 'B', 'objectName': 2, 'id': 'id1',
                                                    "properties": {'name': 'mike', 'age': '50'}})
        my_client_str2 = generate_statement('add', {'projectName': 'B', 'objectName': 2, 'id': 'id2',
                                                    "properties": {'name': 'jane', 'age': '12'}})
        my_client_str3 = generate_statement('update', {'projectName': 'B', 'objectName': 2, 'id': 'id2',
                                                       "properties": {'age': '15'}})
        self.send_msg(my_client_str1)
        self.send_msg(my_client_str2)
        self.send_msg(my_client_str3)

    async def client_init(self):
        loop = asyncio.get_event_loop()
        task1 = asyncio.create_task(self.client_do_core())
        task1.set_name('client_do_core')
        task2 = asyncio.create_task(self.get_message(loop))
        task2.set_name('get_message')
        # 管理员操作集群
        task3 = asyncio.create_task(self.add_server())
        task3.set_name('add_server')
        task4 = asyncio.create_task(self.remove_server())
        task4.set_name('add_server')
        await asyncio.gather(task1, task2,task3,task4)
    def get_message_core(self, loop):
        while True:
            msg = self.queues_dict[self.client_id].get()
            asyncio.run_coroutine_threadsafe(self.process_message(msg), loop)
    async def get_message(self, loop):
        coro = asyncio.to_thread(self.get_message_core, loop)
        task = asyncio.create_task(coro)
        await task

    async def process_message(self, msg):
        msg_type = msg[0]
        if msg_type == 'update_leader':
            lead_id=msg[1]
            self.lead_id=lead_id

    async def add_server(self):
        await asyncio.sleep(10)
        # 起一个进程表示新增的服务器节点
        node=RaftNode(6)
        nodes = [{id:i} for i in range(5)]
        nodes.append(node)
        node.set_peers(nodes)
        balancer = ConsistentHashLoadBalancer(replicas_node_count=3)
        node.set_balancer(balancer)
        node.set_queues_dict(self.queues_dict)
        p = multiprocessing.Process(target=worker, args=(node,))
        p.start()
        p.join()
        if self.lead_id:
            self.queues_dict[self.lead_id].put(('update_server','add_server','server1'))
        else:
            self.receive_msg('暂无leader节点，请稍后add_server')
        pass

    async def remove_server(self):
        await asyncio.sleep(20)
        if self.lead_id:
            self.queues_dict[self.lead_id].put(('update_server','remove_server', 'server8'))
        else:
            self.receive_msg('暂无leader节点，请稍后remove_server')