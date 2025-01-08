import asyncio
import json
import os
import queue
import time

import aiofiles
import psutil

from distributed.my_raft import RaftNode
from distributed.serves.load_balance import ConsistentHashLoadBalancer
from distributed.serves.resolve_client import resolve_client
import random

all_operation = ['add', 'update', 'get', 'delete']


def generate_statement(method, data):
    # 检查必要的键是否存在
    required_keys = ['projectName', 'objectName', 'id']
    if not all(key in data for key in required_keys):
        raise ValueError("Missing required keys in data dictionary")

    # 构造基础语句部分
    base_statement = f"{method} {data['projectName']}.{data['objectName']}.{data['id']}"

    # 根据不同的method构造完整的语句
    if method == "get":
        if ('properties' not in data) or not isinstance(data['properties'], list):
            raise ValueError("你要查询一个文件，但是properties定义错误")
        properties = data.get('properties', [])
        properties_str = ','.join(properties)
        return f"{base_statement} {properties_str}"
    elif method == "add":
        if 'properties' not in data or not isinstance(data['properties'], dict):
            raise ValueError("你要添加一个文件，但是properties定义错误")
        properties = data.get('properties')
        properties_str = ','.join(f"{k}={v}" for k, v in properties.items())
        return f"{base_statement} {properties_str}"
    elif method == "delete":
        return f"{base_statement}"
    elif method == "update":
        if 'properties' not in data or not isinstance(data['properties'], dict):
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
    def __init__(self, client_id, queues_dict):
        self.pending_task = []
        self.client_id = client_id
        self.queues_dict = queues_dict
        self.lead_id = None
        self.request_id_dict={}

    def receive_msg(self, msg):
        pass
        # print(f'{self.client_id}客户端收到了消息，消息说：{msg}')

    def send_msg(self, msg):
        if isinstance(msg, list):
            for one_msg in msg:
                self.send_one_msg(one_msg)
        elif isinstance(msg, str):
            self.send_one_msg(msg)

    def send_one_msg(self, one_msg):

        params = resolve_client(operate_str=one_msg)
        operational = params.get("operational", False)
        if not operational:
            self.receive_msg(params.get("tip", "暂无提示"))
        else:
            try:
                print(f'{self.client_id}客户端发送了一条消息')
                self.queues_dict['Mediator'].put(('client_send_msg', self.client_id, one_msg, time.time()))
            except queue.Full:
                print('????queue.Full')

    def get_message_core(self, loop):
        while True:
            msg = self.queues_dict['client_1'].get()
            asyncio.run_coroutine_threadsafe(self.process_message(msg), loop)

    async def get_message(self, loop):
        coro = asyncio.to_thread(self.get_message_core, loop)
        task = asyncio.create_task(coro)
        await task

    async def process_message(self, msg):
        msg_type = msg[0]
        # print(f'客户端收到消息，{msg}')
        if msg_type == 'server_send_response':
            content,operate_str,request_id = msg[1],msg[2],msg[3]
            if request_id in self.request_id_dict:
                # print('收到重复的')
                return
            self.request_id_dict[request_id]=1
            params=resolve_client(operate_str)
            op=params.get('op')
            if op=='get':
                self.receive_msg(f'操作响应===》{request_id}')

    async def client_do_core(self):
        try:
            await asyncio.sleep(1)
            # print('客户端发起请求')
            await self.batch_test(50,True)
            await asyncio.sleep(6)
            print('再次执行测试')
            await self.batch_test(100)
        except Exception as e:
            print(f"client_do_core有问题: {e}")

    async def client_init(self):
        # pass
        loop = asyncio.get_event_loop()
        task1 = asyncio.create_task(self.get_message(loop))
        task2 = asyncio.create_task(self.client_do_core())
        task2.set_name('client_do_core')
        await asyncio.gather(task1, task2)

    # 批量测试函数
    async def batch_test(self,query_num,firstWrite=False):
        # 假设我们有一些文件要操作
        files = []
        # 存储所有操作结果的日志
        operation_log = []
        try:
            # 1. 增加20个文件
            for i in range(1, 21):  # 增加20个文件
                file_data = {
                    'projectName': 'Project_A',
                    'objectName': 1,
                    'id': f'id{i}',
                    'properties': {'name': f'Name_{i}', 'age': str(random.randint(20, 60))}
                }
                msg = generate_statement('add', file_data)
                files.append(file_data)
                if firstWrite:
                    self.send_msg(msg)
                    # 记录添加操作
                    operation_log.append(f"Added file: {json.dumps(file_data, indent=2)}")
            print('查询所有文件')
            # 2. 查询所有文件 100次
            for i in range(query_num):
                file_to_query = random.choice(files)  # 随机选择一个文件进行查询
                query_data = {
                    'projectName': 'Project_A',
                    'objectName': 1,
                    'id': file_to_query['id'],
                    'properties':['name']
                }
                msg = generate_statement('get', query_data)
                self.send_msg(msg)

                # 记录查询操作
                operation_log.append(f"Queried file: {json.dumps(query_data, indent=2)}")

            # 3. 对部分文件进行修改（更新操作）
            files_to_update = random.sample(files, 5)  # 随机选择5个文件进行更新
            for file_data in files_to_update:
                updated_data = {
                    'projectName': 'Project_A',
                    'objectName': 1,
                    'id': file_data['id'],
                    'properties': {
                        'age': str(random.randint(60, 100))  # 随机修改年龄
                    }
                }
                msg = generate_statement('update', updated_data)
                self.send_msg(msg)

                # 记录更新操作
                operation_log.append(f"Updated file: {json.dumps(updated_data, indent=2)}")

                # 更新files中的文件数据
                for file in files:
                    if file['id'] == updated_data['id']:
                        file['properties'] = updated_data['properties']

        # 4. 查询所有文件（包含修改后的文件）再次进行查询
            for i in range(query_num):  # 再次查询100次
                file_to_query = random.choice(files)  # 随机选择一个文件进行查询
                query_data = {
                    'projectName': 'Project_A',
                    'objectName': 1,
                    'id': file_to_query['id'],
                    'properties': ['age']
                }
                msg = generate_statement('get', query_data)
                self.send_msg(msg)

                # 记录查询操作
                operation_log.append(f"Re-Queried file: {json.dumps(query_data, indent=2)}")

            # await write_file_contents(files,operation_log,firstWrite)
            # 输出所有文件最终内容
            # print("\nFinal file contents:")
            # for file in files:
            #     print(f"File ID: {file['id']}, Content: {json.dumps(file, indent=2)}")
        except Exception as e:
            print(f"客户端发送请求有问题: {e}")

# 异步写入文件的函数
async def write_file_contents(files,operation_log,firstWrite=False,output_file_path='real/log'):
    print('执行了文件写入99999999999')
    # 获取当前脚本所在的目录
    script_directory = os.path.dirname(os.path.abspath(__file__))
    # 拼接目标文件的完整路径
    full_output_path = os.path.join(script_directory, output_file_path)
    # 确保目录存在
    directory = os.path.dirname(full_output_path)
    os.makedirs(directory, exist_ok=True)
    if firstWrite:
        async with aiofiles.open(f'{full_output_path}', 'w') as file:
            # await file.write("\nFinal file contents:\n")
            # for file_data in files:
            #     # 格式化每个文件的内容为 JSON 格式，并异步写入到文件
            #     await file.write(f"File ID: {file_data['id']}, Content: {json.dumps(file_data, indent=2)}\n")
            await file.write(f"----------------------------------------\n")
            await file.write(f"operation_log: {json.dumps(operation_log, indent=2)}\n")
    else:
        async with aiofiles.open(f'{full_output_path}', 'a') as file:
            await file.write(f"----------------------------------------\n")
            await file.write(f"operation_log: {json.dumps(operation_log, indent=2)}\n")
