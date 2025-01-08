import asyncio
import json
import os
import re

import aiofiles
import psutil

from operate_file import serve_num


def create_log_file(entry):
    # 从 entry 中提取必要信息
    temp_content = entry.get('properties_dict','')  # 获取 temp_content
    client_id = entry.get('client', None)  # 获取 client
    # 创建日志条目字典
    log_entry = json.dumps({
        **entry,
        "properties_dict": temp_content,  # 转换为 JSON 字符串
        "client_id": client_id,  # 使用 client.client_id
    })

    return log_entry


def print_sys():
    # 获取当前进程的PID
    current_pid = os.getpid()

    # 获取当前进程对象
    current_process = psutil.Process(current_pid)

    # 获取进程状态
    status = current_process.status()

    # 打印进程状态
    print(f"当前进程ID: {current_pid}")
    print(f"当前进程状态: {status}")

    # 你也可以获取更多关于进程的信息
    info = current_process.as_dict(attrs=['pid', 'name', 'status', 'memory_percent'])
    print(info)


monitor_tasks_time = 7


async def monitor_tasks():
    global monitor_tasks_time
    while True:
        await asyncio.sleep(monitor_tasks_time)
        monitor_tasks_time = 1
        tasks = asyncio.all_tasks()
        # print(f"Active tasks Active tasks Active tasks: {len(tasks)}")
        for task in tasks:
            pass
            # print(task)  # 打印任务信息


def get_max_id(ids):
    # 从 id 中提取数字部分，并找到最大值
    max_id = max(ids, key=lambda x: int(x.replace('server', '')))
    return int(max_id.replace('server', ''))


def getServerName(node_id):
    return 'server' + str(node_id)


async def save_config(config, file_path):
    # 异步创建目录
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    # 异步写入配置到文件
    async with aiofiles.open(file_path, 'w') as f:
        await f.write(json.dumps(config, indent=2))

def increment_server_name(server_name):
    # 使用正则表达式找到所有的数字
    numbers = re.findall(r'\d+', server_name)
    # 假设最后一个数字是需要递增的部分
    if numbers:
        last_number = (int(numbers[-1]) + 1)%serve_num
        # 替换掉原来的数字部分
        new_server_name = re.sub(r'\d+', str(last_number), server_name, count=1)
    else:
        # 如果没有找到数字，返回原字符串
        new_server_name = server_name
    return new_server_name