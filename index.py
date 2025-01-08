
import asyncio
from communicate.message import MessageServer
from serves.my_raft import RaftNode, FakeNode
from util.settings import address

async def task_two(server, launchSynchronizer):
    while True:
        message = await server.get_host_message()
        launchSynchronizer.append_ready_node(message)
        if launchSynchronizer.is_ready:
            break

async def task_one(launchSynchronizer):
    while True:
        await launchSynchronizer.send_ready_signal()
        if launchSynchronizer.is_ready:
            break
        await asyncio.sleep(3)

async def index(address_index,is_new=False):
    try:
        if isinstance(address_index,int):
            host, port=address[address_index]
        else:
            host, port = address_index
        server = MessageServer(host, port)
        node = RaftNode((host, port),server,is_new)
        peers=[FakeNode(one_address) for one_address in address]
        node.set_peers(peers)
        # 启动服务端
        asyncio.create_task(server.start_server())
        await node.run()
    except Exception as e:
        # 其他异常也可以处理
        print(f"'入口函数': {e}")

# 服务端关闭无响应后，其他主机的报错处理？
# 为什么捕捉了错误后，leader就不发心跳消息了？