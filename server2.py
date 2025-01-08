
import asyncio
import multiprocessing

from communicate.message import MessageServer
from serves.my_raft import RaftNode, FakeNode
from util.const import address


async def main(address_index):
    try:
        host, port=address[address_index]
        server = MessageServer(host, port)
        node = RaftNode((host, port),server)
        peers=[FakeNode(one_address) for one_address in address]
        node.set_peers(peers)
        # 启动服务端
        asyncio.create_task(server.start_server())
        await node.run()
    except Exception as e:
        # 其他异常也可以处理
        print(f"'入口函数': {e}")

if __name__ == '__main__':
    asyncio.run(main(0))