import asyncio

from communicate.message import MessageServer
from serves.load_balance_proxy import LoadBalanceProxy
from util.settings import load_balance_address


async def main():
    host, port = load_balance_address
    server = MessageServer(host, port)
    # 启动服务端
    asyncio.create_task(server.start_server())
    load_balance = LoadBalanceProxy(server)
    await load_balance.run()
if __name__ == '__main__':
    asyncio.run(main())