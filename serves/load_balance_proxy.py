import asyncio
import os

from util.settings import peer_address


def get_all_nodes():
    return peer_address


class LoadBalanceProxy:
    def __init__(self,server):
        self.server = server
        self.leader_address = None

    def set_leader(self, ip_port=None):
        """设置当前的 leader IP 和端口。
        Args:
            ip_port (tuple): Leader 的 IP 地址和端口（格式：(IP, PORT)）。
        """
        self.leader_address = ip_port
    def get_leader(self):
        """获取当前的 leader IP 和端口。

        Returns:
            tuple: Leader 的 IP 地址和端口（格式：(IP, PORT)）。
        """
        if not self.leader_address:
            return None
        return self.leader_address

    def get_peers(self):
        return peer_address
    async def init_core(self):
        peers=get_all_nodes()
        for peer in peers:
            asyncio.create_task(self.put_message_to_host(peer,('load_send_peers',peers)))

    async def put_message_to_host(self, address_id, message_tuple):
        try:
            await self.server.send_host_message(address_id, message_tuple)
        except Exception as e:
            print('负载均衡里的错误',e)

    async def run(self):
        try:
            task1 = asyncio.create_task(self.get_message())
            task2 = asyncio.create_task(self.init_core())
            await asyncio.gather(task1, task2)
        except Exception as e:
            print(f"Error in running tasks: {e}")

    async def get_message(self):
        while True:
            msg = await self.server.get_host_message()
            await self.process_message(msg)

    async def process_message(self, msg):
        msg_type = msg[0]
        if msg_type=='set_load_leader':
            # 事件驱动
            leader_address=msg[1]
            self.set_leader(leader_address)
        elif msg_type == 'replicate_logs_request':
            # 心跳机制
            try:
                leader_term, leader_address, commit_index = msg[1], msg[2], msg[3]
                if leader_address != self.leader_address:
                    print(f'负载均衡层的leader变了___{leader_address}---{self.leader_address}')
                    self.set_leader(leader_address)
            except Exception as e:
                print('负载均衡层的同步心跳的错误', e)