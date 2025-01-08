import ast
import asyncio

from util.settings import mediator_address, address, client_address


class MessageQueue:
    def __init__(self):
        self.queue = asyncio.Queue(maxsize=100)  # 使用asyncio.Queue来存储消息

    async def get_message(self):
        """异步获取消息，如果队列为空则会挂起，直到有消息"""
        message = await self.queue.get()  # 阻塞等待直到队列有消息
        return message

    async def put_message(self, message):
        await self.queue.put(message)  # 异步放入消息队列

class MessageServer:
    def __init__(self, host='localhost', port=12345):
        self.establish_connection_lock =asyncio.Lock()
        self.writer_arr = []
        self.signal_handler = None
        self.send_host_message_lock = asyncio.Lock()
        self.get_message_lock = asyncio.Lock()
        self.retry_delay = 0.5
        self.message_queue = MessageQueue()
        self.host = host
        self.port = port
        # self.connections = {}  # 用于保存多个连接,储存给某个服务端写的writer

    async def start_server(self):
        self.server = await asyncio.start_server(self.get_message_core, self.host, self.port)
        try:
            async with self.server:
                await self.server.serve_forever()
        except asyncio.CancelledError:
            print("服务结束")
        # finally:
        #     await self.shutdown_server()
    def signal_handler(self):
        """捕获信号并关闭服务器"""
        print("Signal received, shutting down...")
        self.is_running = False  # 标记为非运行状态
        asyncio.create_task(self.shutdown_server())

    async def shutdown_server(self):
        """优雅关闭服务器并通知其他服务器"""
        # 通知所有连接的对端关闭
        for addr, writer in self.connections.items():
            try:
                if not writer.is_closing():
                    writer.write(b"Server is shutting down...\n")
                    await writer.drain()
                writer.close()
                await writer.wait_closed()
                print(f"Connection to {addr} closed.")
            except Exception as e:
                print(f"Error closing connection to {addr}: {e}")

        # 停止服务器
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            print("Server stopped.")

        # 退出事件循环
        loop = asyncio.get_event_loop()
        loop.stop()
    async def establish_connection(self, target_host, target_port,message):
        connection_id = (target_host, target_port)
        try:
            reader, writer = await asyncio.open_connection(target_host, target_port)
            return writer
        except Exception as e:
            print(f'错误：关闭对应连接，send_host_message里的错误:{e}--->{connection_id}', )
            return None
    async def get_message_core(self, socket_reader, socket_writer):
        addr = socket_writer.get_extra_info('peername')
        try:
            while True:
                data = await socket_reader.readline()
                if not data:  # 连接关闭时，data 会为空
                    break
                message_str = data.decode().strip()  # 解码并去掉换行符
                try:
                    # print(message_str,'message_str')
                    message_tuple = ast.literal_eval(message_str)
                    message_type=message_tuple[0]
                    # if message_type=='server_send_response':
                        # pass
                        # print(f"最开始————收到了一条消息: {message_str}===={addr}")
                    if message_tuple:
                        new_tuple = message_tuple[:-1]
                        await self.message_queue.put_message(new_tuple)
                except Exception as e:
                    print(f"解析消息时出错: {e}==》{message_str}")
        # except asyncio.CancelledError as e:
        #     print('错误：一个异步任务被取消了', e)
        #     await self.close_connection(addr)
        # except ConnectionResetError as e:
        #     print(f'错误：接收消息时，当前建立的长连接中，对方已经自发关闭了:{e}==》{addr}')
        #     await self.close_connection(addr)
        except Exception as e:
            print(f"错误：get_message_core中的连接发生其他出错了：{e}")
            # await self.close_connection(addr)
        finally:
            socket_writer.close()
            await socket_writer.wait_closed()

    async def get_host_message(self):
        message = await self.message_queue.get_message()
        return message

    async def send_host_message(self, server_id, message, is_again_request=False, retry_count=3):
        self.is_sending=True
        if server_id == 'Mediator':
            host, port = mediator_address
        elif server_id == 'client':
            host, port = client_address
        else:
            host, port = server_id
        server_id=(host, port)
        message = message + (is_again_request,)
        writer = await self.establish_connection(host, port,message)
        try:
            if not writer:
                # print('在没有writer地方重试发送！！！！')
                asyncio.create_task(self.retry_server_send_response(message, server_id, retry_count))
                return
            if writer.is_closing():
                # print('在writer.is_closing地方重试发送！！！！')
                asyncio.create_task(self.retry_server_send_response(message, server_id, retry_count))
                return
        except Exception as e:
            print(f'出错了：{e}')
        message_str = str(message) + "\n"
        try:
            writer.write(message_str.encode())
            await writer.drain()
            if is_again_request:
                print(f'重试发送了一条消息：{message_str}')
            # elif message[0]=='server_send_response' or message[0]=='dead':
            # print(f'发送成功一条消息！！！！：{message[0]}')
        except Exception as e:
            print(f'错误：writer.drain里的错误: {e}--->{server_id}-->{message}', )
            # await self.close_connection((host, port))
            if message[0] == 'mediator_send_msg_get':
                raise e
            print('在错误重试发送！！！！')
            asyncio.create_task(self.retry_server_send_response(message, server_id, retry_count))
        finally:
            writer.close()
            await writer.wait_closed()

    async def retry_server_send_response(self,message,server_id,retry_count):
        await asyncio.sleep(1)
        if message[0] == 'server_send_response' and server_id == client_address:
            if retry_count > 0:
                new_message = message[:-1]
                asyncio.create_task(self.send_host_message(server_id, new_message, True, retry_count - 1))


