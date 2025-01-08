import json
import os
import queue
import time
import random

import aiofiles
import asyncio

from util.settings import client1_address, address
from util.operate_file import add_file, delete_file, update_file
from util.resolve_client import resolve_client
from util.helper_util import create_log_file, save_config
from util.lfu_cache import lfu_cache


class FakeNode():
    def __init__(self, node_id):
        self.id = node_id


async def handle_task_result(task):
    try:
        task.result()  # 如果任务有异常，这里会抛出
    except Exception as e:
        print(f"异步任务出错: {e}")


class RaftCore:
    def __init__(self, node_id, server, is_new):
        self.is_dead = False
        self.resolve_follower_replicate_response_lock = asyncio.Lock()
        self.send_vote_lock = asyncio.Lock()
        self.server = server
        self.append_log_lock = asyncio.Lock()
        self.replicate_to_follower_lock = asyncio.Lock()
        self.append_entries_lock = asyncio.Lock()
        self.apply_lock = asyncio.Lock()
        self.run_loop = None
        # {id:flag}
        self.current_votes_received = {}
        self.leader_id = None
        self.id = node_id
        # 村粗所有主机id
        self.peers = []
        self.state = 'FOLLOWER'
        self.current_term = 0
        self.voted_for = None
        self.log = []
        # 记录各个主机的当前已经把log存到内存的最大值
        self.commit_index = -1
        # 记录各个主机的当前已经把log存到文件系统的最大值
        self.applied_index = -1
        # 记录各个follower将要被leader同步的起点
        self.next_index = {}
        # 记录各个follower已经同步log的最大值,用这个判断是否绝大多数都commit了
        self.match_index = {}
        self.election_timeout = self.get_random_election()  # 随机选举超时时间
        self.heartbeat_timeout = 0.05
        self.heartbeat_success_received = asyncio.Event()
        self.heartbeat_received = asyncio.Event()
        self.is_new = is_new

    def set_peers(self, peers):
        self.peers = peers

    async def become_candidate(self):
        try:
            self.state = 'CANDIDATE'
            self.current_term += 1
            self.current_votes_received[self.id] = True
            self.voted_for = self.id
            print(f'{self.id}变为become_candidate，给任期{self.current_term}投票自己')
            self.leader_id = None
            last_log_index = self.log[-1]['index'] if len(self.log) > 0 else -1
            last_log_term = self.log[-1]['term'] if len(self.log) > 0 else -1
            # 创建任务列表
            for peer in self.peers:
                if peer.id != self.id:
                    task = asyncio.create_task(
                        self.send_vote_request_to_peer(peer.id, self.current_term, last_log_index, last_log_term)
                    )
                    task.add_done_callback(lambda t: asyncio.create_task(handle_task_result(t)))
        except Exception as e:
            print(f'become_candidate里的错误', e)

    async def send_vote_request_to_peer(self, follower_id, old_term, last_log_index, last_log_term):
        while self.state == 'CANDIDATE' and follower_id not in self.current_votes_received and old_term == self.current_term:
            print(f"{self.id}向{follower_id}发送投票请求")
            await self.put_message_to_host(
                follower_id,
                ('vote_request', self.current_term, self.id, last_log_index, last_log_term)
            )
            await asyncio.sleep(self.heartbeat_timeout)

    async def start_leader(self):
        print(f"start_leader, {self.id}==>在{self.current_term}期")
        self.leader_id = self.id
        self.state = 'LEADER'
        for node in self.peers:
            if node.id != self.id:
                self.next_index[node.id] = len(self.log)
                self.match_index[node.id] = -1
        task2 = asyncio.create_task(self.start_heartbeat())
        task2.set_name('start_heartbeat')

    async def start_heartbeat(self):
        while self.state == 'LEADER':
            # print('发送一次心跳')
            # 为所有节点创建并发任务
            for peer in self.peers:
                if self.id != peer.id:
                    asyncio.create_task(self.replicate_to_follower(peer.id))
            # 添加 Mediator 的消息任务
            asyncio.create_task(self.put_message_to_host('Mediator',
                                                         ('replicate_logs_request', self.current_term, self.leader_id,
                                                          self.commit_index)))
            await asyncio.sleep(self.heartbeat_timeout)

    async def replicate_logs(self, temp_log=None):
        print("replicate_logs*************")
        async with self.append_log_lock:
            # print(f'leader把更改日志追加到自己内存中，{temp_log}')
            if self.state == 'LEADER' and temp_log:
                if isinstance(temp_log, str):
                    temp_log = json.loads(temp_log)
                self.log.append(temp_log)
                # 更新 next_index
                for peer in self.peers:
                    self.next_index[peer.id] = len(self.log)
                    # print(f'更新{peer.id}的主机的next_index为{len(self.log)}')
            # 后续会随着心跳将日志发过去
            if len(self.log) > 0:
                for peer in self.peers:
                    if self.id != peer.id:
                        leader_next_index = self.next_index[peer.id]
                        # print(f'leader新增一个日志')
                        asyncio.create_task(self.replicate_to_follower(peer.id, leader_next_index))

    async def send_append_entries(self, leader_term, leader_id, leader_prev_log_index, leader_prev_log_term, entries,
                                  leader_commit_index):
        try:
            # print(f'leader_prev_log_index：{leader_prev_log_index},entries:{entries}')
            async with self.append_entries_lock:
                response = {
                    'term': self.current_term,
                    'success': False,
                    'conflict_index': None,
                    "follower_id": self.id,
                }
                # print(f'{self.id[1]}接收到了{leader_id[1]}发送的心跳信号，同步日志')
                if leader_term < self.current_term:
                    # print(f'leader_term:{leader_term},self.current_term:{self.current_term}')
                    pass
                else:
                    # print(f'{self.id}self.heartbeat_received.set()')
                    self.heartbeat_received.set()
                    self.heartbeat_received.clear()
                    self.current_term = leader_term
                    # print(f'更改leader为{leader_id}')
                    self.leader_id = leader_id
                    self.state = 'FOLLOWER'
                    self.voted_for = None
                    last_log_index = len(self.log) - 1
                    if last_log_index >= leader_prev_log_index >= 0 and self.log[leader_prev_log_index][
                        'term'] != leader_prev_log_term:
                        # 如果同样index的项，term不一致也会产生冲突
                        response['conflict_index'] = leader_prev_log_index
                    elif leader_prev_log_index > last_log_index:
                        response['conflict_index'] = max(0, last_log_index + 1)
                        # print(f"conflict_index:{response['conflict_index']}")
                    # 其他情况：如果leader_prev_log_index为-1，直接复制entries
                    # 找到最大共同index了，开始同步entries
                    elif len(entries) > 0:
                        if leader_prev_log_index + 1 < len(self.log):
                            del self.log[leader_prev_log_index + 1:]
                        self.log.extend(entries)
                        # print(f'entries增加了:{entries}')
                        response['success'] = True
                    if self.commit_index < leader_commit_index:
                        self.commit_index = leader_commit_index
                    asyncio.create_task(self.applied_by_commitIndex(self.commit_index))
        except Exception as e:
            # 其他异常也可以处理
            print(f"send_append_entries函数里的错误: {e}")
        asyncio.create_task(self.put_message_to_host(leader_id, ('replicate_logs_response', response)))
        # print(f"其他主机applied_by_commitIndex: {self.commit_index}")

    async def save_log_file(self, entry, commit_index):
        entry.pop('queues_dict', None)
        # 创建日志条目
        real_entry = create_log_file(entry)
        # 异步创建日志目录
        os.makedirs(os.path.dirname(f'{self.id}/logs'), exist_ok=True)
        # 异步打开日志文件并写入内容
        log_file_path = f'{self.id}/logs'
        async with aiofiles.open(log_file_path, 'a') as f:
            await f.write(real_entry + '\n')
        # 异步保存配置文件
        state_file_path = f'{self.id}/state'
        await save_config({"commit_index": commit_index, "applied_index": self.applied_index},
                          state_file_path)

    async def applied_by_commitIndex(self, commit_index, max_retries=5):
        retries = 0
        while self.applied_index < commit_index:
            try:
                with self.apply_lock:  # 确保操作互斥
                    # 增量写入日志
                    self.applied_index += 1
                    entry = self.log[self.applied_index]
                    # 如果文件修改成功，再保存日志
                    if entry['type'] in ['add', 'update', 'delete']:
                        await self.save_log_file(entry, commit_index)
                    # 如果两个操作都成功，提交并结束
                    retries = 0
            except IOError as e:
                # 如果出错，回滚索引并重试
                with self.apply_lock:  # 确保操作互斥
                    self.applied_index -= 1  # 回滚索引
                    retries += 1
                    if retries >= max_retries:
                        print(f"Failed to apply log after {max_retries} retries.")
                        break  # 达到最大重试次数，跳出
                # 可以加入延时或其他重试策略
            except Exception as e:
                # 其他异常也可以处理
                print(f"applied_by_commitIndex函数里: {e}")
                break

    async def replicate_to_follower(self, follower_id, leader_next_index=None):
        async with self.replicate_to_follower_lock:
            try:
                if leader_next_index is None:
                    leader_next_index = len(self.log)
                leader_prev_log_index = leader_next_index - 1
                leader_prev_log_term = self.log[leader_prev_log_index]['term'] if leader_prev_log_index >= 0 else -1
                entries = self.log[leader_next_index:] if (
                            len(self.log) > 0 and leader_next_index < len(self.log)) else []
                # print(f'entries:{entries},leader_prev_log_index:{leader_prev_log_index},log:{self.log},entries:{entries}')
                asyncio.create_task(self.put_message_to_host(follower_id, (
                    'replicate_logs_request', self.current_term, self.leader_id, leader_prev_log_index,
                    leader_prev_log_term,
                    entries, self.commit_index)))
            except Exception as e:
                print(f'replicate_to_follower里的错误: {e}')

    async def resolve_follower_replicate_response(self, response):
        async with self.resolve_follower_replicate_response_lock:
            follower_id = response['follower_id']
            if response['success']:
                # print('领导接收到同步的响应,执行check_commit')
                self.update_match_and_next_index(follower_id)
                await self.check_commit()
            elif response['term'] > self.current_term:
                # follower比leader的任期大，说明该leader已经过时了
                self.current_term = response['term']
                self.state = 'FOLLOWER'
            elif 'conflict_index' in response and response.get('conflict_index') != None:
                self.next_index[follower_id] = response['conflict_index']
                # 改变了next_index，再次尝试同步
                print(f'conflict_index为{response["conflict_index"]}=>前移next_index')
                asyncio.create_task(self.replicate_to_follower(follower_id, response['conflict_index']))

    async def check_commit(self):
        n = len(self.peers)  # 集群中节点的总数
        for i in range(len(self.log) - 1, self.commit_index, -1):  # 从最新的日志条目开始检查
            count = sum(1 for match in self.match_index.values() if match >= i)  # 计算大多数节点是否已经同步了该条目
            if count > n // 2 and i > self.commit_index:  # 确保新的 commit_index 大于原来的 commit_index
                self.commit_index = i  # 更新 commit_index
                print('更新leader的commit_index为', self.commit_index)
                asyncio.create_task(self.applied_by_commitIndex(self.commit_index))
                break  # 一旦找到符合条件的日志条目，更新并退出

    def update_match_and_next_index(self, follower_id):
        leader_next_index = self.next_index[follower_id]
        leader_prev_log_index = leader_next_index - 1
        entries = self.log[leader_next_index:]
        self.match_index[follower_id] = leader_prev_log_index + len(entries)
        self.next_index[follower_id] = self.match_index[follower_id] + 1

    async def send_vote(self, term, candidate_id, last_log_index, last_log_term):
        # print(f'{self.id}收到了{candidate_id}的投票邀请')
        async with self.send_vote_lock:
            vote_granted = False
            if term < self.current_term:
                vote_granted = False
            if term > self.current_term:
                self.state = 'FOLLOWER'
                # 将候选人的term更新为自己的current_term
                self.current_term = term
                self.voted_for = None
                self.leader_id = None
            # 如果候选人的任期更大，更新自己的任期并投票
            if term >= self.current_term:
                # 只有在未投过票并且候选人的日志条目更长或相等时，才投票
                if (not self.voted_for) and self.is_candidate_log_valid(last_log_index, last_log_term):
                    # 收到了任期更大的投票请求
                    if self.state == 'LEADER':
                        self.state = 'FOLLOWER'
                    # 将候选人的term更新为自己的current_term
                    self.current_term = term
                    self.voted_for = candidate_id
                    vote_granted = True
                else:
                    vote_granted = False
            print(f'投票者:{self.id}给{candidate_id}在{term}期,是否投票:{vote_granted}')
            await self.put_message_to_host(candidate_id,
                                           ('vote_request_response', self.id, self.current_term, vote_granted))

    def is_candidate_log_valid(self, last_log_index, last_log_term):
        """
        检查候选人的日志是否合法（即候选人的日志是否比本Follower的日志更新）
        :param last_log_index: 候选人最后一个日志条目的索引
        :param last_log_term: 候选人最后一个日志条目的任期
        :return: True - 合法，False - 不合法
        """
        if len(self.log) == 0:
            return True
        elif last_log_term > self.log[-1]['term']:  # 如果候选人的日志条目在任期上更新
            return True
        elif last_log_term == self.log[-1]['term'] and last_log_index >= self.log[-1]['index']:
            # 如果候选人的日志与Follower的日志条目同任期，但日志条目更长
            return True
        return False

    async def handle_vote_request_response(self, follower_id, follower_term, flag):
        if self.state != 'CANDIDATE':
            return
        print(f'候选人收到了{follower_id}的投票响应:{flag}')
        if self.current_term >= follower_term:
            self.current_votes_received[follower_id] = flag
            votes_for_me = sum(flag for flag in self.current_votes_received.values())
            if votes_for_me >= len(self.peers) / 2:
                self.state = 'LEADER'
                self.current_votes_received = {}
                await self.start_leader()
            elif len(self.current_votes_received) == len(self.peers):
                self.current_votes_received = {}
                self.state = 'FOLLOWER'
        else:
            # print(f"{self.id} 接收到更新的任期{follower_term}，所以切换到Follower")
            self.current_term = follower_term
            self.state = 'FOLLOWER'
            self.current_votes_received = {}

    async def run(self, new_add=False):
        self.new_add = new_add
        # self.sibling_pids_dict=sibling_pids_dict
        # print('node 哈哈哈')
        loop = asyncio.get_event_loop()
        self.run_loop = loop
        task1 = asyncio.create_task(self.run_core())
        task1.set_name('run_core')
        task2 = asyncio.create_task(self.get_message())
        task2.set_name('get_message')
        await asyncio.gather(task1, task2)

    async def process_raft_msg(self, msg):
        msg_type = msg[0]
        # print(f'msg_type:{msg_type}')
        if msg_type == 'vote_request':
            term, candidate_id, last_log_index, last_log_term = msg[1], msg[2], msg[3], msg[4]
            await self.send_vote(term, candidate_id, last_log_index, last_log_term)
        elif msg_type == 'vote_request_response':
            follower_id, follower_term, flag = msg[1], msg[2], msg[3]
            await self.handle_vote_request_response(follower_id, follower_term, flag)
        elif msg_type == 'replicate_logs_request':
            _, leader_term, leader_id, leader_prev_log_index, leader_prev_log_term, entries, leader_commit_index = msg
            # print('11111111111')
            await self.send_append_entries(leader_term, leader_id, leader_prev_log_index, leader_prev_log_term, entries,
                                           leader_commit_index)
        elif msg_type == 'replicate_logs_response':
            await self.resolve_follower_replicate_response(msg[1])

    async def process_message(self, msg):
        msg_type = msg[0]
        if msg_type in ['vote_request', 'vote_request_response', 'replicate_logs_request', 'replicate_logs_response']:
            # raft算法相关的主机通信
            await self.process_raft_msg(msg)
            return


    def remove_peer_by_id(self, node_id):
        self.peers[:] = [peer for peer in self.peers if peer.id != node_id]

    async def resolve_node_msg(self, msg):
        msg_type = msg[0]
        if msg_type == 'leader_send_new_server':
            print('其他节点收到了leader_add_success')
            new_node = msg[1]
            self.peers.append(FakeNode(new_node))
        elif msg_type == 'leader_add_success':
            print('领导收到了leader_add_success')
            if self.new_add:
                self.new_add = False
        elif msg_type == 'dead':
            print('其他节点收到了dead_node')
            dead_node_id = msg[1]
            if self.id == dead_node_id:
                print('我死了')
                self.is_dead = True
            else:
                self.remove_peer_by_id(dead_node_id)

    async def get_message(self):
        while True and not self.is_dead:
            message = await self.server.get_host_message()
            await self.process_message(message)

    async def run_core(self):
        if self.state == 'FOLLOWER':
            await self.run_follower()

    async def run_follower(self):
        while True and not self.is_dead:
            try:
                await asyncio.wait_for(self.heartbeat_received.wait(), timeout=self.election_timeout)
            except asyncio.TimeoutError:
                if self.state == 'FOLLOWER' and not self.is_new:
                    await self.become_candidate()
            finally:
                self.election_timeout = self.get_random_election()

    def get_random_election(self):
        return random.uniform(150, 300) / 1000
        # return random.uniform(750, 1000) / 100

    async def put_message_to_host(self, address_id, message_tuple):
        try:
            await self.server.send_host_message(address_id, message_tuple)
        except Exception as e:
            print(f'message_tuple:{message_tuple}==>{e}')




class RaftNode(RaftCore):
    pending_requests = queue.Queue()
    process_client_request = False

    def __init__(self, node_id, server, is_new):
        super().__init__(node_id, server, is_new)
        self.modify_file_dict = {}
        self.receive_client_lock = asyncio.Lock()
        self.get_file_cache_lock = asyncio.Lock()
        self.requests_dict = {}
        self.cache = self.init_lfu_cache()
        self.applied_index_dict = {}
        self.tasks_dict = {}
        self.response_index = -1
        self.is_change_file = {}

    def init_lfu_cache(self):
        k = 40
        m = 60
        my_lfu_cache = lfu_cache(k)
        # 在六秒后缓存会过期
        my_lfu_cache.schedule_exit(m)
        return my_lfu_cache


    async def process_message(self, msg):
        msg_type = msg[0]
        await super().process_message(msg)
        if msg_type in ['leader_add_server', 'leader_remove_server']:
            # 服务器变更的消息
            await self.process_server_update_msg(msg)
        elif msg_type in ['mediator_send_msg_get', 'get_file_cache', 'send_file_cache', 'delete_cache',
                          'mediator_send_msg_modified']:
            await self.process_operate_file_msg(msg)
        elif msg_type in ['leader_add_success', 'leader_add_success', 'dead']:
            await self.resolve_node_msg(msg)
        elif msg_type in ['load_send_peers']:
            await self.resolve_load_msg(msg)

    async def modify_file_by_log(self, entry):
        method = entry.get('type')
        file_path = entry.get('file_path')
        if method == 'add':
            properties_dict = entry['properties_dict']
            await add_file(file_path, properties_dict, self.id)
        elif method == 'delete':
            await delete_file(file_path, self.id)
        elif method == 'update':
            properties_dict = json.loads(entry['properties_dict'])
            await update_file(file_path, properties_dict, self.id)
        try:
            if file_path in self.is_change_file and isinstance(self.is_change_file[file_path], list):
                self.is_change_file[file_path].pop(0)
                if len(self.is_change_file[file_path]) == 0:
                    del self.is_change_file[file_path]
        except Exception as e:
            print(f'modify_file_by_log里add的错误：{e}')

    async def receive_client_core(self, request_id):
        try:
            async with self.receive_client_lock:
                operate_str, client_id, timestamp, node_id = self.requests_dict.get(request_id)
                params = resolve_client(operate_str=operate_str)
                file_path = params['file_path']
                op = params['op']
                properties_dict = params['properties_dict']
                if op != 'get':
                    file_exist = os.path.exists(f'{self.id}/{file_path}')
                    if (op == 'add' and file_exist) or (op == 'delete' and not file_exist):
                        # asyncio.create_task(self.put_message_to_host('Mediator', ('server_send_response', request_id)))
                        asyncio.create_task(self.put_message_to_host(client_id,
                                                                     ('server_send_response', '错误的操作指令',
                                                                      operate_str, request_id)))
                        return
                    if file_path not in self.modify_file_dict:
                        self.modify_file_dict[file_path] = [{'timestamp': timestamp, 'get_arr': []}]
                    else:
                        self.modify_file_dict[file_path].append({'timestamp': timestamp, 'get_arr': []})
                    # if file_path not in self.is_change_file:
                    #     self.is_change_file[file_path] = [timestamp]
                    # else:
                    #     self.is_change_file[file_path].append(timestamp)
                    temp_log = self.genLog(op, client_id, request_id, file_path, timestamp, properties_dict)
                    print('准备同步新增日志了')
                    await self.replicate_logs(temp_log)
        except Exception as e:
            print(f'receive_client_core里的错:{e}')

    def genLog(self, method, client_id, request_id, file_path, timestamp, content):
        temp = {}
        if method == 'update' or method == 'add':
            temp_content = json.dumps(content)
            if method == 'add':
                temp = {
                    "properties_dict": temp_content,
                }
            else:
                temp = {
                    "properties_dict": temp_content,
                }
        return {
            "index": len(self.log),
            "term": self.current_term,
            "type": method,
            "timestamp": timestamp,
            "leader_id": self.id,
            "communicate": client_id,
            "request_id": request_id,
            "file_path": file_path,
            **temp,
        }

    async def get_disk_file(self, file_path):
        if not os.path.exists(f'{self.leader_id}/{file_path}'):
            return None
        async with aiofiles.open(f'{self.leader_id}/{file_path}', 'r') as source_file:
            file_content = await source_file.read()  # 异步读取源文件
            return file_content
        # while True:
        #     try:
        #         if file_path not in self.is_change_file or (self.is_change_file.get(file_path) and self.is_change_file[file_path][0] > timestamp):
        #             async with aiofiles.open(f'{self.id}/{file_path}', 'r') as source_file:
        #                 file_content = await source_file.read()  # 异步读取源文件
        #                 return file_content
        #         await asyncio.sleep(1)
        #     except FileNotFoundError:
        #         print(f"文件找不到")
        #         return None
        #     except Exception as e:
        #         print(f"读取{file_path}发生错误: {e}")
        #         return None

    async def process_operate_file_msg(self, msg):
        msg_type = msg[0]
        # 处理查询逻辑
        if msg_type == 'mediator_send_msg_get':
            print(f'服务器收到一条mediator_send_msg_get消息')
            _, request_id, operate_str, client_id, timestamp, node_id = msg
            self.requests_dict[request_id] = (operate_str, client_id, timestamp, node_id)
            params = resolve_client(operate_str)
            file_path = params.get('file_path')
            # 使用 setdefault() 保证对于每个 file_path 都有一个唯一的锁,
            lock = self.tasks_dict.setdefault(file_path, asyncio.Lock())
            # 等待获取锁，保证同一时间只有一个线程处理该 file_path
            async with lock:
                file_content = self.cache.visitFile(file_path)
                print(f'缓存中{file_path}的文件内容为：{file_content}==》{self.id} == {self.leader_id}')
                if file_content:
                    print(f'响应成功，走的缓存=》{operate_str}==>{file_content}')
                    asyncio.create_task(self.get_success(request_id, file_content))
                elif self.id == self.leader_id:
                    print(f'leader处理查询操作')
                    try:
                        if file_path in self.modify_file_dict:
                            # 找到最近的修改请求
                            latest_modify_entry = None

                            for entry in reversed(self.modify_file_dict[file_path]):
                                if entry['timestamp'] <= timestamp:
                                    latest_modify_entry = entry
                                    break

                            if latest_modify_entry:
                                latest_modify_entry['get_arr'].append(request_id)
                            # 将处理逻辑转到修改完成后执行
                            return
                        # 当前文件不处于编辑态，可以查询
                        file_content = await self.get_disk_file(file_path)
                        if not file_content:
                            print(f'leader给客户端发送文件不存在的响应,之前并无更改操作：{operate_str}')
                            asyncio.create_task(self.put_message_to_host(client_id,
                                                                         ('server_send_response', '文件不存在',
                                                                          operate_str, request_id)))
                            return
                        self.cache.insert_in_dict(file_path, file_content)
                        print(f'领导将文件内容插入缓存')
                    except Exception as e:
                        print(f'领导查询文件内容出错')
                    # print(f'文件查询次数:{file_content}')
                    asyncio.create_task(self.get_success(request_id, file_content))
                else:
                    asyncio.create_task(self.put_message_to_host(self.leader_id,
                                                                 ('get_file_cache', operate_str, client_id, file_path,
                                                                  request_id, timestamp, node_id)))

        elif msg_type == 'get_file_cache':
            try:
                async with self.get_file_cache_lock:
                    _, operate_str, client_id, file_path, request_id, timestamp, node_id = msg
                    # 给leader主机也储存相关信息，等修改完成后，也可以获取
                    self.requests_dict[request_id] = (operate_str, client_id, timestamp, node_id)
                    if file_path in self.modify_file_dict:
                        # 找到最近的修改请求
                        latest_modify_entry = None
                        for entry in reversed(self.modify_file_dict[file_path]):
                            if entry['timestamp'] <= timestamp:
                                latest_modify_entry = entry
                                break

                        if latest_modify_entry:
                            latest_modify_entry['get_arr'].append(request_id)
                        # 将处理逻辑转到修改完成后执行
                        return
                    # 当前文件不处于编辑态，可以查询
                    file_content = await self.get_disk_file(file_path)
                    asyncio.create_task(
                        self.put_message_to_host(node_id, ('send_file_cache', file_content, request_id)))
            except Exception as e:
                print(f'process_operate_file_msg里的错误：{e}')
        elif msg_type == 'send_file_cache':
            # 其他节点获得了leader查找的文件内容
            try:
                _, file_content, request_id = msg
                operate_str, client_id, timestamp, node_id = self.requests_dict.get(request_id)

                if not file_content:
                    print('其他主机说文件不存在！！！！')
                    asyncio.create_task(self.put_message_to_host(client_id,
                                                                 ('server_send_response', '文件不存在',
                                                                  operate_str, request_id)))
                    return
                params = resolve_client(operate_str)
                file_path = params.get('file_path')
                self.cache.insert_in_dict(file_path, file_content)
                # print(f'文件查询次数:{file_content}')
                asyncio.create_task(self.get_success(request_id, file_content))
            except Exception as e:
                print(f'send_file_cache里的错误：{e}')
        elif msg_type == 'delete_cache':
            file_path = msg[1]
            self.cache.deleteCache(file_path)
        elif msg_type == 'mediator_send_msg_modified':
            # 先同步日志
            print('领导接收到更改消息')
            try:
                _, request_id, operate_str, client_id, timestamp, leader_id = msg
                if leader_id != self.id:
                    await self.put_message_to_host('Mediator', ('server_send_response', request_id, False))
                    return
                self.requests_dict[request_id] = (operate_str, client_id, timestamp, self.leader_id)
                await self.receive_client_core(request_id)
            except Exception as e:
                print(f'mediator_send_msg_modified出错了,{e}')

    async def process_server_update_msg(self, msg):
        msg_type = msg[0]
        if msg_type == 'leader_add_server':
            print('111接收到leader_add_server消息')
            _, request_id, new_node_id = msg
            temp_log = {
                "index": len(self.log),
                "term": self.current_term,
                "type": 'add_server',
                "timestamp": time.time(),
                "leader_id": self.id,
                "node_id": new_node_id,
                "request_id": request_id
            }
            await self.replicate_logs(temp_log)
        elif msg_type == 'leader_remove_server':
            _, request_id, remove_node_id = msg
            temp_log = {
                "index": len(self.log),
                "term": self.current_term,
                "type": 'remove_server',
                "timestamp": time.time(),
                "leader_id": self.id,
                "node_id": remove_node_id,
                "request_id": request_id
            }
            await self.replicate_logs(temp_log)

    async def get_success(self, request_id, file_content=None):
        try:
            # print(f'111111111111self.requests_dict.get(request_id):{self.requests_dict}---{request_id}')
            operate_str, client_id, timestamp, node_id = self.requests_dict.get(request_id)
            params = resolve_client(operate_str)
            if file_content:
                properties = params.get('properties')
                content_dict = json.loads(file_content)
                result = {key: content_dict.get(key, None) for key in properties}
                print(f'给客户端发送成功<<查询>>消息,{operate_str}==>{result}')

                try:
                    asyncio.create_task(self.put_message_to_host(client_id, ('server_send_response', json.dumps(result),
                                                                             operate_str, request_id)))
                except Exception as e:
                    print(f'给客户端发送成功<<查询>>消息处理错误：e')
            else:
                print('给客户端发送成功<<修改>>消息')
                asyncio.create_task(self.put_message_to_host(client_id,
                                                             ('server_send_response', f'文件操作成功', operate_str,
                                                              request_id)))
            # print(f'删除了{request_id}')
            del self.requests_dict[request_id]
        except Exception as e:
            print(f'get_success中的错误：{e}')

    async def response_by_commit_index(self, current_commit_index):
        while self.response_index < current_commit_index:
            try:
                self.response_index = self.response_index + 1
                if self.response_index >= len(self.log):
                    break
                entry = self.log[self.response_index]
                request_id = entry['request_id']
                # await self.put_message_to_host('Mediator', ('server_send_response', request_id, True))
            except Exception as e:
                print('response_by_commit_index里的问题', e)
                self.response_index = self.response_index - 1

    async def applied_by_commitIndex(self, current_commit_index, max_retries=5):
        retries = 0
        while self.applied_index < current_commit_index:
            try:
                async with self.apply_lock:  # 确保操作互斥
                    # if self.id != self.leader_id:
                    # print(f'其他节点---{self.applied_index}与{current_commit_index}---{self.log}')
                    # 增量写入日志
                    self.applied_index += 1
                    # if self.id != self.leader_id:
                    # print(f'中部---是其他节点在apply, {self.applied_index} === {current_commit_index}==={len(self.log)}')
                    if self.applied_index >= len(self.log):
                        return
                    entry = self.log[self.applied_index]
                    type = entry['type']
                    if type in ['add', 'update', 'delete']:
                        print(f'主机：{self.id}正在修改文件')
                        await asyncio.gather(
                            self.modify_file_by_log(entry),  # 子类可能有不同的实现
                            super().save_log_file(entry, current_commit_index)  # 保存日志
                        )
                        if entry['file_path'] in self.modify_file_dict:
                            await self.execute_get_by_modify_done(entry['file_path'], entry['timestamp'])
                        print(f'主机：{self.id}已经把日志存入文件')
                        if self.leader_id == self.id:
                            request_id = entry['request_id']
                            asyncio.create_task(self.get_success(request_id))
                        continue
                    else:
                        # 集群增减
                        await super().save_log_file(entry, current_commit_index)
                        if self.leader_id == self.id:
                            self.update_server(type, entry)
                            # asyncio.create_task(
                            #     self.put_message_to_host(address[3], ('server_send_response', 'remove_server')))
                            asyncio.create_task(
                                self.put_message_to_host('Mediator', ('server_send_response', 'remove_server')))
                    # 如果两个操作都成功，提交并结束
                    retries = 0
            except IOError as e:
                # 如果出错，回滚索引并重试
                print('IOError出错了', e)
                async with self.apply_lock:  # 确保操作互斥
                    self.applied_index -= 1  # 回滚索引
                    retries += 1
                    if retries >= max_retries:
                        print(f"Failed to apply log after {max_retries} retries.")
                        break  # 达到最大重试次数，跳出
                # 可以加入延时或其他重试策略
            except Exception as e:
                print(f"子类中applied_by_commitIndex的错误: {e}")
                break

    async def execute_get_by_modify_done(self, file_path, current_timestamp):
        # 获取所有的修改请求
        matched_request = next(
            (entry for entry in self.modify_file_dict.get(file_path, []) if entry['timestamp'] == current_timestamp),
            None  # 如果没有匹配的请求，返回 None
        )
        if not matched_request:
            return
        get_arr = matched_request.get('get_arr')
        print(f'1111111111get_arr:{get_arr}')
        for get_request_id in get_arr:
            try:
                # 执行 get 请求逻辑
                operate_str, client_id, timestamp, node_id = self.requests_dict.get(get_request_id)
                file_content = await self.get_disk_file(file_path)
                if not file_content:
                    print('leader修改了文件后说文件不存在')
                    asyncio.create_task(self.put_message_to_host(client_id,
                                                                 ('server_send_response', '文件不存在',
                                                                  operate_str, get_request_id)))
                    return
                if node_id == self.leader_id:
                    self.cache.insert_in_dict(file_path, file_content)
                    await self.get_success(get_request_id, file_content)
                else:
                    asyncio.create_task(
                        self.put_message_to_host(node_id, ('send_file_cache', file_content, get_request_id)))
            except Exception as e:
                print(f"处理 get 请求 {get_request_id} 时出错: {e}")

        # 从 modify_file_dict 中移除已完成的请求
        self.modify_file_dict[file_path].remove(matched_request)

        # 如果对应文件的修改请求已为空，移除整个文件路径的键
        if not self.modify_file_dict[file_path]:
            del self.modify_file_dict[file_path]
            print(f"移除文件路径 {file_path} 的所有修改请求")

    def update_server(self, type, entry):
        if self.id != entry['leader_id']:
            return
        if type == 'add_server':
            node_id = entry['node_id']
            if self.id == node_id:
                return
            for peer in self.peers:
                if peer.id != self.leader_id:
                    print(f'给{peer.id}说添加兄弟')
                    asyncio.create_task(self.put_message_to_host(peer.id, ('leader_send_new_server', node_id)))
            self.peers.append(FakeNode(node_id))
            # 让机器正式工作
            print('让机器正式工作')
            asyncio.create_task(self.put_message_to_host(node_id, ('leader_add_success',)))
            print('给中间层说添加成功')
            asyncio.create_task(self.put_message_to_host('Mediator', ('server_send_response', 'add_server')))

        elif type == 'remove_server':
            dead_node_id = entry['node_id']
            leader_id = entry['leader_id']
            if self.id == leader_id:
                for peer in self.peers:
                    if peer.id != leader_id:
                        print(f'给{peer.id}说删除兄弟')
                        asyncio.create_task(self.put_message_to_host(peer.id, ('dead', dead_node_id)))
                self.peers = [peer for peer in self.peers if peer.id != dead_node_id]
                # asyncio.create_task(self.put_message_to_host('Mediator', ('server_send_response', 'remove_server')))

    async def resolve_load_msg(self, msg):
        msg_type=msg[0]
        if msg_type=='load_send_peers':
            peers=msg[1]
            self.set_peers(peers)