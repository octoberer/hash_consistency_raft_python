import json
import os
import queue
import subprocess
import time
import random

import aiofiles
import asyncio
from util.operate_file import add_file, delete_file, update_file
from util.resolve_client import resolve_client
from util import create_log_file, save_config
from util.lfu_cache import lfu_cache

class FakeNode():
    def __init__(self,node_id):
        if isinstance(node_id,str):
            self.id = str(node_id)
        else:
            self.id= 'server' + str(node_id)

class RaftCore:
    def __init__(self, node_id, queues_dict):
        self.sibling_pids_dict = {}
        self.append_log_lock = asyncio.Lock()
        self.replicate_to_follower_lock =  asyncio.Lock()
        self.append_entries_lock = asyncio.Lock()
        self.last_heart_time = 0
        self.apply_lock = asyncio.Lock()
        self.run_loop = None
        self.current_votes_received = []
        self.leader_id = None
        self.id = 'server' + str(node_id)
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
        self.queues_dict = queues_dict

    def become_candidate(self):
        print(f'{self.id}变为become_candidate')
        self.state = 'CANDIDATE'
        self.current_term += 1
        self.current_votes_received.append(True)
        self.voted_for = self.id
        self.leader_id = None
        last_log_index = self.log[-1]['index'] if len(self.log) > 0 else -1
        last_log_term = self.log[-1]['term'] if len(self.log) > 0 else -1
        for peer in self.peers:
            if peer.id != self.id:
                self.queues_dict[peer.id].put(
                    ('vote_request', self.current_term, self.id, last_log_index, last_log_term))

    async def start_leader(self):
        print("start_leader", self.id, "voted_for", self.voted_for)
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
            # print(self.id,'领导我发送心跳一次！！！！！！！！！！！！！！！！')
            for peer in self.peers:
                # print(f'peer:{peer}---leader发送心跳信号，同步日志')
                if self.id != peer.id:
                    self.replicate_to_follower(peer.id)
            self.queues_dict['Mediator'].put(('replicate_logs_request', self.current_term, self.leader_id,self.commit_index))
            # self.queues_dict['client_1'].put(
            #     ('server_send_response', self.current_term, self.leader_id, self.commit_index))
            await asyncio.sleep(self.heartbeat_timeout)

    def resolve_heartbeat_response(self, response):
        if response:
            term = response.get('term', self.current_term)
            # print(f'&&&&resolve_heartbeat_response:{term}--{self.current_term}')
            if term > self.current_term:
                # Follower比自己的任期还大，将自己身份转为FOLLOWER，不再发送给其他主机心跳，等待下一轮选举发生
                self.current_term = term
                self.state = 'FOLLOWER'
                return
            else:
                pass

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

    async def send_append_entries(self, leader_term, leader_id, leader_prev_log_index, leader_prev_log_term, entries,
                                  leader_commit_index):

        async with self.append_entries_lock:
            try:
                response = {
                    'term': self.current_term,
                    'success': False,
                    'conflict_index': None,
                    "follower_id": self.id,
                }

                if leader_term < self.current_term:
                    print(f'leader_term:{leader_term},self.current_term:{self.current_term}')
                    self.queues_dict[leader_id].put(('replicate_logs_response', response))
                    return
                # print(f'{self.id}self.heartbeat_received.set()')
                self.heartbeat_received.set()
                self.heartbeat_received.clear()
                self.current_term = leader_term
                # print(f'更改leader为{leader_id}')
                self.leader_id = leader_id
                self.state = 'FOLLOWER'
                self.voted_for = None
                last_log_index = len(self.log) - 1
                # print(f'3333333:{entries}')
                if last_log_index >= leader_prev_log_index >= 0 and self.log[leader_prev_log_index][
                    'term'] != leader_prev_log_term:
                    # 如果同样index的项，term不一致也会产生冲突
                    response['conflict_index'] = leader_prev_log_index
                    self.queues_dict[leader_id].put(('replicate_logs_response', response))
                    return
                elif leader_prev_log_index > last_log_index:
                    response['conflict_index'] = max(0, last_log_index + 1)
                    self.queues_dict[leader_id].put(('replicate_logs_response', response))
                    return
                # 其他情况：如果leader_prev_log_index为-1，直接复制entries
                # 找到最大共同index了，开始同步entries
                if len(entries) > 0:
                    if leader_prev_log_index + 1 < len(self.log):
                        del self.log[leader_prev_log_index + 1:]
                    self.log.extend(entries)
                response['success'] = True
                self.queues_dict[leader_id].put(('replicate_logs_response', response))

                if self.commit_index < leader_commit_index:
                    self.commit_index = leader_commit_index
            except Exception as e:
            # 其他异常也可以处理
                print(f"send_append_entries函数里的错误: {e}")

        await self.applied_by_commitIndex(self.commit_index)

    async def save_log_file(self, entry,commit_index):
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

    async def applied_by_commitIndex(self,commit_index, max_retries=5):
        retries = 0
        while self.applied_index < commit_index:
            try:
                with self.apply_lock:  # 确保操作互斥
                    # 增量写入日志
                    self.applied_index += 1
                    entry = self.log[self.applied_index]
                    # 如果文件修改成功，再保存日志
                    if entry['type'] in ['add','update','delete']:
                        await self.save_log_file(entry,commit_index)
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


    def replicate_to_follower(self, follower_id):
        try:
            # with self.replicate_to_follower_lock:
            # print(f'外部self.queues_dict,{self.queues_dict}==={follower_id}')
            if follower_id not in self.next_index or self.next_index.get(follower_id)==None:
                # print(f'len(self.log):{len(self.log)}')
                self.next_index[follower_id]=len(self.log)
            # print(f'{self.next_index}====={follower_id}')
            leader_next_index = self.next_index[follower_id]
            # print(leader_next_index,'leader_next_index')
            leader_prev_log_index = leader_next_index - 1
            leader_prev_log_term = self.log[leader_prev_log_index]['term'] if leader_prev_log_index >= 0 else -1
            entries = self.log[leader_next_index:] if (len(self.log) > 0 and leader_next_index < len(self.log)) else []
            # if follower_id=='server4':
            # print(f'replicate_to_follower里的self.queues_dict:{self.queues_dict}，follower_id：{follower_id}')
            self.queues_dict[follower_id].put(('replicate_logs_request', self.current_term, self.leader_id,
                                               leader_prev_log_index, leader_prev_log_term, entries, self.commit_index))
        except Exception as e:
            print(f'replicate_to_follower里的错误:{e}')

    async def resolve_follower_replicate_response(self, response):
        follower_id = response['follower_id']
        if response['success']:
            # print('领导接收到同步的响应')
            self.update_match_and_next_index(follower_id)
            await self.check_commit()
        elif response['term'] > self.current_term:
            # follower比leader的任期大，说明该leader已经过时了
            self.current_term = response['term']
            self.state = 'FOLLOWER'
        elif 'conflict_index' in response and response.get('conflict_index')!=None:
            self.next_index[follower_id] = response['conflict_index']
            # 改变了next_index，再次尝试同步
            self.replicate_to_follower(follower_id)

    async def check_commit(self):
        n = len(self.peers)  # 集群中节点的总数
        for i in range(len(self.log) - 1, self.commit_index, -1):  # 从最新的日志条目开始检查
            count = sum(1 for match in self.match_index.values() if match >= i)  # 计算大多数节点是否已经同步了该条目
            if count > n // 2 and i > self.commit_index:  # 确保新的 commit_index 大于原来的 commit_index
                self.commit_index = i  # 更新 commit_index
                print('更新leader的commit_index为', self.commit_index)
                await self.applied_by_commitIndex(self.commit_index)  # 提交并应用日志到状态机
                break  # 一旦找到符合条件的日志条目，更新并退出

    def update_match_and_next_index(self, follower_id):
        leader_next_index = self.next_index[follower_id]
        leader_prev_log_index = leader_next_index - 1
        entries = self.log[leader_next_index:]
        self.match_index[follower_id] = leader_prev_log_index + len(entries)
        self.next_index[follower_id] = self.match_index[follower_id] + 1

    def send_vote(self, term, candidate_id, last_log_index, last_log_term):
        vote_granted = False
        if term < self.current_term:
            vote_granted = False

        # 如果候选人的任期更大，更新自己的任期并投票
        elif term >= self.current_term:
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
        # print(f'投票结果：投票者:{self.id},投票者的任期:{self.current_term},是否投票:{vote_granted}')
        self.queues_dict[candidate_id].put(('vote_request_response', self.current_term, vote_granted))

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

    async def handle_vote_request_response(self, follower_term, flag):
        if self.state != 'CANDIDATE':
            return
        # print(f'正在执行{self.id}的投票响应:{self.current_term}___{follower_term}', )
        if self.current_term >= follower_term:
            self.current_votes_received.append(flag)
            if sum(self.current_votes_received) >= len(self.peers) / 2:
                self.state = 'LEADER'
                self.current_votes_received = []
                await self.start_leader()
            elif len(self.current_votes_received) == len(self.peers):
                self.current_votes_received = []
                self.state = 'FOLLOWER'
        else:
            # print(f"{self.id} 接收到更新的任期{follower_term}，所以切换到Follower")
            self.current_term = follower_term
            self.state = 'FOLLOWER'
            self.current_votes_received = []

    async def run(self,sibling_pids_dict,new_add=False):
        self.new_add=new_add
        self.sibling_pids_dict=sibling_pids_dict
        # print('node 哈哈哈')
        loop = asyncio.get_event_loop()
        self.run_loop = loop
        task1 = asyncio.create_task(self.run_core())
        task1.set_name('run_core')
        task2 = asyncio.create_task(self.get_message(loop))
        task2.set_name('get_message')
        await asyncio.gather(task1, task2)

    async def process_raft_msg(self, msg):
        msg_type = msg[0]
        # print(f'msg_type:{msg_type}')
        if msg_type == 'vote_request':
            term, candidate_id, last_log_index, last_log_term = msg[1], msg[2], msg[3], msg[4]
            self.send_vote(term, candidate_id, last_log_index, last_log_term)
        elif msg_type == 'vote_request_response':
            follower_term, flag = msg[1], msg[2]
            await self.handle_vote_request_response(follower_term, flag)
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
        elif msg_type in ['send_new_pid','send_all_pid']:
            self.resolve_pid_msg(msg)
    def resolve_pid_msg(self,msg):
        msg_type = msg[0]
        if msg_type=='send_new_pid':
            new_pid=msg[1]
            self.sibling_pids_dict[new_pid]=new_pid
            for peer in self.peers:
                if peer.id!=self.leader_id:
                    self.queues_dict[peer.id].put(('send_all_pid'),self.sibling_pids_dict)
        elif msg_type=='send_all_pid':
            new_sibling_pids_dict=msg[1]
            self.sibling_pids_dict = new_sibling_pids_dict
        elif msg_type=='leader_add_success':
            print('领导收到了leader_add_success')
            if self.new_add:
                # 给leader发送自己的pid
                pid = os.getpid()
                self.queues_dict[self.leader_id].put(('send_new_pid', pid))
                self.new_add=False

    def get_message_core(self, loop):
        while True:
            try:
                msg = self.queues_dict[self.id].get()
                asyncio.run_coroutine_threadsafe(self.process_message(msg), loop)
            except queue.Empty:
                pass  # 如果队列为空，则继续等待

    async def get_message(self, loop):
        coro = asyncio.to_thread(self.get_message_core, loop)
        task = asyncio.create_task(coro)
        await task

    async def run_core(self):
        if self.state == 'FOLLOWER':
            await self.run_follower()

    async def run_follower(self):
        while True:
            # print(f'{self.id}活着')
            try:
                await asyncio.wait_for(self.heartbeat_received.wait(), timeout=self.election_timeout)
                # print(f'{self.id}执行了心跳')
            except asyncio.TimeoutError:
                if self.state == 'FOLLOWER' and not self.new_add:
                    self.become_candidate()
            finally:
                self.election_timeout = self.get_random_election()

    def get_random_election(self):
        return random.uniform(150, 500) / 1000
class RaftNode(RaftCore):
    pending_requests = queue.Queue()
    process_client_request = False

    def __init__(self, node_id, queues_dict):
        super().__init__(node_id, queues_dict)
        self.get_file_cache_lock =asyncio.Lock()
        self.requests_dict = {}
        self.cache = self.init_lfu_cache()
        self.applied_index_dict = {}
        self.tasks_dict = {}
        self.response_index=-1
        self.is_change_file= {}
    def init_lfu_cache(self):
        k = 40
        m = 60
        my_lfu_cache = lfu_cache(k)
        # 在六秒后缓存会过期
        my_lfu_cache.schedule_exit(m)
        return my_lfu_cache

    def set_peers(self, peers):
        self.peers = peers
    async def process_message(self, msg):
        msg_type = msg[0]
        await super().process_message(msg)
        if msg_type in ['leader_add_server', 'leader_remove_server']:
            # 服务器变更的消息
            await self.process_server_update_msg(msg)
        elif msg_type in ['mediator_send_msg_get', 'get_file_cache', 'send_file_cache', 'delete_cache',
                          'mediator_send_msg_modified']:
            # 文件修改的信息
            # print(msg_type,'>>>>>>>>>')
            await self.process_operate_file_msg(msg)


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
                if file_path in self.is_change_file and isinstance(self.is_change_file[file_path],list):
                    self.is_change_file[file_path].pop(0)
                    if len(self.is_change_file[file_path])==0:
                        del self.is_change_file[file_path]
            except Exception as e:
                print(f'modify_file_by_log里add的错误：{e}')


    async def receive_client_core(self, request_id):
        try:
            operate_str, client_id,timestamp,node_id=self.requests_dict.get(request_id)
            params = resolve_client(operate_str=operate_str)
            file_path = params['file_path']
            op = params['op']
            properties_dict = params['properties_dict']
            if op != 'get':
                file_exist=os.path.exists(f'{self.id}/{file_path}')
                if (op=='add' and file_exist) or (op=='delete' and not file_exist):
                    self.queues_dict['Mediator'].put(('server_send_response',request_id))
                    self.queues_dict[client_id].put(('server_send_response', '错误的操作指令',operate_str,request_id))
                    return
                if file_path not in self.is_change_file:
                    self.is_change_file[file_path]=[timestamp]
                else:
                    self.is_change_file[file_path].append(timestamp)
                temp_log = self.genLog(op, client_id,request_id,file_path,timestamp, properties_dict)
                print('准备同步新增日志了')
                await self.replicate_logs(temp_log)
        except Exception as e:
            print(f'receive_client_core里的错:{e}')

    def genLog(self, method, client_id,request_id,file_path,timestamp, content):
        temp={}
        if method == 'update' or method == 'add':
            temp_content = json.dumps(content)
            if method == 'add':
                temp={
                    "properties_dict": temp_content,
                }
            else:
                temp={
                    "properties_dict": temp_content,
                }
        return {
            "index": len(self.log),
            "term": self.current_term,
            "type": method,
            "timestamp": timestamp,
            "leader_id": self.id,
            "client": client_id,
            "request_id":request_id,
            "file_path": file_path,
            **temp,
        }

    async def process_operate_file_msg(self, msg):
        msg_type = msg[0]
        # 处理查询逻辑
        if msg_type == 'mediator_send_msg_get':
            # 当我们拿到锁后，判断是否有缓存
            _, request_id, operate_str, client_id,timestamp,node_id = msg
            self.requests_dict[request_id]=(operate_str, client_id,timestamp,node_id)
            # print(f'存储了self.requests_dict中的{request_id}为{operate_str, client_id, timestamp}')
            params = resolve_client(operate_str)
            file_path = params.get('file_path')
            # 使用 setdefault() 保证对于每个 file_path 都有一个唯一的锁
            lock = self.tasks_dict.setdefault(file_path, asyncio.Lock())
            # print('mediator_send_msg_get')
            # 等待获取锁，保证同一时间只有一个线程处理该 file_path
            async with lock:
                file_content = self.cache.visitFile(file_path)
                if file_content:
                    # print('缓存次数')
                    self.get_success(request_id, file_content)
                else:
                    self.queues_dict[self.leader_id].put(('get_file_cache', file_path, request_id,timestamp,node_id))
        elif msg_type == 'get_file_cache':
            # print(f'222222接收其他主机的查找请求')
            try:
                async with self.get_file_cache_lock:
                    _, file_path, request_id,timestamp,node_id = msg
                    while True:
                        if  file_path not in self.is_change_file or (self.is_change_file[file_path] and self.is_change_file[file_path][0]>timestamp):
                            # 已经是最新的
                            # print(f'处理请求{request_id}——{file_path}')
                            async with aiofiles.open(f'{self.id}/{file_path}', 'r') as source_file:
                                file_content = await source_file.read()  # 异步读取源文件
                                # print(f'333333333找到内容，发送给其他主机的查找请求：{file_content}')
                                self.queues_dict[node_id].put(('send_file_cache', file_content, request_id))
                                break
                        # print(f'{file_path}还没有完成修改动作==>{self.is_change_file.get(file_path)}')
                        await asyncio.sleep(0.1)
            except Exception as e:
                print(f'process_operate_file_msg里的错误：{e}')

        elif msg_type == 'send_file_cache':
            # print(f'444444444接收leader主机的文件内容')
            try:
                _, file_content, request_id = msg
                # print(f'self.requests_dict.get(request_id):{self.requests_dict.get(request_id)}==={request_id}')
                operate_str, client_id,timestamp,node_id=self.requests_dict.get(request_id)
                params = resolve_client(operate_str)
                file_path = params.get('file_path')
                self.cache.insert_in_dict(file_path, file_content)
                # print(f'文件查询次数:{file_content}')
                self.get_success(request_id, file_content)
            except Exception as e:
                print(f'send_file_cache里的错误：{e}')
        elif msg_type == 'delete_cache':
            file_path = msg[1]
            self.cache.deleteCache(file_path)
        elif msg_type == 'mediator_send_msg_modified':
            # 先同步日志
            print('领导接收到更改消息')
            try:
                _, request_id, operate_str, client_id, timestamp,leader_id = msg
                if leader_id!=self.id:
                    self.queues_dict['Mediator'].put(('server_send_response', request_id,False))
                    return
                self.requests_dict[request_id]=(operate_str, client_id,timestamp,self.leader_id)
                await self.receive_client_core(request_id)
            except Exception as e :
                print(f'mediator_send_msg_modified出错了,{e}')

    async def process_server_update_msg(self, msg):
        msg_type = msg[0]
        if msg_type == 'leader_add_server':
            print('111接收到leader_add_server消息')
            _, request_id, new_node_id,queues_dict = msg
            temp_log = {
                "index": len(self.log),
                "term": self.current_term,
                "type": 'add_server',
                "timestamp": time.time(),
                "leader_id": self.id,
                "node_id": new_node_id,
                "request_id":request_id,
                "queues_dict":queues_dict
            }
            await self.replicate_logs(temp_log)
        elif msg_type == 'leader_remove_server':
            _, request_id, remove_node_id, queues_dict = msg
            temp_log = {
                "index": len(self.log),
                "term": self.current_term,
                "type": 'remove_server',
                "timestamp": time.time(),
                "leader_id": self.id,
                "node_id": remove_node_id,
                "request_id": request_id,
                "queues_dict": queues_dict
            }
            await self.replicate_logs(temp_log)

    def get_success(self, request_id, file_content=None):
        try:
            # print(f'111111111111self.requests_dict.get(request_id):{self.requests_dict}---{request_id}')
            operate_str, client_id,timestamp,node_id=self.requests_dict.get(request_id)
            params = resolve_client(operate_str)
            if file_content:
                properties = params.get('properties')
                content_dict = json.loads(file_content)
                result = {key: content_dict.get(key, None) for key in properties}
                # self.queues_dict['Mediator'].put(('server_send_response',request_id))
                # print('总次数')
                self.queues_dict[client_id].put(('server_send_response', json.dumps(result),operate_str,request_id))
            else:
                self.queues_dict[client_id].put(('server_send_response','文件操作成功',operate_str,request_id))
            # print(f'删除了{request_id}')
            del self.requests_dict[request_id]
        except Exception as e:
            print(f'get_success中的错误：{e}')

    def response_by_commit_index(self,current_commit_index):
        while self.response_index < current_commit_index:
            try:
                self.response_index=self.response_index+1
                if self.response_index>=len(self.log):
                    break
                entry=self.log[self.response_index]
                request_id=entry['request_id']
                self.queues_dict['Mediator'].put(('server_send_response', request_id,True))
            except Exception as e:
                print('response_by_commit_index里的问题',e)
                self.response_index = self.response_index - 1

    async def applied_by_commitIndex(self, current_commit_index,max_retries=5):
        if self.id==self.leader_id:
            self.response_by_commit_index(current_commit_index)
        retries = 0
        while self.applied_index < current_commit_index:
            # try:
            async with self.apply_lock:  # 确保操作互斥
                # 增量写入日志
                self.applied_index += 1
                # if self.id != self.leader_id:
                    # print(f'中部---是其他节点在apply, {self.applied_index} === {current_commit_index}==={len(self.log)}')
                if self.applied_index>=len(self.log):
                    return
                entry = self.log[self.applied_index]
                type=entry['type']
                if type in ['add', 'update', 'delete']:
                    await asyncio.gather(
                        self.modify_file_by_log(entry),  # 子类可能有不同的实现
                        super().save_log_file(entry,current_commit_index)  # 保存日志
                    )
                    if self.leader_id==self.id:
                        request_id=entry['request_id']
                        self.get_success(request_id)
                    continue
                await asyncio.gather(
                    self.update_server(type, entry),
                    super().save_log_file(entry, current_commit_index)  # 保存日志
                )
                # 如果两个操作都成功，提交并结束
                retries = 0
            # except IOError as e:
            #     # 如果出错，回滚索引并重试
            #     print('IOError出错了',e)
            #     async with self.apply_lock:  # 确保操作互斥
            #         self.applied_index -= 1  # 回滚索引
            #         retries += 1
            #         if retries >= max_retries:
            #             print(f"Failed to apply log after {max_retries} retries.")
            #             break  # 达到最大重试次数，跳出
                # 可以加入延时或其他重试策略
            # except Exception as e:
            #     print(f"子类中applied_by_commitIndex的错误: {e}")
            #     break

    async def update_server(self,type,entry):
        if type == 'add_server':
            node_id = entry['node_id']
            if self.id==node_id:
                return
            self.queues_dict = entry["queues_dict"]
            self.peers.append(FakeNode(node_id))
            # 让机器正式工作
            self.queues_dict[node_id].put(('leader_add_success'))
            # if self.id == self.leader_id:
            #     print(f'tttttnode_id:{node_id}')
        elif type == 'remove_server':
            dead_node_id = entry['node_id']
            if self.id==dead_node_id:
                # 自杀
                subprocess.run(["taskkill", "/F", "/PID", str(os.getpid())])
            self.queues_dict = entry["queues_dict"]
            self.peers = [peer for peer in self.peers if peer.id != dead_node_id]
            if self.id == self.leader_id:
                # print(f'leader移除该节点的心跳传输:{self.sibling_pids_dict}')
                dead_pid = self.sibling_pids_dict.get(dead_node_id)
                if dead_pid:
                    print('leader杀死节点', self.id, dead_node_id)
                    subprocess.run(["taskkill", "/F", "/PID", str(dead_pid)])
                    del self.sibling_pids_dict[dead_node_id]
        # print(f'兄弟些: {self.peers}')
        # print(f'节点被领导移除成功:{dead_node_id}')



