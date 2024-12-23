import json
import multiprocessing
import os
import queue
import time
import random

import aiofiles
import psutil
import asyncio
from distributed.operate_file import add_file, get_file, delete_file, update_file, serve_num
from distributed.serves.resolve_client import resolve_client



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


def worker(peers, balancer, queues_dict, leader_id):
    p = psutil.Process()  # 当前进程
    p.nice(psutil.HIGH_PRIORITY_CLASS)
    max_node_id = get_max_id([node.id for node in peers])
    current_node_id = getServerName(max_node_id + 1)
    new_server = RaftNode(current_node_id)
    # 添加服务器节点
    balancer.add_server(new_server.id)
    queues_dict[leader_id].put(('add_server_to_leader', current_node_id))
    # ———————添加balancer和raft节点的关联—————————
    peers.append(new_server)
    new_server.set_peers(peers)
    new_server.set_balancer(balancer)
    new_server.set_queues_dict(queues_dict)
    asyncio.run(new_server.run())


def save_config(config, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w') as f:
        json.dump(config, f, indent=2)


class RaftNode:
    pending_requests = queue.Queue()
    process_client_request = False

    def __init__(self, node_id):

        self.done_list = []
        self.isCopy = False
        self.last_successful_response_time = {}
        self.pending_client = []
        self.run_loop = None
        self.heartbeat_loop = None
        self.replicate_logs_count = {}
        self.client = {}
        # self.ready_event = multiprocessing.Event()
        # self.done_event = multiprocessing.Event()
        self.balancer = None
        self.current_client = None
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
        self.applied_index_dict = {}
        self.election_timeout = random.uniform(150, 300) / 1000  # 随机选举超时时间
        self.heartbeat_timeout = 0.1
        self.followers_timeout_threshold = 0.2
        self.heartbeat_success_received = asyncio.Event()
        # self.heartbeat_received =False  # 用于控制超时的事件
        self.heartbeat_received = asyncio.Event()
        self.lock = asyncio.Lock()
        self.lock_follow_replicate = asyncio.Lock()
        # self.queue = message_queue
        self.queues_dict = {}
        self.get_message_interval = 0.1
        # 存储每条增加日志index对应的(file_content,client)
        self.msg_dict = {}
        self.msg_hash_dict = {}
        # 存储file路径
        self.files = []
        # {added_node_id:success_count}
        self.added_node_dict = {}
        self.is_migrating_files = []
        self.is_new=True

    def append_balancer_file(self, file_path):
        self.files.append(file_path)

    def set_peers(self, peers):
        self.peers = peers

    def set_balancer(self, balancer):
        self.balancer = balancer

    def set_queues_dict(self, queues_dict):
        self.queues_dict = queues_dict

    def become_candidate(self):
        print(f'{self.id}变为become_candidate，self.voted_for is None:{self.voted_for is None}')
        self.state = 'CANDIDATE'
        self.current_term += 1
        self.current_votes_received.append(True)
        self.voted_for = self.id
        self.leader_id = None
        last_log_index = self.log[-1]['index'] if len(self.log) > 0 else -1
        last_log_term = self.log[-1]['term'] if len(self.log) > 0 else -1
        for peer in self.peers:
            if peer.id != self.id:
                # print(f'CANDIDATE：{self.id}给{peer.id}发送vote_request信息')
                self.queues_dict[peer.id].put(
                    ('vote_request', self.current_term, self.id, last_log_index, last_log_term))

    async def start_leader(self):
        print("start_leader", self.id, "voted_for", self.voted_for)
        self.leader_id = self.id
        self.state = 'LEADER'
        # 重新按照self.peers初始化balancer
        for node in self.peers:
            self.balancer.add_server(node.id)
        for node in self.peers:
            if node.id != self.id:
                self.next_index[node.id] = len(self.log)
                self.match_index[node.id] = -1
        # task1 = asyncio.create_task(self.replicate_logs(None))
        # task1.set_name('replicate_logs')
        task2 = asyncio.create_task(self.start_heartbeat())

        task2.set_name('start_heartbeat')

        await task2

    async def start_heartbeat(self):
        while self.state == 'LEADER':
            # print(self.id,'领导我发送心跳一次！！！！！！！！！！！！！！！！')
            for peer in self.peers:
                # print(f'{self.id}---leader发送心跳信号，同步日志')
                if self.id != peer.id:
                    self.replicate_to_follower(peer.id)
                    # self.check_unresponsive_followers()
                    # leader_prev_log_index = self.next_index[peer.id]-1
                    # leader_prev_log_term = -2
                    # entries = []
                    # self.queues_dict[peer.id].put(('heartbeat', self.current_term, self.leader_id,
                    #                                leader_prev_log_index, leader_prev_log_term, entries,
                    #                                self.commit_index))
            await asyncio.sleep(self.heartbeat_timeout)
            # loop = asyncio.get_event_loop()  # 获取当前事件循环
            # self.heartbeat_loop=loop
            # print(f"start_heartbeat Current event loop: {loop}")
            # print('self.state',self.state)

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

    def replicate_logs(self, temp_log=None):
        # print("replicate_logs*************")
        if self.state == 'LEADER' and temp_log:
            print('leader把日志追加到自己内存中')
            if isinstance(temp_log, str):
                temp_log = json.loads(temp_log)
            self.log.append(temp_log)
            # 更新 next_index
            for peer in self.peers:
                self.next_index[peer.id] = len(self.log)
                print(f'更新{peer.id}的主机的next_index为{len(self.log)}')
        if len(self.log) > 0:
            for peer in self.peers:
                if self.id != peer.id:
                    self.replicate_to_follower(peer.id)

    async def send_append_entries(self, leader_term, leader_id, leader_prev_log_index, leader_prev_log_term, entries,
                            leader_commit_index):

        response = {
            'term': self.current_term,
            'success': False,
            'conflict_index': None,
            "follower_id": self.id,
        }

        if leader_term < self.current_term:
            self.queues_dict[leader_id].put(('replicate_logs_response', response))
            return

        # 保持心跳,重新进行选举
        self.heartbeat_received.set()

        self.current_term = leader_term
        # print(f'更改leader为{leader_id}')
        self.leader_id = leader_id
        self.state = 'FOLLOWER'
        self.voted_for = None
        last_log_index = len(self.log) - 1
        if last_log_index >= leader_prev_log_index >= 0 and self.log[leader_prev_log_index]['term'] != leader_prev_log_term:
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

        # 如果领导已经提交在自己前面，说明自己有失败的，就要从commit_index开始试图执行一遍，把失败的按顺序再次执行
        await self.execute_by_log(leader_commit_index)
        self.applied_by_commitIndex(leader_commit_index)

    async def execute_by_log(self, leader_commit_index):
        if self.is_new:
            self.commit_index=leader_commit_index
            self.applied_index=leader_commit_index
            self.is_new=False
        if len(self.log)<=self.commit_index+1:
            return
        for entry in self.log[self.commit_index+1:]:
            index = entry['index']
            if index in self.done_list:
                continue
            method = entry['type']

            file_path = entry['file_path']
            server_id_group = entry['server_id_group']

            res = None
            if method == 'add_server':
                new_node_id = entry['node_id']
                need_migrate_nodes = self.balancer.get_need_translate_nodes(new_node_id)
                if new_node_id==self.id:
                    # 我是新加的服务器节点
                    self.get_last_copy_files()
                # 如果该节点需要迁移
                if self.id in need_migrate_nodes:
                    # 去掉最后一个备份
                    self.delete_last_copy_files(self.id)
                self.peers.append({id: new_node_id})
                self.balancer.add_server(new_node_id)
            elif method =='remove_server':
                dead_node_id = entry['node_id']
                self.peers = [peer for peer in self.peers if peer["node_id"] != dead_node_id]
                if self.balancer.get_next_server(dead_node_id)==self.id:
                    # 将删除节点的所有内容交给后一位节点
                    await self.translate_files_from_directory_async(dead_node_id,self.id,False)
            if self.id in server_id_group:
                if method == 'get':
                    if index>leader_commit_index:
                        properties = entry['properties']
                        res = get_file(file_path, properties, self.id)
                elif method == 'add':
                    properties_dict = entry['properties_dict']
                    add_file(file_path, properties_dict, self.id)
                elif method == 'delete':
                    delete_file(file_path, self.id)
                elif method == 'update':
                    properties_dict = json.loads(entry['properties_dict'])
                    update_file(file_path, properties_dict, self.id)
            if self.leader_id and index>leader_commit_index:
                self.queues_dict[self.leader_id].put(('file_operate_success', entry['index'], self.id, res))
                self.done_list.append(index)
            self.log[index]['commit'] = True
            self.commit_index = self.commit_index + find_max_commit_index(self.log[self.commit_index + 1:len(self.log)]) + 1
            save_config({"done_list": self.done_list, "commit_index": self.commit_index,
                         "applied_index": self.applied_index}, f'{self.id}/state')

    def applied_by_commitIndex(self, leader_commit,max_retries=5):
        temp_commit_index=min(self.commit_index,leader_commit)
        while self.applied_index < temp_commit_index:
            try:
                # 增量写入日志
                self.applied_index += 1
                entry = self.log[self.applied_index]
                real_entry=create_log_entry(entry)
                os.makedirs(os.path.dirname(f'{self.id}/logs'), exist_ok=True)
                with open(f'{self.id}/logs', 'a') as f:
                    f.write(json.dumps(real_entry) + '\n')
                save_config({"done_list": self.done_list, "commit_index": self.commit_index,
                             "applied_index": self.applied_index}, f'{self.id}/state')
            except IOError as e:
                # 重试
                self.applied_index -= 1  # 回滚索引以重试当前条目
                retries = 0
                if retries >= max_retries:
                    break


    def replicate_to_follower(self, follower_id):
        leader_next_index = self.next_index[follower_id]
        leader_prev_log_index = leader_next_index - 1
        leader_prev_log_term = self.log[leader_prev_log_index]['term'] if leader_prev_log_index >= 0 else -1
        entries = self.log[leader_next_index:] if len(self.log) > 0 and leader_next_index<len(self.log) else []
        # print(f'向{follower_id}发送同步请求')
        self.queues_dict[follower_id].put(('replicate_logs_request', self.current_term, self.leader_id,
                                           leader_prev_log_index, leader_prev_log_term, entries, self.commit_index))

    def resolve_follower_replicate_response(self, response):
        follower_id = response['follower_id']
        if response['success']:
            self.last_successful_response_time[follower_id] = time.time()
            self.update_match_and_next_index(follower_id)
            # self.check_commit()
        elif response['term'] > self.current_term:
            # follower比leader的任期大，说明该leader已经过时了
            self.current_term = response['term']
            self.state = 'FOLLOWER'
        elif 'conflict_index' in response:
            self.next_index[follower_id] = response['conflict_index']
            # 改变了next_index，再次尝试同步
            self.replicate_to_follower(follower_id)

    def check_unresponsive_followers(self):
        current_time = time.time()
        for follower_id, last_time in list(self.last_successful_response_time.items()):
            elapsed_time_s = current_time - last_time
            if elapsed_time_s > self.followers_timeout_threshold:
                print(f"Follower {follower_id} 已经{elapsed_time_s}没有响应了. 将它视为失败")
                self.resolve_follower_failed(follower_id)

    def resolve_follower_failed(self, follower_id):
        del self.last_successful_response_time[follower_id]
        for node in self.peers:
            self.queues_dict[node.id].put(('node_dead', follower_id))

    def check_commit(self):
        n = len(self.peers)  # 集群中节点的总数
        for i in range(len(self.log) - 1, self.commit_index, -1):  # 从最新的日志条目开始检查
            count = sum(1 for match in self.match_index.values() if match >= i)  # 计算大多数节点是否已经同步了该条目
            if count > n // 2 and i > self.commit_index:  # 确保新的 commit_index 大于原来的 commit_index
                self.commit_index = i  # 更新 commit_index
                print('更新leader的commit_index为', self.commit_index)
                self.applied_by_commitIndex(self.commit_index)  # 提交并应用日志到状态机
                break  # 一旦找到符合条件的日志条目，更新并退出

    def update_match_and_next_index(self, follower_id):
        leader_next_index = self.next_index[follower_id]
        leader_prev_log_index = leader_next_index - 1
        entries = self.log[leader_next_index:]
        self.match_index[follower_id] = leader_prev_log_index + len(entries)
        self.next_index[follower_id] = self.match_index[follower_id] + 1

    def replicate_logs_ok(self):
        if self.replicate_logs_count[self.current_term] + 1 > len(self.peers) / 2:
            # leader收到一半以上的响应了就返回给客户说
            self.current_client.receive_msg('操作成功')
            self.commit_index = len(self.log)
            self.replicate_logs_count[self.current_term] = None
        else:
            self.replicate_logs_count[self.current_term] = self.replicate_logs_count[self.current_term] + 1

    def send_vote(self, term, candidate_id, last_log_index, last_log_term):
        # 模拟网络延迟
        # delay = random.uniform(0.1, 0.5)/100
        # await asyncio.sleep(delay)
        vote_granted = False
        # 如果候选人的任期较小，拒绝投票
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
            print(f"{self.id} 接收到更新的任期{follower_term}，所以切换到Follower")
            self.current_term = follower_term
            self.state = 'FOLLOWER'
            self.current_votes_received = []

    async def process_message(self, msg):
        # print('真正开始处理消息了')
        msg_type = msg[0]  # 提取消息类型
        if msg_type == 'vote_request':
            term, candidate_id, last_log_index, last_log_term = msg[1], msg[2], msg[3], msg[4]
            # print(f'{self.id}接收到投票申请')
            self.send_vote(term, candidate_id, last_log_index, last_log_term)
        elif msg_type == 'vote_request_response':
            follower_term, flag = msg[1], msg[2]
            await self.handle_vote_request_response(follower_term, flag)
        elif msg_type == 'replicate_logs_request':
            _, leader_term, leader_id, leader_prev_log_index, leader_prev_log_term, entries, leader_commit_index = msg
            await self.send_append_entries(leader_term, leader_id, leader_prev_log_index, leader_prev_log_term, entries,
                                     leader_commit_index)
        elif msg_type == 'replicate_logs_response':
            self.resolve_follower_replicate_response(msg[1])
        # elif msg_type == 'heartbeat_response':
        #     self.resolve_heartbeat_response(msg[1])

        # 与客户端相关
        elif msg_type == 'client_send':
            # 有客户端发给leader节点=
            _, client, message = msg
            self.receive_client_core(client, message)
        elif msg_type == 'to_leader_for_replicate':
            raw_message, client = msg[1]
            params = resolve_client(operate_str=raw_message)
            file_path = params['file_path']
            server_id = self.balancer.get_server(file_path)
            properties_dict = params['properties_dict']
            op = params['op']
            temp_log = self.genLog(op, file_path, properties_dict, client.client_id, server_id)
            self.msg_dict[len(self.log) + 1] = (properties_dict, client)
            if op == 'add':
                # properties_dict为文件新增内容
                self.replicate_logs(temp_log, server_id)
            else:
                self.replicate_logs(temp_log)
        elif msg_type == 'applied_success':
            log_index = msg[1]
            self.handle_client_command(log_index)
        elif msg_type == 'replicate_logs_ok':
            self.replicate_logs_ok()
        elif msg_type == 'remove_server':
            node_id = msg[1]
            self.balancer.remove_server(node_id)
            if node_id == self.leader_id:
                self.leader_id = None
            temp = []
            for node in self.peers:
                if node_id != node.id:
                    temp.append(node)
            self.peers = temp
        elif msg_type == 'add_server_to_leader':
            try:
                current_node_id = msg[1]
                for node in self.peers:
                    if self.id != node.id:
                        self.queues_dict[node.id].put(('add_server', current_node_id))
                await self.migrate_to_new_server(current_node_id)
            except Exception as e:
                print(f'add_server_to_leader的e: {e}')
        elif msg_type == 'add_server':
            try:
                current_node_id = msg[1]
                await self.migrate_to_new_server(current_node_id)
                # self.queues_dict[self.id].put(('add_server_response', current_node_id))
            except Exception as e:
                print(f'add_server的e: {e}')
        elif msg_type == 'files_to_migrate_success':
            added_node_id = msg[1]
            if added_node_id in self.added_node_dict:
                self.added_node_dict[added_node_id] = 1
            else:
                self.added_node_dict[added_node_id] = self.added_node_dict[added_node_id] + 1
            if self.added_node_dict[added_node_id] < len(self.peers) / 2:
                del self.added_node_dict[added_node_id]
                for node in self.peers:
                    self.queues_dict[node.id].put(('files_to_migrate_success_follower', added_node_id))
                self.resolve_files_to_migrate_success(added_node_id)
        elif msg_type == 'files_to_migrate_success_follower':
            added_node_id = msg[1]
            self.resolve_files_to_migrate_success(added_node_id)
        elif msg_type == 'files_to_migrate':
            # 这个消息只会新主机接收执行
            file_dict = msg[1]
            for file_path, file_content in file_dict.items:
                new_file_full_path = os.path.join(self.id, file_path)
                os.makedirs(os.path.dirname(new_file_full_path), exist_ok=True)
                async with aiofiles.open(new_file_full_path, 'w') as f:
                    await f.write(file_content)  # 将文件内容写入新服务器
                    self.queues_dict[self.leader_id].put(('files_to_migrate_success', self.id))
        elif msg_type == 'node_dead':
            dead_id = msg[1]
            for node in self.peers:
                if node.id == dead_id:
                    self.peers.remove(node)
                    break
            self.balancer.remove_server(dead_id)
            await self.resolve_remove_node(dead_id)
        # 文件修改的信息
        self.process_operate_file_msg(msg)

    def resolve_files_to_migrate_success(self, added_node_id):
        self.balancer.add_server(added_node_id)
        self.peers.append({id: added_node_id})
        for one_pending_client in self.pending_client:
            pending_task = one_pending_client.get_pending_task()
            for one_pending_task in pending_task:
                self.receive_client_core(one_pending_client, one_pending_task)

    def handle_client_command(self, log_index):
        print(f'handle_client_command:{log_index}:{log_index in self.applied_index_dict}')
        try:
            if log_index not in self.applied_index_dict:
                self.applied_index_dict[log_index] = 1
            else:
                self.applied_index_dict[log_index] += 1
            print()
            if self.applied_index_dict[log_index] > len(self.peers) / 2:
                op_str, client = self.msg_dict[log_index]
                del self.applied_index_dict[log_index]
                client.receive_msg(f'成功了:{op_str}')
        except Exception as e:
            print('handle_client_command error', e)

    def get_message_core(self, loop):
        while True:
            # msg =await loop.run_in_executor(None, self.queues_dict[self.id].get)
            msg = self.queues_dict[self.id].get()
            # print(f'{self.id}接收到消息')
            # asyncio.run(self.process_message(msg))
            # await self.process_message(msg)
            # await asyncio.sleep(self.get_message_interval)
            asyncio.run_coroutine_threadsafe(self.process_message(msg), loop)

    async def get_message(self, loop):
        # print('get_message')
        # """启动一个额外线程，异步获取消息，避免阻塞其他协程"""
        coro = asyncio.to_thread(self.get_message_core, loop)
        task = asyncio.create_task(coro)
        # loop = asyncio.get_event_loop()  # 获取当前事件循环
        # print(f"get_message Current event loop: {loop}")
        # print(f"Are task1 and heartbeat_loop in the same loop? {loop is self.heartbeat_loop}")
        # print(f"Are task1 and run_loop in the same loop? {loop is self.run_loop}")
        await task
        # await self.get_message_core(loop)

    async def kill_leader(self):
        await asyncio.sleep(random.uniform(7, 15))
        if self.leader_id == self.id:
            print('杀了领导进程？？？？？？？')
            raise Exception("Simulating process crash")  # 模拟崩溃

    async def add_server(self):
        await asyncio.sleep(20)
        if self.id == self.leader_id:
            new_process = multiprocessing.Process(target=worker,
                                                  args=(self.peers, self.balancer, self.queues_dict, self.leader_id))
            new_process.start()
            new_process.join()

    async def run(self):
        loop = asyncio.get_event_loop()
        self.run_loop = loop
        # # 启动线程运行 get_message_core
        # thread = threading.Thread(target=self.get_message_core, args=(loop,))
        # thread.start()
        # print('start')
        # loop = asyncio.get_event_loop()
        task1 = asyncio.create_task(self.run_core())
        task1.set_name('run_core')
        task2 = asyncio.create_task(self.get_message(loop))
        task2.set_name('get_message')
        task_add_server = asyncio.create_task(self.add_server())
        task_add_server.set_name('add_server')
        # task3 = asyncio.create_task(monitor_tasks())
        # task3.set_name('monitor_tasks')

        task_kill_leader = asyncio.create_task(self.kill_leader())
        try:
            task_kill_leader.set_name('kill_leader')
            await asyncio.gather(task1, task2, task_add_server, task_kill_leader)
        except Exception as e:
            print(f"An error occurred: {e}")
            task2.cancel()
            task1.cancel()
            task_add_server.cancel()
        # if to_node_id:
        #     task3=asyncio.create_task(self.migrate_to_new_server(to_node_id))
        #     await asyncio.gather(task1, task2, task3)
        # else:
        await asyncio.gather(task1, task2, task_add_server)
        # 迁移自己的文件到新服务器节点上

    async def migrate_to_new_server(self, files_to_migrate,current_node_id):
        self.is_migrating_files = files_to_migrate
        file_dict = {}
        for file_path in files_to_migrate:
            # 补充按这些路径读取文件，并进行以文件名：文件进行存储发
            file_full_path = os.path.join(self.id, file_path)  # 拼接成文件的完整路径
            if os.path.exists(file_full_path):
                async with aiofiles.open(file_full_path, 'r') as f:
                    file_data = await f.read()  # 读取文件内容
                    file_dict[file_path] = file_data
            else:
                print(f"文件 {file_path} 在当前服务器 {self.id} 上不存在，跳过迁移")
        self.queues_dict[current_node_id].put(('files_to_migrate', file_dict))

    async def run_core(self):
        if self.state == 'FOLLOWER':
            await self.run_follower()

    async def run_follower(self):
        while True:
            try:
                await asyncio.wait_for(self.heartbeat_received.wait(), timeout=self.election_timeout)
                self.election_timeout = random.uniform(150, 300) / 1000

            except asyncio.TimeoutError:
                if self.state == 'FOLLOWER':
                    self.become_candidate()
            finally:
                self.heartbeat_received.clear()

    def receive_client(self, client, msg, message_queue):
        pass

    def receive_client_core(self, client, message):
        params = resolve_client(operate_str=message)
        file_path = params['file_path']
        op = params['op']
        properties_dict = params['properties_dict']
        properties = params['properties']

        if op == 'get':
            temp_log = self.genLog(op, file_path, properties, client.client_id)
        else:
            temp_log = self.genLog(op, file_path, properties_dict, client.client_id)
        self.replicate_logs(temp_log)

    def genLog(self, method, file_path, content, client):
        if method == 'update' or method == 'add':
            temp_content = json.dumps(content)
            if method == 'add':
                return {
                    "index": len(self.log),
                    "term": self.current_term,
                    "type": method,
                    "file_path": file_path,
                    "properties_dict": temp_content,
                    "timestamp": time.time(),
                    "leader_id": self.id,
                    "client": client,
                    "server_id_group":self.balancer.get_server(file_path)
                }
            else:
                return {
                    "index": len(self.log),
                    "term": self.current_term,
                    "type": method,
                    "file_path": file_path,
                    "properties_dict": temp_content,
                    "timestamp": time.time(),
                    "leader_id": self.id,
                    "client": client,
                    "server_id_group": self.balancer.get_server(file_path)
                }
        elif method == 'get':
            return {
                "index": len(self.log),
                "term": self.current_term,
                "type": method,
                "file_path": file_path,
                "properties": content,
                "timestamp": time.time(),
                "leader_id": self.leader_id,
                "client": client,
                "server_id_group": self.balancer.get_server(file_path)
            }
        elif method == 'delete':
            return {
                "index": len(self.log),
                "term": self.current_term,
                "type": method,
                "file_path": file_path,
                "timestamp": time.time(),
                "leader_id": self.leader_id,
                "client": client,
                "server_id_group": self.balancer.get_server(file_path)
            }

    async def resolve_remove_node(self, dead_id):
        # 首先根据dead_node_id A获取备份主机B的id（我的备份策略是备份到一致性hash环的后一位）
        # 然后该备份主机B的将所有文件内容copy到它后一位c，以此来实现节点死掉后的应对策略
        # 备份完成后取出在备份过程之间的有关B更新的操作，进行增量更新，直到更新到applied_index和B的一致
        # 当然这个过程中，应该阻止与B更新操作同步发生的应用操作，而是让C备份旧的文件完成后，再执行，那么就需要定义正在备份的文件，让其备份滞后

        old_backup_node_id = self.balancer.get_next_server(dead_id)  # 获取备份节点B
        if old_backup_node_id is None:
            raise Exception(f"无法找到旧的备份节点，节点 {dead_id} 的备份失败。")
        if old_backup_node_id == self.id:
            # 如果自己是这个死掉主机的备份主机，那么就要接收这个死掉主机的源文件和备份它上一位的备份文件，这个死掉主机的源文件本来就备份在本主机上，不需要做任何操作
            # 主要是将死掉主机的备份功能迁移到本机上，也就是本主机要备份死掉主机上一位的源文件
            be_copied_server = self.balancer.get_last_server(dead_id)
            start_time = time.time()
            self.receivingBackup = True
            await self.translate_files_from_directory_async(be_copied_server, self.id, True)
            # 将这段时间发生的更改在备注文件上也实现对其
            # await self.batch_update(start_time, be_copied_server)

        new_backup_node_id = self.balancer.get_next_server(old_backup_node_id)
        if new_backup_node_id is None:
            raise Exception(f"无法找到旧的备份节点 {new_backup_node_id} 的后一位节点。")
        if new_backup_node_id == self.id and old_backup_node_id:
            # 如果自己是这个新替代主机的备份主机，那么就要把属于新替代主机的源文件迁移到自己这里来
            start_time = time.time()
            await self.translate_files_from_directory_async(old_backup_node_id, self.id, True)
            # 将这段时间发生的更改在备注文件上也实现对其
            await self.batch_update(start_time, old_backup_node_id)

    async def copy_file(self, source_path, target_path):
        async with aiofiles.open(source_path, 'rb') as source_file:
            content = await source_file.read()  # 异步读取源文件
            async with aiofiles.open(target_path, 'wb') as target_file:
                await target_file.write(content)  # 异步写入目标文件

    async def translate_files_from_directory_async(self, source_directory, target_directory, only_origin=False):
        """异步复制源目录中的所有文件到目标目录"""
        if not os.path.exists(target_directory):
            os.makedirs(target_directory)  # 创建目标目录

        tasks = []
        for filename in os.listdir(source_directory):
            # 如果只迁移源文件，那么就要判断该文件是否是存储在主机上，如果不是就不迁移
            if only_origin:
                if self.balancer.get_server(filename) != source_directory:
                    break
            source_path = os.path.join(source_directory, filename)
            target_path = os.path.join(target_directory, filename)

            if os.path.isfile(source_path):
                if os.path.exists(target_path):
                    # 如果本来就有这个文件，就不必要再复制了
                    continue
                # 异步复制每个文件
                tasks.append(self.copy_file(source_path, target_path))

        # 等待所有文件复制完成
        await asyncio.gather(*tasks)

    def process_operate_file_msg(self, msg):
        msg_type = msg[0]
        if msg_type == 'file_operate_success':
            _, log_index, serve_id, res = msg
            leader_entry = self.log[log_index]
            if isinstance(leader_entry, dict):
                server_id_group = leader_entry['server_id_group']
                method = leader_entry['type']
                client = leader_entry['client']
                file_path = leader_entry['file_path']
                if leader_entry['commit']:
                    return
                if method == 'get':
                    if serve_id in  server_id_group:
                        client.receive_msg(str(res))
                        leader_entry['commit'] = True
                else:
                    leader_entry['commit'].append(serve_id)
                    length = len(self.balancer.get_server(file_path))
                    if len(leader_entry['commit']) >= length / 2:
                        if method == 'add_server' or method == 'remove_server':
                            node_id = leader_entry['node_id']
                            if self.id==node_id:
                                # 如果被删除和添加的主机都已经给leader说，我做完了该做的事，那就说明这个日志可以写入文件了
                                if method == 'add_server':
                                    self.balancer.add_server(node_id)
                                elif method == 'remove_server':
                                    self.balancer.remove_server(node_id)
                                client.receive_msg(f'{method}:{node_id}成功了')
                        elif serve_id in server_id_group:
                            # 如果成功数量大于主机数的一半和有一个关联的主机已经完成修改操作即可
                            client.receive_msg(f'{method}{file_path}成功了')
                self.commit_index = self.commit_index + find_max_commit_index(
                    self.log[self.commit_index + 1:len(self.log)]) + 1
                self.applied_by_commitIndex(self.commit_index)

        elif msg_type == 'update_server':
            type,new_node_id = msg[1]
            temp_log={}
            if type=='add server':
                # 向各个节点发送add_server消息
                temp_log = {
                    "type": type,
                    "node_id": new_node_id,
                    "term": self.current_term,
                    "timestamp": time.time(),
                    "leader_id": self.leader_id,
                }
            elif type=='remove server':
                files_to_migrate_dict = {}
                # for node in self.peers:
                #     files_to_migrate_dict = {node.id: self.balancer.get_files_to_migrate(self.files, node.id)}
                # 向各个节点发送add_server消息
                temp_log = {
                    "type": type,
                    "node_id": new_node_id,
                    "term": self.current_term,
                    "timestamp": time.time(),
                    "leader_id": self.leader_id,
                }
            self.replicate_logs(temp_log)

    def delete_last_copy_files(self,node_id):
        file_paths=list_all_files(node_id)
        for file_path in file_paths:
            if self.balancer.is_last_copy(file_path,node_id):
                delete_file(file_path,node_id)
    def get_last_copy_files(self,node_id):
        file_paths=list_all_files(node_id)
        res_files=[]
        for file_path in file_paths:
            if self.balancer.is_last_copy(file_path,node_id):
                res_files.append(file_path)
        return res_files

def create_log_entry(self, entry):
    # 从 entry 中提取必要信息
    temp_content = entry.get('properties_dict', {})  # 获取 temp_content
    client = entry.get('client', None)  # 获取 client
    server_id_group = entry.get('server_id_group', None)
    # 检查是否有 client 对象，并获取 client_id
    if client is not None:
        client_id = client.client_id
    else:
        client_id = None  # 如果没有 client，client_id 为 None
    # 创建日志条目字典
    log_entry = {
        **entry,
        "properties_dict": json.dumps(temp_content),  # 转换为 JSON 字符串
        "client_id": client_id,  # 使用 client.client_id
        "server_id_group": json.dumps(server_id_group)  # 转换为 JSON 字符串
    }

    return log_entry

def find_max_commit_index(log):
    # 查找 commit 为 True 的最大索引
    max_index = -1  # 初始化为 -1，表示未找到 commit 为 True 的元素
    for index, entry in enumerate(log):
        if entry.get("commit") == True:
            max_index = index
        else:
            break
    return max_index


def list_all_files(directory):
    file_paths = []
    for root, _, files in os.walk(directory):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths