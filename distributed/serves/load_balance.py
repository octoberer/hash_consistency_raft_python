import hashlib
import bisect

class ConsistentHashLoadBalancer:
    def __init__(self,replicas_node_count=5,copy_num=3):
        self.replicas_node_count = replicas_node_count  # 虚拟节点的数量
        self.hash_server_dict = {}  # 真实服务器节点
        self.ring = []  # 哈希环，存储虚拟节点的哈希值
        self.files = {}  # 存储文件路径 -> 文件信息（例如文件路径对应的哈希值等）
        self.copy_num=copy_num
    def _hash(self, key):
        # 使用MD5哈希算法计算哈希值
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_server(self, server_id):
        # 添加服务器的虚拟节点
        for i in range(self.replicas_node_count):
            # 为每个服务器创建虚拟节点
            virtual_node = f"{server_id}:{i}"
            hash_value = self._hash(virtual_node)
            # 将虚拟节点添加到哈希环
            self.ring.append(hash_value)
            self.hash_server_dict[hash_value] = server_id
        # 保持哈希环有序
        self.ring.sort()

    def remove_server(self, server_id):
        # 移除服务器节点
        try:
            for i in range(self.replicas_node_count):
                virtual_node = f"{server_id}:{i}"
                hash_value = self._hash(virtual_node)
                if hash_value in self.hash_server_dict:
                    self.ring.remove(hash_value)
                    del self.hash_server_dict[hash_value]
            # 重新排序哈希环
            self.ring.sort()
        except Exception as e:
            print(f'ConsistentHashLoadBalancer的remove_server的e:{e}')

    def get_server(self, key):
        # 根据请求的key获取对应的服务器节点
        if not self.ring:
            return None
        # 计算请求的哈希值
        hash_value = self._hash(key)
        # 在哈希环上找到第一个不小于该哈希值的虚拟节点
        index = bisect.bisect_left(self.ring, hash_value)
        if index == len(self.ring):
            index = 0  # 如果到达环的末尾，回到环的开头
            # 获取顺延的 num_servers 个节点，包括自己
        servers = []
        visited = set()  # 用于记录已经添加的节点

        # 从当前节点开始，查找顺延的 num_servers 个节点，直到找到足够的不同节点
        while len(servers) < self.copy_num:
            node = self.ring[(index + len(servers)) % len(self.ring)]
            # 确保节点不重复
            if node not in visited:
                visited.add(node)
                servers.append(self.hash_server_dict[node])
        return servers

    def replace_node(self, old_node, new_node):
        self.remove_server(old_node)
        self.add_server(new_node)

    def get_files_to_migrate(self,files,node_id):
        files_to_migrate = []
        for file_path in files:
            # 获取当前文件的服务器
            current_server = self.get_server(file_path)
            if current_server == node_id:
                # 如果文件当前存储在需要迁移的节点上
                files_to_migrate.append(file_path)

        return files_to_migrate

    def get_next_server(self, dead_node_id):
        """获取死节点后一个节点"""
        # 获取死节点的哈希值
        dead_node_hash = self._hash(dead_node_id)

        # 使用二分查找找到下一个大于dead_node_hash的节点
        index = bisect.bisect_right(self.ring, dead_node_hash)

        # 如果index等于环的长度，说明已达到环末尾，需要回到环的开头
        if index == len(self.ring):
            index = 0

        # 返回下一个节点的ID
        next_node_hash = self.ring[index]
        return self.hash_server_dict[next_node_hash]

    def get_last_server(self, dead_node_id):
        """获取死节点前一个节点"""
        # 获取死节点的哈希值
        dead_node_hash = self._hash(dead_node_id)

        # 使用二分查找找到下一个小于dead_node_hash的节点
        index = bisect.bisect_left(self.ring, dead_node_hash)

        # 如果index等于0，说明已达到环的开头，需要回到环的末尾
        if index == 0:
            index = len(self.ring) - 1
        else:
            index -= 1

        # 返回上一个节点的ID
        last_node_hash = self.ring[index]
        return self.hash_server_dict[last_node_hash]

    def get_need_translate_nodes(self,new_node_id):
        s=[]
        t=new_node_id
        for i in range(0,self.copy_num):
            t=self.get_next_server(t)
            s.append(t)
        return s
    def is_last_copy(self,file_path,node_id):
        return node_id== self.get_server(file_path)[-1]










