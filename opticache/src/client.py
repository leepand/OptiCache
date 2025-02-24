import grpc
import logging
import threading
import cache_pb2
import cache_pb2_grpc

import logging
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

class DistributedCacheClient:
    def __init__(self, coordinator_address):
        self.coordinator_channel = grpc.insecure_channel(coordinator_address)
        self.coordinator_stub = cache_pb2_grpc.CoordinatorServiceStub(self.coordinator_channel)
        self.node_channels = {}  # 缓存每个节点的 channel 和 stub
        self.node_addresses = []  # 缓存所有节点的地址
        self.lock = threading.Lock()  # 用于线程安全的锁
        self.update_node_addresses()  # 初始化时获取节点地址

    def update_node_addresses(self):
        """从 Coordinator 获取所有节点地址"""
        try:
            response = self.coordinator_stub.GetNodes(cache_pb2.GetNodesRequest(key=""))
            self.node_addresses = response.addresses
            logging.info(f"Updated node addresses: {self.node_addresses}")
        except Exception as e:
            logging.error(f"Failed to update node addresses: {e}")

    def get_node_stub(self, address):
        """获取或创建指定节点的 stub"""
        with self.lock:
            if address not in self.node_channels:
                channel = grpc.insecure_channel(address)
                stub = cache_pb2_grpc.CacheServiceStub(channel)
                self.node_channels[address] = (channel, stub)
            return self.node_channels[address][1]

    def get(self, key):
        """从主节点或副本节点读取数据"""
        node_addresses = self.get_nodes(key)
        if not node_addresses:
            raise Exception("No available nodes")
        
        # 优先从主节点读取，如果主节点失败，则尝试副本节点
        for address in node_addresses:
            try:
                stub = self.get_node_stub(address)
                response = stub.Get(cache_pb2.GetRequest(key=key))
                if response.found:  # 如果找到数据，返回数据
                    return response.value
                else:  # 如果 key 不存在，返回 None
                    return None
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNAVAILABLE:
                    logging.error(f"Node {address} unavailable: {e.details()}")
                else:
                    logging.error(f"Error reading from node {address}: {e}")
            except Exception as e:
                logging.error(f"Error reading from node {address}: {e}")
        
        # 如果所有节点都失败，抛出异常
        raise Exception("All nodes failed")

    def set(self, key, value):
        """将数据写入主节点和副本节点"""
        node_addresses = self.get_nodes(key)
        if not node_addresses:
            raise Exception("No available nodes")
        
        # 将数据写入所有节点（主节点和副本节点）
        success = False
        #print(node_addresses,"node_addresses")
        for address in node_addresses:
            try:
                stub = self.get_node_stub(address)
                response = stub.Set(cache_pb2.SetRequest(key=key, value=value))
                if response.success:
                    success = True
                    #logging.info(f"Data written to node: {address}")
                else:
                    logging.warning(f"Failed to write to node: {address}")
            except Exception as e:
                logging.error(f"Error writing to node {address}: {e}")
        if not success:
            raise Exception("Failed to write to all nodes")
        return True

    def delete(self, key):
        """从主节点和副本节点删除数据"""
        node_addresses = self.get_nodes(key)
        if not node_addresses:
            raise Exception("No available nodes")
        
        # 从所有节点删除数据
        success = False
        for address in node_addresses:
            try:
                stub = self.get_node_stub(address)
                response = stub.Delete(cache_pb2.DeleteRequest(key=key))
                if response.success:
                    success = True
                    logging.info(f"Data deleted from node: {address}")
                else:
                    logging.warning(f"Failed to delete from node: {address}")
            except Exception as e:
                logging.error(f"Error deleting from node {address}: {e}")
        if not success:
            raise Exception("Failed to delete from all nodes")
        return True

    def get_nodes(self, key):
        """根据 key 获取节点地址列表"""
        try:
            response = self.coordinator_stub.GetNodes(cache_pb2.GetNodesRequest(key=key))
            return list(set(response.addresses))
        except Exception as e:
            logging.error(f"Failed to get nodes for key {key}: {e}")
            return []

client = DistributedCacheClient('x.31.18.x:6001')
import numpy as np

# 计算元素数量
num_elements = 1024*1024 // 8

# 生成1MB的向量
vector = np.zeros(num_elements, dtype=np.float64)

# 检查向量的大小
print(f"向量的大小: {vector.nbytes / 1024 / 1024} MB")

# %%timeit -r 1000 -n 20
import logging
import pickle
# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)
binary_data = b'\x80\x04\x95\n\x00\x00\x00\x00\x00\x00\x00\x8c\x06vector\x94.'
binary_data=pickle.dumps(vector)
set_response = client.set("key1", binary_data)
#print(" Set response:", set_response)


# %%timeit -r 100 -n 20
# 测试 Get

get_response = client.get("key1")
#print("Get response (key1):", get_response)
