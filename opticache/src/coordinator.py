import grpc
from concurrent import futures
import hashlib
import threading
import time
import logging
import sqlite3
import cache_pb2
import cache_pb2_grpc

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)



def consistent_hash(key, nodes, replica_count=3):
    """一致性哈希算法"""
    hash_value = int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
    node_list = list(nodes)
    primary_index = hash_value % len(node_list)
    # 选择主节点和接下来的 replica_count-1 个节点作为副本节点
    addresses = [
        node_list[(primary_index + i) % len(node_list)]
        for i in range(replica_count)
    ]
    return addresses



class CoordinatorService(cache_pb2_grpc.CoordinatorServiceServicer):
    def __init__(self, db_path='coordinator.db'):
        self.db_path = db_path
        self.lock = threading.Lock()
        self.init_db()
        self.load_nodes()
        self.start_health_check()
        logging.info("Coordinator started")

    def init_db(self):
        """初始化数据库"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS nodes (
                    address TEXT PRIMARY KEY,
                    last_active REAL
                )
            ''')

    def load_nodes(self):
        """从数据库加载节点信息"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute('SELECT address, last_active FROM nodes')
            self.nodes = {row[0]: row[1] for row in cursor.fetchall()}
        logging.info(f"Loaded {len(self.nodes)} nodes from database")

    def save_node(self, address, last_active):
        """将节点信息保存到数据库"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('''
                INSERT OR REPLACE INTO nodes (address, last_active)
                VALUES (?, ?)
            ''', (address, last_active))

    def delete_node(self, address):
        """从数据库删除节点信息"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute('DELETE FROM nodes WHERE address = ?', (address,))

    def RegisterNode2(self, request, context):
        with self.lock:
            self.nodes[request.address] = time.time()
            self.save_node(request.address, time.time())
            logging.info(f"Node registered: {request.address}")
        return cache_pb2.RegisterNodeResponse(success=True)
    
    def RegisterNode(self, request, context):
        with self.lock:
            if request.address in self.nodes:
                logging.warning(f"Node already registered: {request.address}")
                return cache_pb2.RegisterNodeResponse(success=False)
            self.nodes[request.address] = time.time()
            self.save_node(request.address, time.time())
            logging.info(f"Node registered: {request.address}")
        return cache_pb2.RegisterNodeResponse(success=True)
    
    def GetNodes(self, request, context):
        with self.lock:
            if not self.nodes:
                logging.warning("No nodes available")
                return cache_pb2.GetNodesResponse(addresses=[])
            # 使用一致性哈希算法选择主节点和副本节点
            addresses = list(set(consistent_hash(request.key, self.nodes.keys())))
            logging.info(f"Nodes selected for key '{request.key}': {addresses}")
            return cache_pb2.GetNodesResponse(addresses=addresses)


    def GetNodes2(self, request, context):
        with self.lock:
            if not self.nodes:
                logging.warning("No nodes available")
                return cache_pb2.GetNodesResponse(addresses=[])
            # 使用一致性哈希算法选择主节点和副本节点
            key = request.key
            hash_value = int(hashlib.md5(key.encode('utf-8')).hexdigest(), 16)
            node_list = list(self.nodes.keys())
            primary_index = hash_value % len(node_list)
            # 选择主节点和接下来的 2 个节点作为副本节点
            addresses = [
                node_list[(primary_index + i) % len(node_list)]
                for i in range(3)
            ]
            logging.info(f"Nodes selected for key '{key}': {addresses}")
            return cache_pb2.GetNodesResponse(addresses=addresses)

    def start_health_check(self):
        def check_health():
            while True:
                with self.lock:
                    current_time = time.time()
                    for address in list(self.nodes.keys()):
                        try:
                            channel = grpc.insecure_channel(address)
                            stub = cache_pb2_grpc.CacheServiceStub(channel)
                            response = stub.Heartbeat(cache_pb2.HeartbeatRequest(), timeout=1)
                            if not response.success:
                                del self.nodes[address]
                                self.delete_node(address)
                                logging.warning(f"Node removed: {address}")
                        except:
                            del self.nodes[address]
                            self.delete_node(address)
                            logging.warning(f"Node removed: {address}")
                time.sleep(10)  # 每 10 秒检查一次

        threading.Thread(target=check_health, daemon=True).start()
        
    def register_with_coordinator(self):
        """向 Coordinator 注册当前节点"""
        retry_count = 3
        for i in range(retry_count):
            try:
                channel = grpc.insecure_channel(self.coordinator_address)
                stub = cache_pb2_grpc.CoordinatorServiceStub(channel)
                response = stub.RegisterNode(cache_pb2.RegisterNodeRequest(address=self.node_address))
                if response.success:
                    logging.info(f"Registered with Coordinator at {self.coordinator_address}")
                    return
                else:
                    logging.warning("Failed to register with Coordinator")
            except Exception as e:
                logging.error(f"Registration error: {e}")
            time.sleep(1)  # 等待 1 秒后重试
        logging.error("Failed to register after multiple attempts")


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cache_pb2_grpc.add_CoordinatorServiceServicer_to_server(CoordinatorService(), server)
    port=6001
    server.add_insecure_port('[::]:6001')
    server.start()
    logging.info("Coordinator is running on port 6001")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
