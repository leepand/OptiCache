import grpc
from concurrent import futures
import diskcache as dc
import cache_pb2
import cache_pb2_grpc
import logging
import time
import threading

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler()
    ]
)

class CacheService(cache_pb2_grpc.CacheServiceServicer):
    def __init__(self, coordinator_address, node_address):
        self.cache = dc.Cache('center_cache_db', raw=True)
        self.coordinator_address = coordinator_address
        self.node_address = node_address
        self.register_with_coordinator()
        # self.start_heartbeat()
        logging.info(f"Cache node started at {self.node_address}")
        
        
    def Heartbeat(self, request, context):
        return cache_pb2.HeartbeatResponse(success=True)

    def register_with_coordinator2(self):
        """向 Coordinator 注册当前节点"""
        try:
            channel = grpc.insecure_channel(self.coordinator_address)
            stub = cache_pb2_grpc.CoordinatorServiceStub(channel)
            response = stub.RegisterNode(cache_pb2.RegisterNodeRequest(address=self.node_address))
            if response.success:
                logging.info(f"Registered with Coordinator at {self.coordinator_address}")
            else:
                logging.error("Failed to register with Coordinator")
        except Exception as e:
            logging.error(f"Failed to register with Coordinator: {e}")
            
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


    def start_heartbeat(self):
        """定期向 Coordinator 发送心跳"""
        def send_heartbeat():
            while True:
                try:
                    channel = grpc.insecure_channel(self.coordinator_address)
                    stub = cache_pb2_grpc.CacheServiceStub(channel)
                    response = stub.Heartbeat(cache_pb2.HeartbeatRequest())
                    if response.success:
                        logging.debug("Heartbeat sent to Coordinator")
                    else:
                        logging.warning("Heartbeat failed")
                except Exception as e:
                    logging.error(f"Heartbeat error: {e}")
                time.sleep(5)  # 每 5 秒发送一次心跳

        threading.Thread(target=send_heartbeat, daemon=True).start()

    def Get(self, request, context):
        key = request.key.encode('utf-8')
        value = self.cache.get(key)
        #logging.info(f"Get request: key={request.key}, found={value is not None}")
        return cache_pb2.GetResponse(value=value if value else b"", found=value is not None)

    def Set(self, request, context):
        key = request.key.encode('utf-8')
        self.cache.set(key, request.value)
        #logging.info(f"Set request: key={request.key}")
        return cache_pb2.SetResponse(success=True)

    def Delete(self, request, context):
        key = request.key.encode('utf-8')
        if key in self.cache:
            del self.cache[key]
            logging.info(f"Delete request: key={request.key}")
            return cache_pb2.DeleteResponse(success=True)
        logging.warning(f"Delete request: key={request.key} not found")
        return cache_pb2.DeleteResponse(success=False)

    def Heartbeat(self, request, context):
        logging.debug("Heartbeat received")
        return cache_pb2.HeartbeatResponse(success=True)

def serve(port, coordinator_address):
    node_address = f"x.31.18.x:{port}"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cache_pb2_grpc.add_CacheServiceServicer_to_server(CacheService(coordinator_address, node_address), server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    logging.info(f"Cache node is running on port {port}")
    server.wait_for_termination()

if __name__ == '__main__':
    import sys
    if len(sys.argv) < 3:
        logging.error("Usage: python cache_node.py <port> <coordinator_address>")
        sys.exit(1)
    port = int(sys.argv[1])  # 通过命令行参数指定端口
    coordinator_address = sys.argv[2]  # 通过命令行参数指定 Coordinator 地址
    serve(port, coordinator_address)
