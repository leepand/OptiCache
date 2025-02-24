import grpc
from concurrent import futures
import diskcache as dc
import cache_pb2
import cache_pb2_grpc

class CacheService(cache_pb2_grpc.CacheServiceServicer):
    def __init__(self):
        self.cache = dc.Cache('center_cache_db')

    def Get(self, request, context):
        value = self.cache.get(request.key)
        # 如果键不存在，返回空的 bytes 对象
        return cache_pb2.GetResponse(value=value if value is not None else b"", found=value is not None)
        # return cache_pb2.GetResponse(value=value if value else "", found=value is not None)
    

    def Set(self, request, context):
        self.cache.set(request.key, request.value)
        return cache_pb2.SetResponse(success=True)

    def Delete(self, request, context):
        if request.key in self.cache:
            del self.cache[request.key]
            return cache_pb2.DeleteResponse(success=True)
        return cache_pb2.DeleteResponse(success=False)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    cache_pb2_grpc.add_CacheServiceServicer_to_server(CacheService(), server)
    server.add_insecure_port('[::]:6001')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
