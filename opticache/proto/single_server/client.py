import grpc
import cache_pb2
import cache_pb2_grpc

def run():
    channel = grpc.insecure_channel('x.31.18.x:6001')
    stub = cache_pb2_grpc.CacheServiceStub(channel)

    # 测试 Set
    set_response = stub.Set(cache_pb2.SetRequest(key="key1", value="value1"))
    print("Set response:", set_response.success)

    # 测试 Get
    get_response = stub.Get(cache_pb2.GetRequest(key="key1"))
    print("Get response:", get_response.value, get_response.found)

    # 测试 Delete
    delete_response = stub.Delete(cache_pb2.DeleteRequest(key="key1"))
    print("Delete response:", delete_response.success)

if __name__ == '__main__':
    run()


channel = grpc.insecure_channel('x.31.18.x:6001')
stub = cache_pb2_grpc.CacheServiceStub(channel)


# %%timeit -r 1000 -n 20
# 测试 Get
get_response = stub.Get(cache_pb2.GetRequest(key="key1"))
#print("Get response:", get_response.value, get_response.found)

# %%time
import pickle
get_response = stub.Get(cache_pb2.GetRequest(key="key1"))
if get_response.found:
    print("Get response:", pickle.loads(get_response.value), get_response.found)
else:
    print("Get response:", get_response.value, get_response.found)
