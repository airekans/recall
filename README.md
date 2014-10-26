recall
======

[![Build Status](https://travis-ci.org/airekans/recall.svg?branch=master)](https://travis-ci.org/airekans/recall)

Python High performance RPC framework based on protobuf

## Example

Suppose you want to write an Hello RPC service by recall, then you will define the following protocol:

```protobuf
// hello.proto

option py_generic_services = true; // This is required
message HelloRequest {
  string data;
}

message HelloResponse {
  string data;
}

service HelloService {
  rpc HelloMethod (HelloRequest) returns (HelloResponse);
}
```

Then compile the proto file:

```bash
$ protoc --python_out=. hello.proto
```

Then you can write your Hello service server like the following:

```python
import hello_pb2
from recall.server import RpcServer

class HelloServiceImpl(hello_pb2.HelloService):
    def HelloMethod(self, rpc_controller, request, done):
        print 'recv', request.data
        return hello_pb2.HelloResponse(data='world')

if __name__ == '__main__':
    addr = ('0.0.0.0', 12345)
    print 'server listen at %s' % str(addr)
    server = RpcServer(addr)
    server.register_service(HelloServiceImpl())
    server.run(print_stat_interval=60)

if __name__ == '__main__':
    main()
```

And to access Hello service, you can simply use `RpcClient` like the following:

```python
import hello_pb2
from recall.controller import RpcController
import recall.client

if __name__ == '__main__':
    client = recall.client.RpcClient()
    channel = client.get_tcp_channel(('127.0.0.1', 12345))
    stub = hello_pb2.HelloService_Stub(channel)

    req = hello_pb2.HelloRequest(data='hello')
    controller = RpcController()
    rsp = stub.HelloMethod(controller, req, None)
    print 'recv', rsp.data
```

## Installation

You can install recall by simply using pip:

```bash
$ pip install recall
```

Or you can install recall by github source code:

```bash
$ git clone https://github.com/airekans/recall.git
$ cd recall
$ sudo python setup.py install
```
