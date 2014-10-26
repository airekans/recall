import unittest
import gevent
from recall import rpc
from recall.codec import serialize_message
from recall.proto import rpc_meta_pb2
from recall.server import RpcServer, RpcServerStat
from test_proto import test_pb2
from test_rpc import FakeTcpSocket


class FakeRpcServer(RpcServer):

    def __init__(self, service_timeout=10):
        self._addr = ('127.0.0.1', 12345)
        self._services = {}
        self._service_timeout = service_timeout
        self._stat = RpcServerStat()
        self._pool = None
        self._spawn = gevent.spawn
        self._register_builtin_services()
        # not calling parent's __init__ to bypass the StreamServer init

    def set_service_timeout(self, service_timeout):
        self._service_timeout = service_timeout

    def get_service(self, name):
        return self._services[name]

    def run(self):
        pass

    def handle_connection(self, socket, addr):
        self._handle_connection(socket, addr)


class FakeTestService(test_pb2.TestService):
    def __init__(self, is_async, rsp, sleep_time=1):
        self.is_async = is_async
        self.rsp = rsp
        self.sleep_time = sleep_time

    def TestMethod(self, rpc_controller, request, done):
        if self.is_async:
            gevent.sleep(self.sleep_time)

        return self.rsp


class RpcServerTest(unittest.TestCase):

    def setUp(self):
        self.server = FakeRpcServer()

        self.service = test_pb2.TestService()
        self.method = None
        for method in self.service.GetDescriptor().methods:
            self.method = method
            break

        self.service_descriptor = self.service.GetDescriptor()

    def get_serialize_message(self, flow_id, service_desc, method_name, msg):
        has_error = isinstance(msg, rpc_meta_pb2.ErrorResponse)
        meta_info = rpc_meta_pb2.MetaInfo(flow_id=flow_id, service_name=service_desc.full_name,
                                          method_name=method_name, has_error=has_error)
        return serialize_message(meta_info, msg)

    def test_register_service(self):
        service_name = self.service_descriptor.full_name
        self.server.register_service(self.service)

        self.assertIs(self.service, self.server.get_service(service_name))

    def test_parse_message_with_empty_buf(self):
        self.assertIsNone(self.server.parse_message(''))

    def test_parse_message_with_non_reg_service(self):
        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        self.assertIsNone(self.server.parse_message(serialized_req[6:]))

    def test_parse_message_with_wrong_method(self):
        self.server.register_service(self.service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    'WrongMethodName', req)
        self.assertIsNone(self.server.parse_message(serialized_req[6:]))

    def test_parse_message_with_wrong_msg(self):
        self.server.register_service(self.service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        wrong_req = serialized_req[:-3] + 'abc'
        self.assertIsNone(self.server.parse_message(wrong_req[6:]))

    def test_parse_message(self):
        self.server.register_service(self.service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        result = self.server.parse_message(serialized_req[6:])
        self.assertIsNotNone(result)
        self.assertTrue(len(result) == 4)
        meta_info, service, method, actual_req = result
        self.assertIs(self.service, service)
        self.assertIs(self.method, method)
        self.assertEqual(req, actual_req)

    def test_handle_connection(self):
        rsp = test_pb2.TestResponse(return_code=0, msg='SUCCESS')
        service = FakeTestService(False, rsp)
        self.server.register_service(service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        serialized_rsp = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, rsp)
        socket = FakeTcpSocket(is_client=False)
        socket.connect(('127.0.0.1', 34567))
        socket.set_recv_content(serialized_req)

        t = gevent.spawn(self.server.handle_connection, socket, ('127.0.0.1', 34567))
        gevent.sleep(1)
        actual_serialized_rsp = socket.get_send_content()
        self.assertEqual(serialized_rsp, actual_serialized_rsp)

        socket.close()
        t.join()

    def test_handle_connection_async(self):
        rsp = test_pb2.TestResponse(return_code=0, msg='SUCCESS')
        service = FakeTestService(True, rsp)
        self.server.register_service(service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        serialized_rsp = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, rsp)
        socket = FakeTcpSocket(is_client=False)
        socket.connect(('127.0.0.1', 34567))
        socket.set_recv_content(serialized_req + serialized_req) # 2 requests

        t = gevent.spawn(self.server.handle_connection, socket, ('127.0.0.1', 34567))
        gevent.sleep(1)
        self.assertEqual("", socket.get_send_content())
        gevent.sleep(1)
        socket.close()
        t.join()

        actual_serialized_rsp = socket.get_send_content()
        self.assertEqual(serialized_rsp + serialized_rsp, actual_serialized_rsp)

    def test_handle_connection_timeout(self):
        self.server.set_service_timeout(1)
        rsp = test_pb2.TestResponse(return_code=0, msg='SUCCESS')
        service = FakeTestService(True, rsp, sleep_time=3)
        self.server.register_service(service)

        req = test_pb2.TestRequest(name='abc', num=1)
        serialized_req = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, req)
        err_rsp = rpc_meta_pb2.ErrorResponse(err_code=rpc_meta_pb2.SERVER_SERVICE_TIMEOUT,
                                             err_msg='service timeout')
        serialized_rsp = self.get_serialize_message(1, self.service_descriptor,
                                                    self.method.name, err_rsp)
        socket = FakeTcpSocket(is_client=False)
        socket.connect(('127.0.0.1', 34567))
        socket.set_recv_content(serialized_req)

        t = gevent.spawn(self.server.handle_connection, socket, ('127.0.0.1', 34567))
        gevent.sleep(2)
        actual_serialized_rsp = socket.get_send_content()
        self.assertEqual(serialized_rsp, actual_serialized_rsp)

        socket.close()
        t.join()

    def test_handle_heartbeat(self):
        req = rpc_meta_pb2.HeartBeatRequest(magic_num=4321)
        rsp = rpc_meta_pb2.HeartBeatResponse(return_code=0)
        service_desc = rpc_meta_pb2.BuiltinService.GetDescriptor()
        method_desc = service_desc.FindMethodByName('HeartBeat')
        serialized_req = self.get_serialize_message(1, service_desc,
                                                    method_desc.name, req)
        serialized_rsp = self.get_serialize_message(1, service_desc,
                                                    method_desc.name, rsp)

        socket = FakeTcpSocket(is_client=False)
        socket.connect(('127.0.0.1', 34567))
        socket.set_recv_content(serialized_req)

        t = gevent.spawn(self.server.handle_connection, socket, ('127.0.0.1', 34567))
        gevent.sleep(1)
        actual_serialized_rsp = socket.get_send_content()
        self.assertEqual(serialized_rsp, actual_serialized_rsp)

        socket.close()
        t.join()


if __name__ == '__main__':
    unittest.main()
