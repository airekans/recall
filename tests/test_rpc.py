from recall import rpc
import unittest
from recall.server import RpcServerStat, RpcServer
from test_proto import test_pb2
from recall.proto import rpc_meta_pb2
from recall import loadbalance
import gevent


class FakeTcpSocket(object):

    global_is_client = None

    def __init__(self, is_client=True, send_func=None, recv_func=None):
        self.__recv_content = ""
        self.__send_content = ""
        self.__is_connected = False
        if FakeTcpSocket.global_is_client is not None:
            self.__is_client = FakeTcpSocket.global_is_client
        else:
            self.__is_client = is_client
        self._send_func = self._send if send_func is None else send_func
        self._recv_func = self._recv if recv_func is None else recv_func

    def set_send_func(self, send_func):
        self._send_func = send_func

    def set_recv_func(self, recv_func):
        self._recv_func = recv_func

    def set_is_client(self, is_client):
        self.__is_client = is_client

    def connect(self, addr):
        self.__is_connected = True

    def close(self):
        self.__is_connected = False

    def setsockopt(self, *args):
        pass

    def is_connected(self):
        return self.__is_connected

    def recv(self, size):
        return self._recv_func(size)

    def _recv(self, size):
        if self.__is_client:
            while len(self.__send_content) == 0: # not recv anything
                gevent.sleep(0)

        if len(self.__recv_content) == 0:
            if not self.__is_client:
                while self.__is_connected:
                    gevent.sleep(1)

            return ""

        buf = self.__recv_content[:size]
        self.__recv_content = self.__recv_content[size:]
        return buf

    def _send(self, buf):
        self.__send_content += buf
        return len(buf)

    def send(self, buf):
        return self._send_func(buf)

    def set_recv_content(self, recv_content):
        self.__recv_content = recv_content

    def get_send_content(self):
        return self.__send_content


def fake_spawn(func, *args, **kwargs):
    func(*args, **kwargs)


class FakeTcpConnection(rpc.TcpConnection):

    rpc.TcpConnection.socket_cls = FakeTcpSocket

    def __init__(self, addr, evt_listener=None, get_flow_func=None,
                 spawn=None, send_task_num=None, load_balancer=None,
                 avg_delay=0, heartbeat_interval=1):
        if evt_listener is None:
            evt_listener = self.fake_state_evt_listener
        if get_flow_func is None:
            get_flow_func = self.fake_get_flow_func
        rpc.TcpConnection.__init__(self, addr, evt_listener, get_flow_func,
                                   spawn=fake_spawn,
                                   heartbeat_interval=heartbeat_interval)
        self._send_task_num = send_task_num
        self._avg_delay_per_min = avg_delay

    def fake_state_evt_listener(self, *args, **kwargs):
        pass

    def fake_get_flow_func(self):
        raise NotImplementedError

    def get_socket(self):
        return self._socket

    def get_pending_send_task_num(self):
        if self._send_task_num is None:
            return super(FakeTcpConnection, self).get_pending_send_task_num()
        else:
            return self._send_task_num

    def get_avg_delay_per_min(self):
        return self._avg_delay_per_min

    def set_heartbeat_interval(self, interval):
        self._heartbeat_interval = interval

    def set_flow_func(self, flow_func):
        self._get_flow_func = flow_func

    def set_heartbeat_timeout(self, hb_timeout):
        self._heartbeat_timeout = hb_timeout


class FakeTcpChannel(rpc.TcpChannel):

    my_conn_cls = FakeTcpConnection

    def __init__(self, addr, spawn, heartbeat_interval=30,
                 connect_interval=30):
        rpc.TcpChannel.conn_cls = FakeTcpChannel.my_conn_cls
        rpc.TcpChannel.__init__(self, addr, connect_interval=connect_interval)

        for conn in self._all_connections:
            conn.set_heartbeat_interval(heartbeat_interval)
            conn.set_flow_func(self._get_flow_id)

    def get_connections(self):
        return self._good_connections

    def get_socket(self):
        return self._all_connections[0].get_socket()

    def get_flow_id(self):
        return self._flow_id


class LoadBalancerTest(unittest.TestCase):

    class TcpConn(FakeTcpConnection):
        def _spawn_workers(self):
            self._workers = []

    def setUp(self):
        self.req = test_pb2.TestRequest()
        self.conn_cls = LoadBalancerTest.TcpConn

    def test_SingleLoadBalancer(self):
        balancer = loadbalance.SingleConnLoadBalancer()
        conns = [self.conn_cls('127.0.0.1:11111')]
        conn = balancer.get_connection_for_req(0, self.req, conns)
        self.assertIs(conn, conns[0])

    def test_SingleLoadBalancerWithMultipleConn(self):
        balancer = loadbalance.SingleConnLoadBalancer()
        conns = [self.conn_cls('127.0.0.1:11111'),
                 self.conn_cls('127.0.0.1:11112')]
        try:
            conn = balancer.get_connection_for_req(0, self.req, conns)
            self.fail()
        except AssertionError:
            pass

    def test_IncLoadBalancerWithSingleConn(self):
        balancer = loadbalance.FixedLoadBalancer()
        conns = [self.conn_cls('127.0.0.1:11111')]
        for flow_id in xrange(10):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            self.assertIs(conn, conns[0])

    def test_IncLoadBalancerWithMultipleConns(self):
        balancer = loadbalance.FixedLoadBalancer()
        conns = [self.conn_cls('127.0.0.1:11111'),
                 self.conn_cls('127.0.0.1:11112'),
                 self.conn_cls('127.0.0.1:11112')]
        for flow_id in xrange(10):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            self.assertIs(conn, conns[flow_id % len(conns)])

    def test_RandomLoadBalancerWithSingleConn(self):
        balancer = loadbalance.RandomLoadBalancer()
        conns = [self.conn_cls('127.0.0.1:11111')]
        for flow_id in xrange(10):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            self.assertIs(conn, conns[0])

    def test_RandomLoadBalancerWithMultipleConns(self):
        balancer = loadbalance.RandomLoadBalancer()
        conns = [self.conn_cls('127.0.0.1:11111'),
                 self.conn_cls('127.0.0.1:11112'),
                 self.conn_cls('127.0.0.1:11113')]
        conn_set = set(conns)
        conn_count = dict((conn, 0) for conn in conns)
        for flow_id in xrange(10):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            self.assertIn(conn, conn_set)
            conn_count[conn] += 1

        count_sum = 0
        for count in conn_count.itervalues():
            self.assertGreaterEqual(count, 0)
            count_sum += count

        self.assertEqual(10, count_sum)

    def test_ReqNumLoadBalancerWithSingleConn(self):
        balancer = loadbalance.ReqNumLoadBalancer()
        conns = [self.conn_cls('127.0.0.1:11111', send_task_num=2)]
        for flow_id in xrange(10):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            self.assertIs(conn, conns[0])

    def test_ReqNumLoadBalancerWithMultipleConns(self):
        balancer = loadbalance.ReqNumLoadBalancer()
        conns = [self.conn_cls('127.0.0.1:11111', send_task_num=3),
                 self.conn_cls('127.0.0.1:11112', send_task_num=2),
                 self.conn_cls('127.0.0.1:11113', send_task_num=1)]
        for flow_id in xrange(10):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            self.assertIs(conn, conns[2])

        conns[2] = self.conn_cls('127.0.0.1:11113', send_task_num=4)
        for flow_id in xrange(10):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            self.assertIs(conn, conns[1])

    def test_DelayLoadBalancerWithSingleConn(self):
        balancer = loadbalance.DelayLoadBalancer()
        conns = [self.conn_cls('127.0.0.1:11111', avg_delay=2)]
        for flow_id in xrange(10):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            self.assertIs(conn, conns[0])

    def test_DelayLoadBalancerWithMultipleConns(self):
        balancer = loadbalance.DelayLoadBalancer(False)
        conns = [self.conn_cls('127.0.0.1:11111', avg_delay=0.3),
                 self.conn_cls('127.0.0.1:11112', avg_delay=0.2),
                 self.conn_cls('127.0.0.1:11113', avg_delay=0.1)]
        conn_map = dict((conn, 0) for conn in conns)
        for flow_id in xrange(1000):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            conn_map[conn] += 1
            self.assertIn(conn, conns)
        print [(conn.get_avg_delay_per_min(), cnt) for conn, cnt in conn_map.iteritems()]

        conns[2] = self.conn_cls('127.0.0.1:11112', avg_delay=0.4)
        conn_map = dict((conn, 0) for conn in conns)
        for flow_id in xrange(1000):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            conn_map[conn] += 1
            self.assertIn(conn, conns)
        print [(conn.get_avg_delay_per_min(), cnt) for conn, cnt in conn_map.iteritems()]

    def test_DelayLoadBalancerWithInvalidDelays(self):
        balancer = loadbalance.DelayLoadBalancer(False)
        conns = [self.conn_cls('127.0.0.1:11111', avg_delay=-1),
                 self.conn_cls('127.0.0.1:11112', avg_delay=-1),
                 self.conn_cls('127.0.0.1:11113', avg_delay=-1)]
        conn_map = dict((conn, 0) for conn in conns)
        for flow_id in xrange(1000):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            conn_map[conn] += 1
            self.assertIn(conn, conns)
        print [(conn.get_avg_delay_per_min(), cnt) for conn, cnt in conn_map.iteritems()]

        conns[2] = self.conn_cls('127.0.0.1:11113', avg_delay=0.4)
        conn_map = dict((conn, 0) for conn in conns)
        for flow_id in xrange(1000):
            conn = balancer.get_connection_for_req(flow_id, self.req, conns)
            conn_map[conn] += 1
            self.assertIn(conn, conns)
        print [(conn.get_avg_delay_per_min(), cnt) for conn, cnt in conn_map.iteritems()]


class TcpConnectionTest(unittest.TestCase):

    def setUp(self):
        self.conn = FakeTcpConnection('127.0.0.1:11111')

    def tearDown(self):
        rpc.TcpConnection.socket_cls = FakeTcpSocket

    def test_not_connected_after_getting_connection(self):
        self.assertFalse(self.conn.is_connected())
        self.conn.connect()
        self.assertTrue(self.conn.is_connected())

    def test_not_connected_after_connect_fail(self):
        class ConnectFailSocket(FakeTcpSocket):
            def connect(self, addr):
                raise gevent.socket.error(123, 'connect fail')

        rpc.TcpConnection.socket_cls = ConnectFailSocket
        self.assertFalse(self.conn.is_connected())
        self.assertRaises(rpc.TcpConnection.Exception, self.conn.connect)
        self.assertFalse(self.conn.is_connected())

    def test_reconnect_after_close(self):
        self.assertFalse(self.conn.is_connected())

        for _ in xrange(3):
            self.conn.connect()
            self.assertTrue(self.conn.is_connected())
            self.conn.close()
            self.assertFalse(self.conn.is_connected())


class TcpChannelTest(unittest.TestCase):

    def setUp(self):
        self.channel = FakeTcpChannel('127.0.0.1:11111', None)
        self.channel.connect()
        self.assertTrue(self.channel.is_connected())
        self.assertTrue(self.channel.get_socket().is_connected())

        self.service_stub = test_pb2.TestService_Stub(self.channel)
        self.method = None
        for method in self.service_stub.GetDescriptor().methods:
            self.method = method
            break

        self.service_descriptor = self.method.containing_service
        request_class = self.service_stub.GetRequestClass(self.method)
        self.request_class = request_class
        self.request = request_class(name='test', num=123)
        self.response_class = self.service_stub.GetResponseClass(self.method)

    def tearDown(self):
        FakeTcpSocket.global_is_client = None
        FakeTcpChannel.my_conn_cls = FakeTcpConnection
        conns = self.channel.get_connections()
        self.channel.close()
        for conn in conns:
            self.assertFalse(conn.is_connected())

    def get_serialize_message(self, flow_id, msg):
        meta_info = rpc_meta_pb2.MetaInfo(
            flow_id=flow_id,
            service_name=self.service_descriptor.full_name,
            method_name=self.method.name)
        return rpc._serialize_message(meta_info, msg)

    def test_not_connected_after_getting_channel(self):
        channel = FakeTcpChannel('127.0.0.1:11111', None,
                                 heartbeat_interval=2)
        self.assertFalse(channel.is_connected())

    def test_connect_multiple_addr_and_fail_some(self):
        class ConnectFailTcpConnection(FakeTcpConnection):
            def connect(self):
                if self._addr[1] == 11112:
                    raise rpc.TcpConnection.Exception(213, 'connect fail')
                else:
                    FakeTcpConnection.connect(self)

        FakeTcpChannel.my_conn_cls = ConnectFailTcpConnection
        channel = FakeTcpChannel(('127.0.0.1:11111', '127.0.0.1:11112'),
                                 None, heartbeat_interval=2)
        self.assertFalse(channel.is_connected())

        channel.connect()
        self.assertEqual(1, len(channel.get_connections()))

    def test_CallMethod(self):
        channel = self.channel
        sock = channel.get_socket()
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = self.response_class(return_code=0, msg='SUCCESS')
        serialized_response = self.get_serialize_message(0, rsp)
        sock.set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, sock.get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertEqual(rsp, actual_rsp, str(actual_rsp))

    def test_CallMethodTimeout(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())
        channel.get_socket().set_is_client(False)

        serialized_request = self.get_serialize_message(0, self.request)

        controller = rpc.RpcController(method_timeout=1)
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, channel.get_socket().get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.SERVICE_TIMEOUT, controller.err_code)

    def test_CallMethodWithSendingError(self):
        channel = self.channel
        sock = channel.get_socket()
        self.assertEqual(0, channel.get_flow_id())

        def send_func(_buf):
            from socket import error as soc_error
            raise soc_error

        sock.set_send_func(send_func)

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual('', sock.get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.SERVER_CLOSE_CONN_ERROR, controller.err_code)

    def test_CallMethodWithEmptyBuffer(self):
        channel = self.channel
        sock = channel.get_socket()
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        sock.set_recv_content('')

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, sock.get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.SERVER_CLOSE_CONN_ERROR, controller.err_code)

    def test_CallMethodWithBufferNotStartsWithPb(self):
        channel = self.channel
        sock = channel.get_socket()
        self.assertIsNotNone(sock)
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        sock.set_recv_content('AB1231')

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, sock.get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.SERVER_CLOSE_CONN_ERROR, controller.err_code)

    def test_CallMethodWithWrongFlowId(self):
        channel = self.channel
        sock = channel.get_socket()
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = self.response_class(return_code=0, msg='SUCCESS')
        serialized_response = self.get_serialize_message(2, rsp)
        sock.set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, sock.get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.SERVER_CLOSE_CONN_ERROR, controller.err_code)

    def test_CallMethodWithExceptionInRecv(self):
        channel = self.channel
        self.assertEqual(0, channel.get_flow_id())
        self.assertEqual(1, len(channel.get_connections()))

        def recv_exception(size):
            raise RuntimeError

        channel.get_socket().set_recv_func(recv_exception)

        controller = rpc.RpcController()

        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertIsNone(actual_rsp)
        self.assertEqual(0, len(channel.get_connections()))

    def test_CallMethodWithWrongMetaInfo(self):
        channel = self.channel
        sock = channel.get_socket()
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = self.response_class(return_code=0, msg='SUCCESS')
        meta_info = rpc_meta_pb2.MetaInfo(flow_id=0,
                                          service_name='WrongServiceName',
                                          method_name='WrongMethodName')
        serialized_response = rpc._serialize_message(meta_info, rsp)
        sock.set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, sock.get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc.RpcController.WRONG_RSP_META_ERROR, controller.err_code)

    def test_CallMethodWithErrorInRspMeta(self):
        channel = self.channel
        sock = channel.get_socket()
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = rpc_meta_pb2.ErrorResponse(err_code=rpc_meta_pb2.SERVER_SERVICE_ERROR,
                                         err_msg='test error')
        meta_info = rpc_meta_pb2.MetaInfo(flow_id=0,
                                          service_name=self.service_descriptor.full_name,
                                          method_name=self.method.name,
                                          has_error=True)
        serialized_response = rpc._serialize_message(meta_info, rsp)
        sock.set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = channel.CallMethod(self.method, controller,
                                        self.request, self.response_class, None)

        self.assertEqual(serialized_request, sock.get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertIsNone(actual_rsp)
        self.assertTrue(controller.Failed())
        self.assertEqual(rpc_meta_pb2.SERVER_SERVICE_ERROR, controller.err_code)

    def test_CallMethodAsync(self):
        channel = self.channel
        sock = self.channel.get_socket()
        self.assertEqual(0, channel.get_flow_id())

        serialized_request = self.get_serialize_message(0, self.request)
        rsp = self.response_class(return_code=0, msg='SUCCESS')
        serialized_response = self.get_serialize_message(0, rsp)
        sock.set_recv_content(serialized_response)

        controller = rpc.RpcController()
        actual_rsp = []
        done = lambda ctrl, rsp: actual_rsp.append(rsp)
        result = channel.CallMethod(self.method, controller,
                                    self.request, self.response_class, done)
        self.assertIsNone(result)
        self.assertEqual(0, len(actual_rsp))
        gevent.sleep(1)

        self.assertEqual(serialized_request, sock.get_send_content())
        self.assertEqual(1, channel.get_flow_id())
        self.assertEqual(1, len(actual_rsp))
        self.assertEqual(rsp, actual_rsp[0], str(actual_rsp))
        self.assertFalse(controller.Failed())

    def test_CallMethodWithUserFlowId(self):
        class TestUserFlowIdTcpChannel(FakeTcpChannel):
            def _call_method_on_conn(self, conn, flow_id, _m, _r,
                                     _req, _rsp, _done):
                self.user_conn = conn

        addrs = ('127.0.0.1:11111', '127.0.0.1:11112')
        channel = TestUserFlowIdTcpChannel(addrs, None)
        channel.connect()
        self.assertEqual(0, channel.get_flow_id())
        conns = channel.get_connections()
        self.assertEqual(len(addrs), len(conns))

        controller = rpc.RpcController()
        res = channel.CallMethod(self.method, controller,
                                 self.request, self.response_class, None)
        self.assertIsNone(res)
        self.assertEqual(1, channel.get_flow_id())
        self.assertIn(channel.user_conn, conns)

        # call again and the conn should change
        controller = rpc.RpcController()
        res = channel.CallMethod(self.method, controller,
                                 self.request, self.response_class, None)
        self.assertIsNone(res)
        self.assertEqual(2, channel.get_flow_id())
        self.assertIn(channel.user_conn, conns)

        # call with user defined flow id
        controller = rpc.RpcController(flow_id=3)
        res = channel.CallMethod(self.method, controller,
                                 self.request, self.response_class, None)
        self.assertIsNone(res)
        self.assertEqual(3, channel.get_flow_id())
        self.assertIs(conns[1], channel.user_conn)

        # call again and the conn should be the same
        controller = rpc.RpcController(flow_id=3)
        res = channel.CallMethod(self.method, controller,
                                 self.request, self.response_class, None)
        self.assertIsNone(res)
        self.assertEqual(4, channel.get_flow_id())
        self.assertIs(conns[1], channel.user_conn)

    def test_connection_heartbeat(self):
        channel = FakeTcpChannel('127.0.0.1:11111', None,
                                 heartbeat_interval=2)
        channel.connect()
        self.assertTrue(self.channel.get_socket().is_connected())
        socket = channel.get_socket()

        self.service_descriptor = rpc_meta_pb2.BuiltinService.GetDescriptor()
        self.method = self.service_descriptor.FindMethodByName('HeartBeat')

        req = rpc_meta_pb2.HeartBeatRequest(magic_num=4231)
        serialized_request = self.get_serialize_message(0, req)
        rsp = rpc_meta_pb2.HeartBeatResponse(return_code=0)
        serialized_response = self.get_serialize_message(0, rsp)
        socket.set_recv_content(serialized_response)

        gevent.sleep(3)
        self.assertEqual(serialized_request, socket.get_send_content())
        self.assertEqual(1, channel.get_flow_id())

    def test_connection_heartbeat_fail(self):
        FakeTcpSocket.global_is_client = False
        channel = FakeTcpChannel('127.0.0.1:11111', None,
                                 heartbeat_interval=1)
        channel.connect()
        self.assertTrue(self.channel.get_socket().is_connected())
        good_conns = channel.get_connections()
        self.assertEqual(1, len(good_conns))
        good_conns[0].set_heartbeat_timeout(1)

        gevent.sleep(9)
        self.assertEqual(0, len(good_conns))

    def test_connection_fail_putting_tasks_to_other_conns(self):
        class OnlySendHbTcpConnection(FakeTcpConnection):
            def __init__(self, *args, **kwargs):
                super(OnlySendHbTcpConnection, self).__init__(*args, **kwargs)

                self._regular_send_task = []

            def _do_add_send_task(self, send_task):
                req_data = send_task.req_data
                if isinstance(req_data.req_msg, rpc_meta_pb2.HeartBeatRequest):
                    super(OnlySendHbTcpConnection, self)._do_add_send_task(send_task)
                else:
                    self._regular_send_task.append(send_task)

            def get_pending_send_tasks(self):
                regular_size = len(self._regular_send_task)
                hb_size = self._send_task_queue.qsize()
                for task in self._regular_send_task:
                    self._send_task_queue.put_nowait(task)
                self._regular_send_task = []

                assert self._send_task_queue.qsize() == regular_size + hb_size

                return super(OnlySendHbTcpConnection, self).get_pending_send_tasks()

            def get_pending_send_task_num(self):
                return self._send_task_queue.qsize() + len(self._regular_send_task)

        def done(_con, _rsp):
            pass

        FakeTcpSocket.global_is_client = False
        FakeTcpChannel.my_conn_cls = OnlySendHbTcpConnection
        addrs = ('127.0.0.1:11111', '127.0.0.1:11112')
        channel = FakeTcpChannel(addrs, None, heartbeat_interval=1)
        channel.connect()

        conns = channel.get_connections()
        self.assertEqual(2, len(conns))

        problem_conn = conns[0]
        problem_conn.set_heartbeat_timeout(1)

        sockets = [_conn.get_socket() for _conn in channel.get_connections()]
        for soc in sockets:
            self.assertTrue(soc.is_connected())

        for _ in xrange(10):
            controller = rpc.RpcController()
            res = channel.CallMethod(self.method, controller,
                        self.request, self.response_class, done)
            self.assertIsNone(res)

        self.assertGreater(problem_conn.get_pending_send_task_num(), 0)

        gevent.sleep(9)
        self.assertEqual(0, problem_conn.get_pending_send_task_num())
        good_conns = channel.get_connections()
        self.assertEqual(1, len(good_conns))

    def test_single_connection_failed_and_reconnect(self):
        channel = FakeTcpChannel('127.0.0.1:11111', None,
                                 connect_interval=1)
        channel.connect()
        self.assertEqual(1, len(channel.get_connections()))

        conn = channel.get_connections()[0]
        self.assertTrue(conn.is_connected())

        conn.close()
        self.assertFalse(channel.is_connected())
        self.assertFalse(conn.is_connected())
        controller = rpc.RpcController()
        res = channel.CallMethod(self.method, controller,
                                 self.request, self.response_class, None)
        self.assertIsNone(res)
        self.assertEqual(rpc.RpcController.CONN_FAILED_ERROR,
                         controller.err_code)

        gevent.sleep(2)
        controller = rpc.RpcController()
        res = channel.CallMethod(self.method, controller,
                                 self.request, self.response_class, None)
        self.assertIsNone(res)
        self.assertEqual(rpc.RpcController.CONN_FAILED_ERROR,
                         controller.err_code)

        gevent.sleep(1)
        self.assertTrue(conn.is_connected())
        self.assertTrue(channel.is_connected())

    def test_multiple_connections_failed_and_reconnect(self):
        FakeTcpSocket.global_is_client = False
        channel = FakeTcpChannel(('127.0.0.1:11111', '127.0.0.1:11112'),
                                 None, connect_interval=1)
        channel.connect()
        self.assertEqual(2, len(channel.get_connections()))

        conn = channel.get_connections()[0]
        self.assertTrue(conn.is_connected())

        conn.close()
        self.assertTrue(channel.is_connected())
        self.assertFalse(conn.is_connected())
        self.assertEqual(1, len(channel.get_connections()))

        # done should not be called
        done = lambda _c, _r: self.fail()
        controller = rpc.RpcController()
        res = channel.CallMethod(self.method, controller,
                                 self.request, self.response_class, done)
        self.assertIsNone(res)
        self.assertEqual(rpc.RpcController.SUCCESS,
                         controller.err_code)

        gevent.sleep(2)
        controller = rpc.RpcController()
        res = channel.CallMethod(self.method, controller,
                                 self.request, self.response_class, done)
        self.assertIsNone(res)
        self.assertEqual(rpc.RpcController.SUCCESS,
                         controller.err_code)

        gevent.sleep(1)
        self.assertTrue(conn.is_connected())
        self.assertTrue(channel.is_connected())
        self.assertEqual(2, len(channel.get_connections()))

    def test_resolve_addr_with_single_addr(self):
        expected_addrs = [('127.0.0.1', 30012)]
        addrs = self.channel.resolve_addr('127.0.0.1:30012')
        self.assertEqual(expected_addrs, addrs)

        addrs = self.channel.resolve_addr('ip/127.0.0.1:30012')
        self.assertEqual(expected_addrs, addrs)

    def test_resolve_addr_with_multiple_addrs(self):
        expected_addrs = [('127.0.0.1', 30012), ('192.168.1.1', 30021)]
        addrs = self.channel.resolve_addr(['127.0.0.1:30012',
                                           '192.168.1.1:30021'])
        self.assertEqual(expected_addrs, addrs)

        addrs = self.channel.resolve_addr(['ip/127.0.0.1:30012',
                                           'ip/192.168.1.1:30021'])
        self.assertEqual(expected_addrs, addrs)

    def test_resolve_addr_with_wrong_addr(self):
        addrs = self.channel.resolve_addr('127.0.0.1')
        self.assertIsNone(addrs)


class RpcClientTest(unittest.TestCase):

    def setUp(self):
        rpc.RpcClient.tcp_channel_class = FakeTcpChannel
        self.client = rpc.RpcClient()

    def tearDown(self):
        FakeTcpChannel.my_conn_cls = FakeTcpConnection

    def test_get_tcp_channel_with_one_ip(self):
        test_addr = '127.0.0.1:30002'
        channel1 = self.client.get_tcp_channel(test_addr)
        channel2 = self.client.get_tcp_channel(test_addr)
        self.assertIs(channel1, channel2)
        self.assertTrue(channel1.is_connected())

    def test_get_tcp_channel_with_multiple_ips(self):
        test_addrs = ('127.0.0.1:30002', '192.168.1.12:30003')
        channel1 = self.client.get_tcp_channel(test_addrs)
        channel2 = self.client.get_tcp_channel(test_addrs)
        self.assertIs(channel1, channel2)

    def test_get_tcp_channel_without_wait(self):
        class SlowConnectTcpConnection(FakeTcpConnection):
            def connect(self):
                gevent.sleep(2)

        FakeTcpChannel.my_conn_cls = SlowConnectTcpConnection

        test_addr = '127.0.0.1:30002'
        channel = self.client.get_tcp_channel(test_addr, False)
        self.assertFalse(channel.is_connected())


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
        return rpc._serialize_message(meta_info, msg)

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
