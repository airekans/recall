import struct
import google.protobuf.service
from socket import error as soc_error
import logging
import time
import heapq

import gevent
import gevent.socket
import gevent.queue
import gevent.event

from recall import loadbalance
from recall.codec import serialize_message, parse_message, parse_meta
from recall.controller import RpcController
from recall.proto import rpc_meta_pb2
from recall.util import Pool, kill_all_but_this


class TcpConnectionStat(object):
    def __init__(self):
        self.total_req_num_per_min = 0
        self.total_rsp_num_per_min = 0
        self.total_delay_s_per_min = 0
        self.max_delay_s_per_min = -1
        self.min_delay_s_per_min = -1

    def add_req_stat(self, count=1):
        self.total_req_num_per_min += count

    def add_rsp_stat(self, count, delay_s):
        self.total_rsp_num_per_min += count
        self.total_delay_s_per_min = delay_s

        if delay_s > self.max_delay_s_per_min:
            self.max_delay_s_per_min = delay_s

        if self.min_delay_s_per_min < 0 or delay_s < self.min_delay_s_per_min:
            self.min_delay_s_per_min = delay_s

    @property
    def avg_delay_s_per_min(self):
        if self.total_rsp_num_per_min <= 0:
            return -1
        else:
            return self.total_delay_s_per_min / self.total_rsp_num_per_min

    def reset_stat_per_min(self):
        self.total_req_num_per_min = 0
        self.total_rsp_num_per_min = 0
        self.total_delay_s_per_min = 0
        self.min_delay_s_per_min = -1
        self.max_delay_s_per_min = -1


class TcpConnection(object):

    socket_cls = gevent.socket.socket

    class Exception(Exception):
        def __init__(self, err_code, err_msg):
            self.err_code, self.err_msg = err_code, err_msg

        def __str__(self):
            return "<TcpConnectionError (%d, '%s')>" % \
                   (self.err_code, self.err_msg)

    class RequestData(object):
        def __init__(self, is_async, *call_args):
            self.begin_time = 0
            self.is_async = is_async
            self.call_args = call_args
            self.rpc_controller = call_args[2]
            self.req_msg = call_args[3]

    # state enumeration definition
    CLOSED = 0
    CONNECTED = 1
    UNHEALTHY = 2

    def __init__(self, addr, state_evt_listener, get_flow_func,
                 spawn=gevent.spawn, heartbeat_interval=30):
        self._state = TcpConnection.CLOSED
        self._state_evt_listener = state_evt_listener
        self._get_flow_func = get_flow_func
        self._addr = addr
        self._spawn = spawn
        self._heartbeat_interval = heartbeat_interval
        self._heartbeat_timeout = 3
        self._socket = None
        self._is_closed = True

        self._send_task_queue = gevent.queue.Queue()
        self._recv_infos = {}
        self._timeout_queue = []
        self._workers = []
        self._stat = TcpConnectionStat()

        self._recv_meta_info = rpc_meta_pb2.MetaInfo()

    def __str__(self):
        return '<TcpConnection %s>' % str(self._addr)

    def _spawn_workers(self):
        self._workers = [gevent.spawn(self.send_loop), gevent.spawn(self.recv_loop),
                         gevent.spawn(self.timeout_loop), gevent.spawn(self.heartbeat_loop)]

    def get_addr(self):
        return self._addr

    def is_connected(self):
        return self._socket is not None

    # client should call connect after getting TcpConnection
    def connect(self):
        try:
            assert self._socket is None
            self._socket = TcpConnection.socket_cls()
            self._socket.connect(self._addr)
            self._socket.setsockopt(gevent.socket.SOL_TCP, gevent.socket.TCP_NODELAY, 1)
            self._socket.setsockopt(gevent.socket.IPPROTO_TCP, gevent.socket.TCP_NODELAY, 1)
            self.change_state(TcpConnection.CONNECTED)
            self._is_closed = False

            self._spawn_workers()
        except gevent.socket.error, e:
            self._socket = None
            raise TcpConnection.Exception(e.errno, e.strerror)

    def close(self):
        if self._is_closed:
            logging.warning('conn is closed')
            return

        self._socket.close()
        self._is_closed = True
        self.change_state(TcpConnection.CLOSED)

        # clear all data
        while not self._send_task_queue.empty():
            try:
                self._send_task_queue.get_nowait()
            except gevent.queue.Empty:
                pass

        self._timeout_queue = []

        # check if there is any request has not been processed.
        for v in self._recv_infos.itervalues():
            expected_meta, rpc_controller, response_class, done, req_data = v
            rpc_controller.SetFailed((RpcController.SERVER_CLOSE_CONN_ERROR,
                                      'channel has been closed prematurely'))
            self._finish_rpc(done, rpc_controller, None, req_data.is_async)

        self._recv_infos.clear()

        # socket has to be set to None here, because in _finish_rpc
        # the control flow may be switched to another greenlet.
        # And in TcpChannel, it may reconnect the connection.
        # If we don't set it at last, it may cause problem.
        self._socket = None

        if len(self._workers) > 0:
            kill_all_but_this(self._workers)

    def change_state(self, state):
        if self._state != state:
            old_state = self._state
            self._state = state
            self._state_evt_listener(self, old_state, state)

    # when change to unhealthy state,
    # TcpChannel will call this functions to get all
    # unsent task to dispatch to other good connections.
    def get_pending_send_tasks(self):
        unsent_tasks = []
        while not self._send_task_queue.empty():
            try:
                task = self._send_task_queue.get_nowait()
                req_data = task.req_data
                # all heartbeat requests and request targetting only to this
                # connection should not be sent in other connections
                if not isinstance(req_data.req_msg, rpc_meta_pb2.HeartBeatRequest) and \
                    req_data.rpc_controller.GetFlowId() is None:
                    unsent_tasks.append((req_data.is_async, req_data.call_args))
            except gevent.queue.Empty:
                pass

        logging.debug('unsent task num: %d' % len(unsent_tasks))
        return unsent_tasks

    def get_stat(self):
        return self._stat

    def get_avg_delay_per_min(self):
        return self.get_stat().avg_delay_s_per_min

    def get_pending_send_task_num(self):
        return self._send_task_queue.qsize()

    def add_send_task(self, flow_id, method_descriptor, rpc_controller,
                      request, response_class, done, req_data):
        send_task = lambda: self.send_req(flow_id, method_descriptor, rpc_controller,
                                          request, response_class, done, req_data)
        send_task.req_data = req_data

        self._do_add_send_task(send_task)

    def _do_add_send_task(self, send_task):
        self._send_task_queue.put_nowait(send_task)

    def _finish_rpc(self, done, controller, rsp, is_async):
        if is_async:
            self._spawn(done, controller, rsp)
        else:
            done(controller, rsp)

    def timeout_loop(self):
        while True:
            gevent.sleep(1)
            if len(self._timeout_queue) > 0:
                now = time.time()
                while len(self._timeout_queue) > 0:
                    deadline, flow_id = self._timeout_queue[0]  # the smallest element
                    if deadline > now:
                        break
                    else:
                        heapq.heappop(self._timeout_queue)
                        recv_info = self._recv_infos.get(flow_id)
                        if recv_info is None:
                            continue

                        meta_info, rpc_controller, response_class, done, req_data = \
                            recv_info
                        timeout_time = time.time()
                        self._stat.add_rsp_stat(1, timeout_time - req_data.begin_time)
                        err_msg = 'service timeout'
                        rpc_controller.SetFailed((RpcController.SERVICE_TIMEOUT, err_msg))
                        logging.warning(err_msg)
                        del self._recv_infos[flow_id]
                        self._finish_rpc(done, rpc_controller, None, req_data.is_async)

    def heartbeat_loop(self):
        service_descriptor = rpc_meta_pb2.BuiltinService_Stub.GetDescriptor()
        method_descriptor = service_descriptor.FindMethodByName('HeartBeat')
        request = rpc_meta_pb2.HeartBeatRequest(magic_num=4231)
        response_class = rpc_meta_pb2.HeartBeatResponse
        hb_fail_count = 0

        while True:
            gevent.sleep(self._heartbeat_interval)

            res = gevent.event.AsyncResult()
            flow_id = self._get_flow_func()
            rpc_controller = RpcController(method_timeout=self._heartbeat_timeout)
            done = lambda _, rsp: res.set(rsp)
            req_data = self.RequestData(False, flow_id, method_descriptor, rpc_controller,
                                        request, response_class, done)
            self.add_send_task(flow_id, method_descriptor, rpc_controller,
                               request, response_class, done,
                               req_data)
            hb_rsp = res.get()
            if hb_rsp is None:
                logging.warning('connection %s heartbeat timeout' % str(self._addr))
                hb_fail_count += 1
                if hb_fail_count >= 3:
                    logging.warning('connection %s become unhealthy' % str(self._addr))
                    self.change_state(TcpConnection.UNHEALTHY)
            else:
                hb_fail_count = 0
                self.change_state(TcpConnection.CONNECTED)

    def recv_loop(self):
        try:
            while True:
                if not self.recv_rsp():
                    break
        except Exception, e:
            logging.warning('recv_loop failed and exit: ' + str(e))
        finally:
            self.close()

    def send_loop(self):
        while True:
            send_task = self._send_task_queue.get()
            if not send_task():
                break

    def send_req(self, flow_id, method_descriptor, rpc_controller,
                 request, response_class, done, req_data):
        service_descriptor = method_descriptor.containing_service
        meta_info = rpc_meta_pb2.MetaInfo()
        meta_info.flow_id = flow_id
        meta_info.service_name = service_descriptor.full_name
        meta_info.method_name = method_descriptor.name
        serialized_req = serialize_message(meta_info, request)
        sent_bytes = 0
        try:
            while sent_bytes < len(serialized_req):
                sent_bytes += self._socket.send(serialized_req[sent_bytes:])
        except soc_error as e:
            logging.warning('socket error: ' + str(e))
            rpc_controller.SetFailed((RpcController.SERVER_CLOSE_CONN_ERROR,
                                      'connection has sending error'))
            self._finish_rpc(done, rpc_controller, None, req_data.is_async)

            self.close()
            return False

        req_data.begin_time = time.time()
        self._recv_infos[flow_id] = (meta_info, rpc_controller, response_class, done, req_data)
        self._stat.add_req_stat()
        if rpc_controller.method_timeout > 0:
            heapq.heappush(self._timeout_queue,
                           (req_data.begin_time + rpc_controller.method_timeout, flow_id))

        return True

    def _recv(self, expected_size):
        recv_buf = ''
        while len(recv_buf) < expected_size:
            try:
                buf = self._socket.recv(expected_size - len(recv_buf))
                if len(buf) == 0:
                    break

                recv_buf += buf
            except Exception, e:
                logging.warning('recv failed: ' + str(e))
                raise

        return recv_buf

    _expected_size = 2 + struct.calcsize("!I")
    _error_msg_name = rpc_meta_pb2.ErrorResponse.DESCRIPTOR.full_name

    # return True when the connection is not closed
    # return False when it's closed
    def recv_rsp(self):
        expected_size = TcpConnection._expected_size
        pb_buf = self._recv(expected_size)
        if len(pb_buf) == 0:
            logging.info('socket has been closed')
            return False
        if len(pb_buf) < expected_size:
            logging.warning('rsp buffer broken')
            return True
        if pb_buf[:2] != 'PB':
            logging.warning('rsp buffer not begin with PB')
            return True

        buf_size = struct.unpack("!I", pb_buf[2:])[0]
        pb_buf = self._recv(buf_size)
        if len(pb_buf) == 0:
            logging.info('socket has been closed')
            return False

        result = parse_meta(pb_buf, self._recv_meta_info)
        if result is None:
            logging.warning('pb decode error, skip this message')
            return True

        meta_len, pb_msg_len, meta_info = result

        if meta_info.flow_id in self._recv_infos:
            expected_meta, rpc_controller, response_class, done, req_data = \
                self._recv_infos[meta_info.flow_id]

            try:
                if meta_info.flow_id != expected_meta.flow_id:
                    err_msg = 'rsp flow id not match: %d %d' % (expected_meta.flow_id,
                                                                meta_info.flow_id)
                    raise TcpConnection.Exception(RpcController.WRONG_FLOW_ID_ERROR, err_msg)
                elif meta_info.service_name != expected_meta.service_name or \
                                meta_info.method_name != expected_meta.method_name:
                    err_msg = 'rsp meta not match'
                    raise TcpConnection.Exception(RpcController.WRONG_RSP_META_ERROR, err_msg)
                elif meta_info.HasField('has_error'):
                    if meta_info.msg_name != TcpConnection._error_msg_name:
                        err_msg = 'rsp meta has error, but with wrong msg name'
                        raise TcpConnection.Exception(RpcController.WRONG_MSG_NAME_ERROR, err_msg)
                    else:
                        response_class = rpc_meta_pb2.ErrorResponse
                elif meta_info.msg_name != response_class.DESCRIPTOR.full_name:
                    err_msg = 'wrong response class'
                    raise TcpConnection.Exception(RpcController.WRONG_MSG_NAME_ERROR, err_msg)

                rsp_time = time.time()
                self._stat.add_rsp_stat(1, rsp_time - req_data.begin_time)
                rsp = parse_message(pb_buf[8 + meta_len:8 + meta_len + pb_msg_len],
                                     response_class)

                if meta_info.HasField('has_error'):
                    raise TcpConnection.Exception(rsp.err_code, rsp.err_msg)

                del self._recv_infos[meta_info.flow_id]
                self._finish_rpc(done, rpc_controller, rsp, req_data.is_async)
                return True
            except TcpConnection.Exception as e:
                rpc_controller.SetFailed((e.err_code, e.err_msg))
                logging.warning(e.err_msg)
                del self._recv_infos[meta_info.flow_id]
                self._finish_rpc(done, rpc_controller, None, req_data.is_async)
                return True
        else:
            logging.warning('flow id not found: %d' % meta_info.flow_id)
            return True


class TcpChannel(google.protobuf.service.RpcChannel):

    conn_cls = TcpConnection
    _default_load_balancer = loadbalance.DelayLoadBalancer()
    _fixed_load_balancer = loadbalance.FixedLoadBalancer()

    def __init__(self, addr, load_balancer=None,
                 spawn=gevent.spawn, connect_interval=30):
        google.protobuf.service.RpcChannel.__init__(self)
        self._flow_id = 0
        self._addr = addr
        self._user_load_balancer = load_balancer
        self._balancer = None
        self._spawn = spawn

        self._all_connections = []
        self._good_connections = []
        self._bad_connections = []
        # TODO: handle addr format error
        for ip_port in self.resolve_addr(addr):
            conn = TcpChannel.conn_cls(ip_port, self.on_conn_state_changed,
                                       self._get_flow_id, spawn=spawn)
            # because these connections is not connected,
            # put them to bad connections
            self._all_connections.append(conn)

            # no need to add conn because it will
            # add itself in state_changed function.

        self._connected_event = gevent.event.Event()
        self._last_connect_time = time.time()
        self._connect_interval = connect_interval

    def __del__(self):
        self.close()

    def _get_flow_id(self):
        flow_id = self._flow_id
        self._flow_id += 1
        return flow_id

    # after close is called, the channel cannot be used
    def close(self):
        for conn in self._all_connections:
            if conn.is_connected():
                conn.close()

        self._good_connections = []
        self._bad_connections = []
        self._all_connections = []

    def connect(self):
        self._last_connect_time = time.time()
        if not self.is_connected():
            self._do_connect()

    def _do_connect(self):
        for conn in self._all_connections:
            try:
                if not conn.is_connected():
                    conn.connect()
            except TcpConnection.Exception, e:
                logging.warning('connection %s failed: %s' % (conn, str(e)))
                # just ignore it and continue connect next one

    def is_connected(self):
        return len(self._good_connections) > 0
    
    def wait_until_connected(self):
        while len(self._good_connections) == 0:
            self._connected_event.wait()

        self._connected_event.clear()

    def on_conn_state_changed(self, conn, old_state, new_state):
        try:  # TODO: this try block should be removed in production
            if new_state == TcpConnection.CONNECTED:
                if old_state == TcpConnection.UNHEALTHY:
                    self._bad_connections.remove(conn)
                self._good_connections.append(conn)
            elif new_state == TcpConnection.UNHEALTHY:
                assert old_state == TcpConnection.CONNECTED
                self._good_connections.remove(conn)
                self._bad_connections.append(conn)

                if len(self._good_connections) == 0:
                    logging.warning('All connections in channel is unhealthy.')
                else:
                    unsent_tasks = conn.get_pending_send_tasks()
                    for is_async, call_args in unsent_tasks:
                        (flow_id, method_descriptor,
                         rpc_controller, request, response_class,
                         done) = call_args
                        conn = self._load_balance(flow_id, rpc_controller, request)
                        self._call_method_on_conn(conn, flow_id, method_descriptor,
                                                  rpc_controller, request, response_class,
                                                  done, is_async)

            else:  # TcpConnection.CLOSED
                if old_state == TcpConnection.CONNECTED:
                    self._good_connections.remove(conn)
                else:  # TcpConnection.UNHEALTHY
                    self._bad_connections.remove(conn)

            if len(self._good_connections) == 0:
                return
            else:
                self._connected_event.set()

            # choose load balancer
            if len(self._good_connections) == 1:
                self._balancer = loadbalance.SingleConnLoadBalancer()
            elif self._user_load_balancer is not None:
                self._balancer = self._user_load_balancer
            else:
                self._balancer = TcpChannel._default_load_balancer
            logging.info('use %s as load balancer' % self._balancer.__class__.__name__)
        except:
            assert False
            raise

    def resolve_addr(self, addr):
        if isinstance(addr, (list, set, tuple)):
            ip_ports = []
            for ad in addr:
                ads = self.resolve_addr(ad)
                if ads is None:
                    return None
                ip_ports += ads
            return ip_ports
        elif isinstance(addr, str):
            sep_index = addr.find('/')
            if sep_index == -1:  # cannot find '/', so treat it as ip port
                try:
                    ip, port = addr.split(':')
                except ValueError:
                    return None

                port = int(port)
                return [(ip, port)]
            else:
                addr_type = addr[:sep_index]
                addr_str = addr[sep_index + 1:]
                if addr_type == 'ip':
                    ip, port = addr_str.split(':')
                    port = int(port)
                    return [(ip, port)]
                elif addr_type == 'zk':
                    raise NotImplementedError
                else:
                    raise NotImplementedError

    def _call_method_on_conn(self, conn, flow_id, method_descriptor,
                             rpc_controller, request, response_class,
                             done, is_async=None):
        if is_async is not None:
            req_data = TcpConnection.RequestData(is_async, flow_id,
                                method_descriptor, rpc_controller,
                                request, response_class, done)
            conn.add_send_task(flow_id, method_descriptor, rpc_controller,
                               request, response_class, done, req_data)
            return None
        elif done is None:
            req_data = TcpConnection.RequestData(False, flow_id,
                                method_descriptor, rpc_controller,
                                request, response_class, done)
            res = gevent.event.AsyncResult()
            done = lambda _, rsp: res.set(rsp)
            conn.add_send_task(flow_id, method_descriptor, rpc_controller,
                               request, response_class, done, req_data)
            return res.get()
        else:
            req_data = TcpConnection.RequestData(True, flow_id,
                                method_descriptor, rpc_controller,
                                request, response_class, done)
            conn.add_send_task(flow_id, method_descriptor, rpc_controller,
                               request, response_class, done, req_data)
            return None

    def _load_balance(self, flow_id, rpc_controller, request):
        conn_flow_id = flow_id
        user_flow_id = rpc_controller.GetFlowId()
        balancer = self._balancer
        if user_flow_id is not None:
            conn_flow_id = user_flow_id
            balancer = TcpChannel._fixed_load_balancer
        conn = balancer.get_connection_for_req(conn_flow_id, request, self._good_connections)
        return conn

    # when done is None, it means the method call is synchronous
    # when it's not None, it means the call is asynchronous
    def CallMethod(self, method_descriptor, rpc_controller,
                   request, response_class, done):
        if len(self._all_connections) > \
                len(self._good_connections) + len(self._bad_connections):
            now = time.time()
            if now - self._last_connect_time >= self._connect_interval:
                # connect async
                logging.info('try reconnect')
                self._last_connect_time = now
                self._spawn(self._do_connect)

        if len(self._good_connections) == 0:
            rpc_controller.SetFailed((RpcController.CONN_FAILED_ERROR,
                                      'No connection is valid'))
            if done is not None:
                self._spawn(done, rpc_controller, None)
            return None

        flow_id = self._get_flow_id()
        conn = self._load_balance(flow_id, rpc_controller, request)

        return self._call_method_on_conn(conn, flow_id, method_descriptor,
                                         rpc_controller, request,
                                         response_class, done)


class RpcClient(object):

    tcp_channel_class = TcpChannel

    def __init__(self, pool_size=1000):
        self._channels = {}
        self._pool = Pool(pool_size)

    def __del__(self):
        self.close()

    def close(self):
        for channel in self._channels.itervalues():
            channel.close()
        self._channels.clear()

        self._pool.join()

    def get_tcp_channel(self, addr, is_wait_connected=True):
        if isinstance(addr, list):
            addr = tuple(addr)
        if addr not in self._channels:
            # if TcpConnection connects for a long time, and
            # other client use get_tcp_channel may cause problem.
            channel = RpcClient.tcp_channel_class(addr, spawn=self._pool.spawn)
            self._channels[addr] = channel

            # connect asynchronously
            self._pool.spawn(channel.connect)
        else:
            channel = self._channels[addr]
            if not channel.is_connected():
                self._pool.spawn(channel.connect)

        if is_wait_connected:
            channel.wait_until_connected()

        return channel


