from _socket import error as soc_error
import logging
import struct
import traceback
import gevent
from gevent import Timeout
from recall.codec import serialize_message, parse_message, parse_meta
from recall.controller import RpcController
from recall.proto import rpc_meta_pb2
from recall.util import Pool


class RpcServerStat(object):

    class MethodStat(object):
        def __init__(self):
            self.call_num_per_min = 0
            self.total_call_num = 0

        def reset_stat_per_min(self):
            self.call_num_per_min = 0

    class ServiceStat(object):
        def __init__(self):
            self.call_num_per_min = 0
            self.total_call_num = 0
            self.method_stats = {}

        def add_method_stat(self, method, count):
            try:
                method_stat = self.method_stats[method]
            except KeyError:
                method_stat = RpcServerStat.MethodStat()
                self.method_stats[method] = method_stat

            method_stat.call_num_per_min += count
            method_stat.total_call_num += count

        def reset_stat_per_min(self):
            self.call_num_per_min = 0
            for method_stat in self.method_stats.itervalues():
                method_stat.reset_stat_per_min()

    def __init__(self):
        self.service_stats = {}

    def add_method_stat(self, service, method, count):
        try:
            service_stat = self.service_stats[service]
        except KeyError:
            service_stat = RpcServerStat.ServiceStat()
            self.service_stats[service] = service_stat

        service_stat.call_num_per_min += count
        service_stat.total_call_num += count
        service_stat.add_method_stat(method, count)

    def print_stat(self):
        print 'name\t| total_count\t| count/60s'

        for service_name, service_stat in self.service_stats.iteritems():
            print '%s\t| %d\t| %d' % (service_name, service_stat.total_call_num,
                                      service_stat.call_num_per_min)
            for method_name, method_stat in service_stat.method_stats.iteritems():
                print '   %s\t| %d\t| %d' % (method_name, method_stat.total_call_num,
                                             method_stat.call_num_per_min)

        print ''

    def reset_stat_per_min(self):
        for service_stat in self.service_stats.itervalues():
            service_stat.reset_stat_per_min()


class BuiltinServiceImpl(rpc_meta_pb2.BuiltinService):
    def HeartBeat(self, rpc_controller, request, done):
        rsp = rpc_meta_pb2.HeartBeatResponse()
        if request.magic_num == 4321:
            rsp.return_code = 0
        else:
            rsp.return_code = 1

        return rsp


class RpcServer(object):
    def __init__(self, addr, service_timeout=10, spawn=1000):
        if isinstance(addr, str):
            self._addr = addr.split(':')  # addr string like '127.0.0.1:30006'
            self._addr[1] = int(self._addr[1])
        else:
            self._addr = addr

        if isinstance(spawn, (int, long)):
            self._pool = Pool(spawn)
            self._spawn = self._pool.spawn
        else:
            self._pool = None
            self._spawn = spawn

        self._services = {}
        self._service_timeout = service_timeout
        self._stat = RpcServerStat()
        self._stream_server = gevent.server.StreamServer(self._addr,
                                                         self._handle_connection)

        self._register_builtin_services()

    def _handle_connection(self, socket, addr):
        socket.setsockopt(gevent.socket.SOL_TCP, gevent.socket.TCP_NODELAY, 1)
        socket.setsockopt(gevent.socket.IPPROTO_TCP, gevent.socket.TCP_NODELAY, 1)

        rsp_queue = gevent.queue.Queue()
        is_connection_closed = [False]

        def call_service(req_info):
            meta_info, service, method, req = req_info
            self._stat.add_method_stat(meta_info.service_name,
                                       meta_info.method_name, 1)

            controller = RpcController()
            try:
                with Timeout(self._service_timeout):
                    rsp = service.CallMethod(method, controller, req, None)
            except Timeout:
                meta_info.has_error = True
                rsp = rpc_meta_pb2.ErrorResponse(err_code=rpc_meta_pb2.SERVER_SERVICE_TIMEOUT,
                                                 err_msg='service timeout')
            except:
                meta_info.has_error = True
                err_msg = 'Error calling service: ' + traceback.format_exc()
                rsp = rpc_meta_pb2.ErrorResponse(err_code=rpc_meta_pb2.SERVER_SERVICE_ERROR,
                                                 err_msg=err_msg)

            rsp_queue.put_nowait((meta_info, rsp))

        def recv_req():
            content = ""
            while True:
                try:
                    recv_buf = socket.recv(1024)
                    if len(recv_buf) == 0:
                        break
                except Exception, e:
                    logging.warning('recv_req error: ' + str(e))
                    break

                content += recv_buf
                mem_content = memoryview(content)
                cur_index = 0
                while cur_index < len(content):
                    if len(mem_content[cur_index:]) < 6:
                        break
                    elif mem_content[cur_index:cur_index + 2] != 'PB':
                        cur_index += 2  # skip the first 2 bytes
                        break

                    (buf_size,) = struct.unpack('!I',
                                                mem_content[cur_index + 2: cur_index + 6].tobytes())
                    if len(mem_content[cur_index + 6:]) < buf_size:
                        break

                    pb_buf = mem_content[cur_index + 6: cur_index + 6 + buf_size].tobytes()
                    cur_index += buf_size + 6
                    result = self.parse_message(pb_buf)
                    if result is None:
                        logging.warning('pb decode error, skip this message')
                        break

                    self._spawn(call_service, result)

                if cur_index > 0:
                    content = content[cur_index:]

            logging.info(str(addr) + 'has disconnected')
            is_connection_closed[0] = True

        def send_rsp():
            while not is_connection_closed[0]:
                try:
                    meta_info, rsp = rsp_queue.get(timeout=1)
                except gevent.queue.Empty:
                    continue

                serialized_rsp = serialize_message(meta_info, rsp)
                sent_bytes = 0
                try:
                    while sent_bytes < len(serialized_rsp):
                        sent_bytes += socket.send(serialized_rsp[sent_bytes:])
                except soc_error as e:
                    logging.warning('socket send error: ' + str(e))
                    break

        workers = [gevent.spawn(recv_req), gevent.spawn(send_rsp)]
        gevent.joinall(workers)

    def parse_message(self, buf):
        result = parse_meta(buf)
        if result is None:
            return None
        meta_len, pb_msg_len, meta_info = result

        # try to find the service
        try:
            service = self._services[meta_info.service_name]
        except KeyError:
            logging.warning('cannot find the service: ' + meta_info.service_name)
            return None

        method = service.GetDescriptor().FindMethodByName(meta_info.method_name)
        if method is None:
            logging.warning('cannot find the method: ' + meta_info.method_name)
            return None

        msg = parse_message(buf[8 + meta_len:8 + meta_len + pb_msg_len],
                             service.GetRequestClass(method))
        if msg is None:
            return None
        else:
            return meta_info, service, method, msg

    def register_service(self, service):
        self._services[service.GetDescriptor().full_name] = service

    def _register_builtin_services(self):
        self.register_service(BuiltinServiceImpl())

    def print_stat(self, interval):
        while True:
            gevent.sleep(interval)
            self._stat.print_stat()
            self._stat.reset_stat_per_min()

    def run(self, print_stat_interval=None):
        if print_stat_interval is not None and print_stat_interval > 0:
            stat_worker = gevent.spawn(self.print_stat, print_stat_interval)

        try:
            self._stream_server.serve_forever()
        finally:
            stat_worker.kill()
            if self._pool is not None:
                self._pool.join()