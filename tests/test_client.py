from monsit import rpc
from test_proto import test_pb2
import sys
import gevent.event
import gevent
import psutil
import GreenletProfiler


def main():
    if len(sys.argv) < 3:
        print 'Usage: %s REQ_PER_SEC SERVER_ADDR' % sys.argv[0]
        sys.exit(1)

    p = psutil.Process()
    p.set_cpu_affinity([2])

    req_num, server_addr = int(sys.argv[1]), sys.argv[2]
    client = rpc.RpcClient()
    channel = client.get_tcp_channel(server_addr)
    stub = test_pb2.TestService_Stub(channel)

    event = gevent.event.Event()

    def timer():
        while True:
            event.set()
            gevent.sleep(1)

    gevent.spawn(timer)

    req = test_pb2.TestRequest(name='test_client', num=12345)
    sent_req_num = 0
    recv_rsp_num = [0]

    def done(_, rsp):
        if rsp:
            recv_rsp_num[0] += 1

    try:
        while True:
            event.wait()
            event.clear()
            print 'sent_req_num', sent_req_num, 'recv_rsp_num', recv_rsp_num[0]
            sent_req_num, recv_rsp_num[0] = 0, 0

            for _ in xrange(req_num):
                controller = rpc.RpcController()
                stub.TestMethod(controller, req, done)

            sent_req_num = req_num
    finally:
        client.close()


if __name__ == '__main__':
    GreenletProfiler.set_clock_type('cpu')
    GreenletProfiler.start()
    try:
        main()
    except KeyboardInterrupt:
        print 'client got SIGINT, exit.'
    GreenletProfiler.stop()
    stats = GreenletProfiler.get_func_stats()
    #stats.print_all()
    stats.save('client.profile', type='pstat')