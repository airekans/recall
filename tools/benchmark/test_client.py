from recall import rpc
from proto import test_pb2
import gevent.event
import gevent
import psutil
import logging
import time
import greenprofile
import optparse


def main():
    opt_parser = optparse.OptionParser(usage="%prog [options] REQ_PER_SEC "
                                       "SERVER_ADDR [SERVER_ADDR1...]")
    opt_parser.add_option('--profile', dest="is_profile",
                          help="Turn on the profile", action="store_true",
                          default=False)
    opt_parser.add_option('--cpu-affinity', dest="cpu_affinity",
                          help="Set the CPU affinity", type=int)
    opt_parser.add_option('--runtime', dest="runtime",
                          help="Run specified time and exit", type=int)

    opts, args = opt_parser.parse_args()

    if len(args) < 2:
        opt_parser.error('please enter REQ_PER_SEC SERVER_ADDR')

    req_num = args[0]
    try:
        req_num = int(req_num)
    except ValueError:
        opt_parser.error('please enter correct REQ_PER_SEC')

    server_addrs = args[1:]

    print 'connecting to %s' % str(server_addrs)

    logging.basicConfig(level=logging.DEBUG)

    if opts.cpu_affinity is not None:
        p = psutil.Process()
        p.set_cpu_affinity([opts.cpu_affinity])

    with greenprofile.Profiler(opts.is_profile, 'client.profile'):
        client = rpc.RpcClient()
        channel = client.get_tcp_channel(server_addrs)
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

        start_time = time.time()
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
                if opts.runtime is not None:
                    now = time.time()
                    if now - start_time >= opts.runtime:
                        break

        finally:
            client.close()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print 'client got SIGINT, exit.'

