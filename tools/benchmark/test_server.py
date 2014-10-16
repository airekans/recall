from recall import rpc
from proto import test_pb2
import psutil
import greenprofile
import optparse


class TestServiceImpl(test_pb2.TestService):
    def TestMethod(self, rpc_controller, request, done):
        rsp = test_pb2.TestResponse(return_code=0, msg='SUCCESS')
        return rsp


def main():
    opt_parser = optparse.OptionParser(usage="%prog [options]")
    opt_parser.add_option('--port', dest="server_port",
                          help="Port of the server", type=int,
                          default=54321)
    opt_parser.add_option('--profile', dest="is_profile",
                          help="Turn on the profile", action="store_true",
                          default=False)
    opt_parser.add_option('--cpu-affinity', dest="cpu_affinity",
                          help="Set the CPU affinity", type=int)

    opts, _ = opt_parser.parse_args()

    server_port = opts.server_port
    is_profile = opts.is_profile

    addr = ('0.0.0.0', server_port)

    if opts.cpu_affinity is not None:
        p = psutil.Process()
        p.set_cpu_affinity([opts.cpu_affinity])

    with greenprofile.Profiler(is_profile, 'server.profile'):
        print 'server listen at %s' % str(addr)
        server = rpc.RpcServer(addr)
        server.register_service(TestServiceImpl())
        try:
            server.run(print_stat_interval=60)
        except KeyboardInterrupt:
            print 'server got SIGINT, exit.'


if __name__ == '__main__':
    main()

