import google.protobuf


class RpcController(google.protobuf.service.RpcController):
    SUCCESS = 0
    SERVICE_TIMEOUT = 100
    WRONG_FLOW_ID_ERROR = 101
    WRONG_RSP_META_ERROR = 102
    WRONG_MSG_NAME_ERROR = 103
    SERVER_CLOSE_CONN_ERROR = 104
    CONN_FAILED_ERROR = 105

    def __init__(self, method_timeout=0, flow_id=None):
        self.err_code = RpcController.SUCCESS
        self.err_msg = None
        self.method_timeout = method_timeout
        self._flow_id = flow_id

    def Reset(self):
        self.err_code = RpcController.SUCCESS
        self.err_msg = None

    def Failed(self):
        return self.err_code != RpcController.SUCCESS

    def ErrorText(self):
        return self.err_msg

    def SetFailed(self, reason):
        self.err_code, self.err_msg = reason

    def SetFlowId(self, flow_id):
        assert isinstance(flow_id, int)
        self._flow_id = flow_id

    def GetFlowId(self):
        return self._flow_id

