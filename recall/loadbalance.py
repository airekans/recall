import random


class LoadBalancerBase(object):
    def get_connection_for_req(self, flow_id, req, conns):
        raise NotImplementedError


class SingleConnLoadBalancer(LoadBalancerBase):
    def get_connection_for_req(self, flow_id, req, conns):
        assert len(conns) == 1
        return conns[0]


class FixedLoadBalancer(LoadBalancerBase):
    def get_connection_for_req(self, flow_id, req, conns):
        return conns[flow_id % len(conns)]


class RandomLoadBalancer(LoadBalancerBase):
    def get_connection_for_req(self, flow_id, req, conns):
        return random.choice(conns)


class ReqNumLoadBalancer(LoadBalancerBase):
    def get_conn_req_num(self, conn):
        return conn.get_pending_send_task_num()

    def get_connection_for_req(self, flow_id, req, conns):
        return min(conns, key=self.get_conn_req_num)


class DelayLoadBalancer(LoadBalancerBase):
    def __init__(self, is_random=True):
        self._call_times = 0
        self._is_random = is_random

    def get_conn_avg_delay(self, conn):
        return conn.get_avg_delay_per_min()

    def get_delay_index(self, conns):
        delay_idxs = []
        sum_delay = 0.0
        for conn in conns:
            delay = self.get_conn_avg_delay(conn)
            delay_index = 1.0 / delay if delay > 0.0 else 0.01
            if delay_index < 0.01:
                delay_index = 0.01
            sum_delay += delay_index
            delay_idxs.append(delay_index)

        return sum_delay, delay_idxs

    def get_connection_for_req(self, flow_id, req, conns):
        sum_delay, delay_idxs = self.get_delay_index(conns)
        rand_value = random.uniform(0, sum_delay)
        for i, delay in enumerate(delay_idxs):
            rand_value -= delay
            if rand_value <= 0.0:
                return conns[i]

        # should not reach here
        return random.choice(conns)

