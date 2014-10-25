import gevent
import gevent.queue
import gevent.pool


class Pool(object):
    def __init__(self, pool_size=None):
        self._task_queue = gevent.queue.JoinableQueue()
        self._pool = gevent.pool.Pool(pool_size)
        if pool_size is None:
            pool_size = 100

        for _ in xrange(pool_size):
            self._pool.spawn(self.worker_func)

    def worker_func(self):
        while True:
            task = self._task_queue.get()
            if task is None:
                self._task_queue.task_done()
                break
            task()
            self._task_queue.task_done()

    def spawn(self, func, *args, **kwargs):
        task = lambda: func(*args, **kwargs)
        self._task_queue.put_nowait(task)

    def join(self):
        for _ in xrange(len(self._pool)):
            self._task_queue.put_nowait(None)
        self._task_queue.join()
        self._pool.join()

    def kill(self):
        self._pool.kill()


def kill_all_but_this(greenlets):
    this_greenlet = gevent.getcurrent()
    for gl in greenlets:
        if gl is not this_greenlet:
            gl.kill(block=False)

