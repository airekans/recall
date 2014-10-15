import GreenletProfiler


class Profiler(object):
    def __init__(self, is_start, profile_file):
        self._profile_file = profile_file
        self._is_start = is_start

    def __enter__(self):
        if self._is_start:
            GreenletProfiler.set_clock_type('cpu')
            GreenletProfiler.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self._is_start:
            GreenletProfiler.stop()
            stats = GreenletProfiler.get_func_stats()
            #stats.print_all()
            stats.save(self._profile_file, type='pstat')

