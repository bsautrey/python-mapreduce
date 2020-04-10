# profiler.py is the helper class for aggregate some profile information for mapper, shuffler and reducer.

import psutil
from configs_parser import get_configs


class Profiler:

    def __init__(self, server_id, job_name):
        # configs
        configs = get_configs('profiler')
        self.ram_usage_threshold = configs['ram_usage_threshold']

        # internal variables from child classes
        self.job_name = job_name
        self.server_id = server_id

    def check_memory_usage(self):
        try:
            ram_usage = psutil.virtual_memory().percent
        except:
            ram_usage = 0

        if ram_usage > self.ram_usage_threshold:
            msg = "Worker exceeded allowed memory usage %s%% (threshold %s%%). Server %s, job name: %s" % \
                  (ram_usage, self.ram_usage_threshold, self.server_id, self.job_name)
            raise Exception(msg)
