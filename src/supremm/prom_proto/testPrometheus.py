import os
import json
import requests
import time
import logging

import prometheus_api_client as pac

from supremm.proc_common import filter_plugins, instantiatePlugins
from supremm.plugin import loadpreprocessors, loadplugins

from promsummarize import PromSummarize


class MockPromJob():
    def __init__(self):
        self.job_id = "1"
        self.end_str = "end"
        self.walltime = 9751
        self.nodecount = 1
        self.acct = {"end_time": 12312, "id": 1, "uid": 0, "user": "root", "partition": "normal", "local_job_id": "1008", "resource_manager": "slurm"}  
        self.nodes = ["prometheus-dev"]

        self._data = {}
        self._errors = [] 

        self.start_datetime = "2022-04-05T09:30:00.781Z"
        self.end_datetime = "2022-04-05T09:35:00.781Z" 

    def get_errors(self):
        """ return job errors """
        return self._errors

    def data(self):
        """ Add job metadata """
        return self._data

    def adddata(self, name, data):
        """ Add job metadata """
        self._data[name] = data

    def getdata(self, name):
        """ return job metadata for name """
        if name in self._data:
            return self._data[name]
        return None

    def __str__(self):
        return "{} {} {} {}".format(self.job_id, self.walltime, self.nodes, self.node_archives)

def main():
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S', level=logging.DEBUG)
    logging.captureWarnings(True)
    
    preprocs = loadpreprocessors()
    plugins = loadplugins()

    preprocs, plugins = filter_plugins({"plugin_whitelist": ['CpuUsage', 'CpuPerfCounters']}, preprocs, plugins)

    logging.debug("Loaded %s preprocessors", len(preprocs))
    logging.debug("Loaded %s plugins", len(plugins))

    job = MockPromJob()
    
    # Instantiate plugins by job's available metrics (PCP naming) 
    #preprocs = [x(job) for x in preprocs]
    plugins = [x(job) for x in plugins]  

    s = PromSummarize(plugins, job)
    s.process()
    #s.series_meta("2022-04-05T09:30:00.781Z", "2022-04-05T09:35:00.781Z")
    result = s.get()
    print(json.dumps(result, indent=4))

if __name__ == "__main__":
    main()
