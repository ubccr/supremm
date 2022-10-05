import json
from getopt import getopt
import sys
import os
import requests
import time
import traceback
import logging

from supremm import outputter
from supremm.config import Config
from supremm.proc_common import filter_plugins, instantiatePlugins
from supremm.plugin import loadpreprocessors, loadplugins
from supremm.scripthelpers import setuplogger
from supremm.xdmodaccount import XDMoDAcct

from promsummarize import PromSummarize


def usage():
    """ print usage """
    print("usage: {0} \"<prometheus-ip>:<port>\"")

def getoptions():
    """ process comandline options """

    opts, args = getopt(sys.argv[1:], "dqhi:e:c:j:a:", [
        "debug",
        "quiet",
        "help",
        "plugin-include",
        "plugin-exclude",
        "config="
    ])

    retdata = {
        "log": logging.INFO,
        "plugin_whitelist": [],
        "plugin_blacklist": [],
        "config": None
    }

    for opt, arg in opts:
        if opt in ("-d", "--debug"):
            retdata['log'] = logging.DEBUG
        if opt in ("-q", "--quiet"):
            retdata['log'] = logging.ERROR
        if opt in ("-i", "--plugin-include"):
            retdata['plugin_whitelist'].append(arg)
        if opt in ("-e", "--plugin-exclude"):
            retdata['plugin_blacklist'].append(arg)
        if opt in ("-c", "--config"):
            retdata['config'] = arg
        if opt in ("-h", "--help"):
            usage()
            sys.exit(0)

    return (retdata, args)

class MockPromJob():
    def __init__(self):
        self.job_id = "2007"
        self.end_str = "end"
        self.walltime = 9751
        self.nodecount = 1
        self.acct = {"end_time": 12312, "id": 1, "uid": 0, "user": "root", "partition": "normal", "local_job_id": "2007", "resource_manager": "slurm"}  
        self.nodes = ["prometheus-dev"]

        self._data = {}
        self._errors = [] 

        self.start_datetime = "2022-07-01T14:36:09.000Z"
        self.end_datetime = "2022-07-04T15:35:25.000Z"

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
        return "{} {} {}".format(self.job_id, self.walltime, self.nodes)

def summarizejobprom(job, plugins, preprocs):
    mdata = {}
    summarizeerror = None

    # Instantiate plugins by job's available metrics (PCP naming)
    preprocs = [x(job) for x in preprocs]
    plugins = [x(job) for x in plugins]

    s = PromSummarize(preprocs, plugins, job)
    s.process()

    return s, mdata, True, summarizeerror

def main():
    """
    Main entry point for script
    """
    config = Config()
    opts, args = getoptions()

    setuplogger('INFO')    

    preprocs = loadpreprocessors()
    plugins = loadplugins()

    if opts['plugin_whitelist']:
        preprocs, plugins = filter_plugins({"plugin_whitelist": opts['plugin_whitelist']}, preprocs, plugins)
    elif opts['plugin_blacklist']:
        preprocs, plugins = filter_plugins({"plugin_blacklist": opts['plugin_blacklist']}, preprocs, plugins)

    logging.debug("Loaded %s preprocessors", len(preprocs))
    logging.debug("Loaded %s plugins", len(plugins))

    #with outputter.factory(config, resconf, dry_run=opts["dry_run"]) as m:
    dbif = XDMoDAcct('11', config)

    # Test on single job with walltime > 3 days and greatest node count (13)
    for job in dbif.getbylocaljobid('8970792'):
        try:
            summarize_start = time.time()
            res = summarizejobprom(job, plugins, preprocs)
            s, mdata, success, s_err = res
            summarize_time = time.time() - summarize_start
            summary_dict = s.get()
            print(json.dumps(summary_dict, indent=4, default=str))
        except Exception as e:
            logging.error("Failure for summarization of job %s %s. Error: %s %s", job.job_id, job.jobdir, str(e), traceback.format_exc())
    #process_summary(m, dbif, opts, job, summarize_time, (summary_dict, mdata, success, s_err))

if __name__ == "__main__":
    main()
