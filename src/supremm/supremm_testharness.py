#!/usr/bin/env python
""" Summarization software test harness

    This test harness provides a convienient way to test individual plugins and
    preprocessors.
"""

import json
from getopt import getopt
import sys
import os
import logging
import datetime
import math

from pcp import pmapi
import cpmapi as c_pmapi

from supremm.summarize import Summarize
from supremm.plugin import loadplugins, loadpreprocessors
from supremm.config import Config
from supremm.proc_common import filter_plugins

def usage():
    """ print usage """
    print "usage: {0} /full/path/to/pcp/archive"

def getoptions():
    """ process comandline options """

    opts, args = getopt(sys.argv[1:], "dqhi:e:c:", [
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

class MockJob(object):
    """ Object that has the same external API as the Job object """
    def __init__(self, archivelist):
        self.node_archives = archivelist
        self.jobdir = os.path.dirname(archivelist[0])
        self.job_id = "1"
        self.end_str = "end"
        self.walltime = 9751
        self.nodecount = len(archivelist)
        self.acct = {"end_time": 12312, "id": 1, "uid": "sdf", "user": "werqw", "partition": "test", "local_job_id": "1234", "resource_manager": "slurm"}
        self.nodes = ["node" + str(i) for i in xrange(len(archivelist))]
        self._data = {}
        self._errors = []

        archive_starts = []
        archive_ends = []
        for archive in archivelist:
            context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, archive)
            mdata = context.pmGetArchiveLabel()
            archive_starts.append(datetime.datetime.utcfromtimestamp(math.floor(mdata.start)))
            archive_ends.append(datetime.datetime.utcfromtimestamp(math.ceil(context.pmGetArchiveEnd())))

        self.start_datetime = min(archive_starts)
        self.end_datetime = max(archive_ends)


    def get_errors(self):
        """ return job errors """
        return self._errors

    def data(self):
        """ return all job metadata """
        return self._data

    def adddata(self, name, data):
        """ Add job metadata """
        self._data[name] = data

    def getdata(self, name):
        """ return job metadata for name """
        if name in self._data:
            return self._data[name]
        return None

    def nodearchives(self):
        """ generator for node archive information """
        i = 0
        for filename in self.node_archives:
            yield ("node" + str(i), i, filename)
            i += 1

    def __str__(self):
        return "{} {} {} {}".format(self.job_id, self.walltime, self.nodes, self.node_archives)



def main():
    """
    main entry point for script
    """
    opts, args = getoptions()

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S', level=opts['log'])
    logging.captureWarnings(True)

    preprocs = loadpreprocessors()
    plugins = loadplugins()

    if opts['plugin_whitelist']:
        preprocs, plugins = filter_plugins({"plugin_whitelist": opts['plugin_whitelist']}, preprocs, plugins)
    elif opts['plugin_blacklist']:
        preprocs, plugins = filter_plugins({"plugin_blacklist": opts['plugin_blacklist']}, preprocs, plugins)

    logging.debug("Loaded %s preprocessors", len(preprocs))
    logging.debug("Loaded %s plugins", len(plugins))

    archivelist = args

    job = MockJob(archivelist)
    config = Config(confpath=opts['config'])

    preprocessors = [x(job) for x in preprocs]
    analytics = [x(job) for x in plugins]

    s = Summarize(preprocessors, analytics, job, config)
    s.process()
    result = s.get()
    print json.dumps(result, indent=4, default=str)


if __name__ == "__main__":
    main()
