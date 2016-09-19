#!/usr/bin/env python

from supremm.summarize import Summarize
from supremm.plugin import loadplugins, loadpreprocessors

import json
from getopt import getopt
import sys
import os
import logging

def usage():
    """ print usage """
    print "usage: {0} /full/path/to/pcp/archive"

def getoptions():
    """ process comandline options """

    opts, args = getopt(sys.argv[1:], "dqh",
                     ["debug", 
                      "quiet", 
                      "help"])

    retdata = {"log": logging.INFO}

    for opt in opts:
        if opt[0] in ("-d", "--debug"):
            retdata['log'] = logging.DEBUG
        if opt[0] in ("-q", "--quiet"):
            retdata['log'] = logging.ERROR
        if opt[0] in ("-h", "--help"):
            usage()
            sys.exit(0)

    return (retdata, args)

class MockJob(object):
    def __init__(self, archivelist):
        self.node_archives = archivelist
        self.jobdir = os.path.dirname(archivelist[0])
        self.job_id = "1"
        self.end_str = "end"
        self.walltime = 9751
        self.nodecount = 1
        self.acct = {"end_time": 12312, "id": 1, "uid": "sdf", "user": "werqw"}
        self.nodes = ["node" + str(i) for i in xrange(len(archivelist))]
        self._data = {}

    def get_errors(self):
        return []

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
    if sys.version.startswith("2.7"):
        logging.captureWarnings(True)

    preprocs = loadpreprocessors()
    logging.debug("Loaded %s preprocessors", len(preprocs))

    plugins = loadplugins()
    logging.debug("Loaded %s plugins", len(plugins))

    archivelist = [args[0]]

    job = MockJob(archivelist)

    preprocessors = [x(job) for x in preprocs]
    analytics = [x(job) for x in plugins]

    s = Summarize(preprocessors, analytics, job)
    s.process()
    result = s.get()
    print json.dumps(result, indent=4)


if __name__ == "__main__":
    main()
