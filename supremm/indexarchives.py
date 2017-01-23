#!/usr/bin/env python
""" Script that indexes the pc archives for a given resource.
"""

import logging
from pcp import pmapi
import cpmapi as c_pmapi
import time

from supremm.config import Config
from supremm.scripthelpers import parsetime, setuplogger

from supremm.account import DbArchiveCache
from supremm.xdmodaccount import XDMoDArchiveCache

import sys
import os
from datetime import datetime, timedelta
from getopt import getopt
import re

def archive_cache_factory(resconf, config):
    """ Return the implementation of the accounting class for the resource """
    atype = resconf['batch_system']
    if atype == "XDMoD":
        return XDMoDArchiveCache(config)
    else:
        return DbArchiveCache(config)


class PcpArchiveProcessor(object):
    """ Parses a pcp archive and adds the archive information to the index """

    def __init__(self, config, resconf):
        self.resource_id = resconf['resource_id']
        self.hostname_mode = resconf['hostname_mode']
        if self.hostname_mode == "fqdn":
            self.hostnameext = resconf['host_name_ext']
        self.dbac = archive_cache_factory(resconf, config)

    @staticmethod
    def parsejobid(archivename):
        """ Archives that are created at job start and end have the jobid encoded
            in the filename.
        """
        jobid = None
        fname = os.path.basename(archivename)
        if fname.startswith("job-"):
            try:
                jobid = fname.split("-")[1]
            except KeyError:
                jobid = None

        return jobid

    def processarchive(self, archive):
        """ Try to open the pcp archive and extract the timestamps of the first and last
            records and hostname. Store this in the DbArchiveCache
        """
        try:
            context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, archive)
            mdata = context.pmGetArchiveLabel()
            hostname = mdata.hostname
            if self.hostname_mode == "fqdn":
                # The fully qualiifed domain name uniqly identifies the host. Ensure to
                # add it if it is missing
                if self.hostnameext != "" and (not hostname.endswith(self.hostnameext)):
                    hostname += "." + self.hostnameext
            elif self.hostname_mode == "hostname":
                # The full domain name is ignored and only the hostname part matters
                # to uniquely identify a node
                hostname = mdata.hostname.split(".")[0]

            jobid = self.parsejobid(archive)

            self.dbac.insert(self.resource_id, hostname, archive[:-6],
                             float(mdata.start), float(context.pmGetArchiveEnd()), jobid)

            logging.debug("processed archive %s", archive)

        except pmapi.pmErr as exc:
            logging.error("archive %s. %s", archive, exc.message())

    def close(self):
        """ cleanup and close the connection """
        self.dbac.postinsert()


class PcpArchiveFinder(object):
    """ Helper class that finds all pcp archive files in a directory
        mindate is the minimum datestamp of files that should be processed
    """

    def __init__(self, mindate, maxdate):
        self.mindate = mindate
        self.maxdate = maxdate
        if self.mindate != None:
            self.minmonth = datetime(year=mindate.year, month=mindate.month, day=1) - timedelta(days=1)
        else:
            self.minmonth = None
        self.fregex = re.compile(
            r".*(\d{4})(\d{2})(\d{2})(?:\.(\d{2}).(\d{2})(?:[\.-](\d{2}))?)?\.index$")
        self.sregex = re.compile(r"^(\d{4})(\d{2})$")

    def subdirok(self, subdir):
        """ check the name of a subdirectory and return whether to
            descend into it based on the name.
            @returns None if the name is not a datestamp
                     true if the name is a date that is >= the reference date
                     false if the name is a date that is < the reference
        """
        mtch = self.sregex.match(subdir)
        if mtch == None:
            return None

        if self.minmonth == None:
            return True

        subdirdate = datetime(year=int(mtch.group(1)), month=int(mtch.group(2)), day=1)

        return subdirdate > self.minmonth

    def filenameok(self, filename):
        """ parse filename to get the datestamp and compare with the reference datestamp
        """
        if self.mindate == None:
            return True

        mtch = self.fregex.match(filename)
        if mtch == None:
            logging.error(
                "Unparsable filename %s processing anyway.", filename)
            return True

        if mtch.group(4) != None and mtch.group(5) != None:
            filedate = datetime(year=int(mtch.group(1)), month=int(mtch.group(2)), day=int(mtch.group(3)), hour=int(mtch.group(4)), minute=int(mtch.group(5)))
        else:
            filedate = datetime(year=int(mtch.group(1)), month=int(mtch.group(2)), day=int(mtch.group(3)))

        if self.maxdate == None:
            return filedate > self.mindate
        else:
            return filedate > self.mindate and filedate < self.maxdate

    def find(self, topdir):
        """  find all archive files in topdir """
        if topdir == "":
            return

        hosts = os.listdir(topdir)

        starttime = time.time()
        hostcount = 0
        currtime = starttime

        for hostname in hosts:
            hostdir = os.path.join(topdir, hostname)
            t1 = time.time()
            datdirs = os.listdir(hostdir)
            t2 = time.time()
            for datedir in datdirs:

                datedirOk = self.subdirok(datedir)
                if datedirOk == None:
                    t3 = t2
                    t4 = t2
                    if datedir.endswith(".index") and self.filenameok(datedir):
                        yield os.path.join(hostdir, datedir)
                elif datedirOk == True:
                    dirpath = os.path.join(hostdir, datedir)
                    t3 = time.time()
                    filenames = os.listdir(dirpath)
                    t4 = time.time()
                    for filename in filenames:
                        if filename.endswith(".index") and self.filenameok(filename):
                            yield os.path.join(dirpath, filename)

            hostcount += 1
            lasttime = currtime
            currtime = time.time()
            logging.info("Processed %s of %s (last %s = (%s + %s +) total %s estimated completion %s",
                         hostcount, len(hosts), currtime-lasttime, t2-t1, t4-t3, currtime - starttime,
                         datetime.fromtimestamp(starttime) + timedelta(seconds=(currtime - starttime) / hostcount * len(hosts)))


DAY_DELTA = 3


def usage():
    """ print usage """
    print "usage: {0} [OPTS]".format(os.path.basename(__file__))
    print "  -r --resource=RES    process only archive files for the specified resource,"
    print "                       if absent then all resources are processed"
    print "  -c --config=PATH     specify the path to the configuration directory"
    print "  -m --mindate=DATE    specify the minimum datestamp of archives to process"
    print "                       (default", DAY_DELTA, "days ago)"
    print "  -M --maxdate=DATE    specify the maximum datestamp of archives to process"
    print "                       (default now())"
    print "  -D --debugfile       specify the path to a log file. If this option is"
    print "                       present then the process will log a DEBUG level to this"
    print "                       file. This logging is independent of the console log."
    print "  -a --all             process all archives regardless of age"
    print "  -d --debug           set log level to debug"
    print "  -q --quiet           only log errors"
    print "  -h --help            print this help message"


def getoptions():
    """ process comandline options """

    retdata = {
        "log": logging.INFO,
        "resource": None,
        "config": None,
        "debugfile": None,
        "mindate": datetime.now() - timedelta(days=DAY_DELTA),
        "maxdate": datetime.now() - timedelta(minutes=10)
    }

    opts, _ = getopt(sys.argv[1:], "r:c:m:M:D:adqh", ["resource=", "config=", "mindate=", "maxdate=", "debugfile", "all", "debug", "quiet", "help"])

    for opt in opts:
        if opt[0] in ("-r", "--resource"):
            retdata['resource'] = opt[1]
        elif opt[0] in ("-d", "--debug"):
            retdata['log'] = logging.DEBUG
        elif opt[0] in ("-q", "--quiet"):
            retdata['log'] = logging.ERROR
        elif opt[0] in ("-c", "--config"):
            retdata['config'] = opt[1]
        elif opt[0] in ("-m", "--mindate"):
            retdata['mindate'] = parsetime(opt[1])
        elif opt[0] in ("-M", "--maxdate"):
            retdata['maxdate'] = parsetime(opt[1])
        elif opt[0] in ("-D", "--debugfile"):
            retdata["debugfile"] = opt[1]
        elif opt[0] in ("-a", "--all"):
            retdata['mindate'] = None
            retdata['maxdate'] = None
        elif opt[0] in ("-h", "--help"):
            usage()
            sys.exit(0)

    return retdata


def runindexing():
    """ main script entry point """
    opts = getoptions()

    setuplogger(opts['log'], opts['debugfile'], logging.DEBUG)

    config = Config(opts['config'])

    logging.info("archive indexer starting")

    for resourcename, resource in config.resourceconfigs():

        if opts['resource'] in (None, resourcename, str(resource['resource_id'])):

            acache = PcpArchiveProcessor(config, resource)
            afind = PcpArchiveFinder(opts['mindate'], opts['maxdate'])

            for archivefile in afind.find(resource['pcp_log_dir']):
                acache.processarchive(archivefile)

            acache.close()

    logging.info("archive indexer complete")

if __name__ == "__main__":
    runindexing()
