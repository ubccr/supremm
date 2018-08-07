#!/usr/bin/env python
""" Script that indexes the pc archives for a given resource.
"""

import logging
import math

import pytz
import tzlocal
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
from multiprocessing import Pool
import functools
import tempfile
import csv
import itertools


def datetime_to_timestamp(dt):
    return (dt - datetime.utcfromtimestamp(0)).total_seconds()


JOB_ARCHIVE_RE = re.compile(
    "job-(\d+)-(?:begin|end)-(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})\.(?P<hour>\d{2})\.(?P<minute>\d{2})\.(?P<second>\d{2})"
)


class TimezoneAdjuster(object):
    def __init__(self, timezone_name, guess_early=True):
        self.timezone = pytz.timezone(timezone_name) if timezone_name is not None else tzlocal.get_localzone()
        self.guess_early = guess_early

    def adjust(self, dt):
        timestamp = datetime_to_timestamp(dt)
        try:
            return timestamp - self.timezone.utcoffset(dt).total_seconds()
        except pytz.exceptions.AmbiguousTimeError:
            return timestamp - self.timezone.utcoffset(dt, self.guess_early).total_seconds()


class PcpArchiveProcessor(object):
    """ Parses a pcp archive and adds the archive information to the index """

    def __init__(self, resconf):
        self.hostname_mode = resconf['hostname_mode']
        if self.hostname_mode == "fqdn":
            self.hostnameext = resconf['host_name_ext']
        self.tz_adjuster = TimezoneAdjuster(resconf.get("timezone"))

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

    def processarchive(self, archive, fast_index, host_from_path=None):
        """ Try to open the pcp archive and extract the timestamps of the first and last
            records and hostname. Store this in the DbArchiveCache
        """
        start_timestamp = None
        if fast_index:
            start_timestamp = self.get_archive_data_fast(archive)

        if start_timestamp is not None:
            hostname = host_from_path
            end_timestamp = start_timestamp

        else:
            # fallback implementation that opens the archive
            try:
                context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, archive)
                mdata = context.pmGetArchiveLabel()
                hostname = mdata.hostname
                start_timestamp = float(mdata.start)
                end_timestamp = float(context.pmGetArchiveEnd())
            except pmapi.pmErr as exc:
                #pylint: disable=not-callable
                logging.error("archive %s. %s", archive, exc.message())
                return None


        if self.hostname_mode == "fqdn":
            # The fully qualiifed domain name uniqly identifies the host. Ensure to
            # add it if it is missing
            if self.hostnameext != "" and (not hostname.endswith(self.hostnameext)):
                hostname += "." + self.hostnameext
        elif self.hostname_mode == "hostname":
            # The full domain name is ignored and only the hostname part matters
            # to uniquely identify a node
            hostname = hostname.split(".")[0]

        jobid = self.parsejobid(archive)

        return hostname, archive[:-6], start_timestamp, end_timestamp, jobid

    def get_archive_data_fast(self, arch_path):
        # return None
        # TODO: option
        arch_name = os.path.basename(arch_path)
        match = JOB_ARCHIVE_RE.match(arch_name)
        if not match:
            return None

        date_dict = {k: int(v) for k, v in match.groupdict().iteritems()}
        start_datetime = datetime(**date_dict)
        return self.tz_adjuster.adjust(start_datetime)


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

    def ymdok(self, year, month=12, day=None):
        """ Check candidate dates for YYYY/MM/DD directory structure """
        if len(year) != 4:
            return None

        try:
            yyyy = int(year)
            mm = int(month)

            if day == None:
                # Some datetime arithmetic to get the last day of the month
                tmpdate = datetime(year=yyyy, month=mm, day=28, hour=23, minute=59, second=59) + timedelta(days=4)
                dirdate = tmpdate - timedelta(days=tmpdate.day)
            else:
                dirdate = datetime(year=yyyy, month=mm, day=int(day), hour=23, minute=59, second=59)

        except ValueError:
            return None

        if self.mindate == None:
            return True

        return dirdate > self.mindate

    @staticmethod
    def listdir(pathname):
        """ Return sorted list of paths under the supplied path. I/O errors
            such as permission denied are logged at error level and an empty
            list is returned """

        dirents = []
        try:
            dirents = os.listdir(pathname)
        except OSError as err:
            logging.error(str(err))

        dirents.sort()

        return dirents

    def find(self, topdir):
        """  find all archive files in topdir """
        if topdir == "":
            return

        hosts = self.listdir(topdir)

        starttime = time.time()
        hostcount = 0
        currtime = starttime

        for hostname in hosts:
            hostdir = os.path.join(topdir, hostname)
            listdirtime = 0.0
            yieldtime = 0.0
            t1 = time.time()
            datedirs = self.listdir(hostdir)
            listdirtime += (time.time() - t1)

            for datedir in datedirs:
                t1 = time.time()

                yeardirOk = self.ymdok(datedir)

                if yeardirOk is True:
                    for monthdir in self.listdir(os.path.join(hostdir, datedir)):
                        if self.ymdok(datedir, monthdir) is True:
                            for daydir in self.listdir(os.path.join(hostdir, datedir, monthdir)):
                                if self.ymdok(datedir, monthdir, daydir) is True:
                                    for filename in self.listdir(os.path.join(hostdir, datedir, monthdir, daydir)):
                                        if filename.endswith(".index") and self.filenameok(filename):
                                            beforeyield = time.time()
                                            yield os.path.join(hostdir, datedir, monthdir, daydir, filename), True, hostname
                                            yieldtime += (time.time() - beforeyield)
                    listdirtime += (time.time() - t1 - yieldtime)
                    continue
                elif yeardirOk is False:
                    continue
                # else fall through to check other formats
                elif yeardirOk is None:
                    datedirOk = self.subdirok(datedir)
                    if datedirOk is None:
                        if datedir.endswith(".index") and self.filenameok(datedir):
                            yield os.path.join(hostdir, datedir), False, None
                    elif datedirOk is True:
                        dirpath = os.path.join(hostdir, datedir)
                        filenames = self.listdir(dirpath)
                        for filename in filenames:
                            if filename.endswith(".index") and self.filenameok(filename):
                                yield os.path.join(dirpath, filename), False, None

            hostcount += 1
            lasttime = currtime
            currtime = time.time()
            logging.info("Processed %s of %s (hosttime %s, listdirtime %s, yieldtime %s) total %s estimated completion %s",
                         hostcount, len(hosts), currtime-lasttime, listdirtime, yieldtime, currtime - starttime,
                         datetime.fromtimestamp(starttime) + timedelta(seconds=(currtime - starttime) / hostcount * len(hosts)))


class LoadFileIndexUpdater(object):
    def __init__(self, config, resconf):
        self.config = config
        self.resource_id = resconf["resource_id"]
        self.batch_system = resconf['batch_system']

    def __enter__(self):
        if self.batch_system == "XDMoD":
            self.dbac = XDMoDArchiveCache(self.config)
        else:
            self.dbac = DbArchiveCache(self.config)

        self.paths_file = tempfile.NamedTemporaryFile('wb', delete=False, suffix=".csv")
        self.paths_csv = csv.writer(self.paths_file, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        self.joblevel_file = tempfile.NamedTemporaryFile('wb', delete=False, suffix=".csv")
        self.joblevel_csv = csv.writer(self.joblevel_file, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        self.nodelevel_file = tempfile.NamedTemporaryFile('wb', delete=False, suffix=".csv")
        self.nodelevel_csv = csv.writer(self.nodelevel_file, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        print self.paths_file.name
        print self.joblevel_file.name
        print self.nodelevel_file.name
        self.paths_file.file.flush()
        self.joblevel_file.file.flush()
        self.nodelevel_file.file.flush()
        self.dbac.insert_from_files(self.paths_file.name, self.joblevel_file.name, self.nodelevel_file.name)
        self.paths_file.close()
        self.joblevel_file.close()
        self.nodelevel_file.close()

    def insert(self, hostname, archive_path, start_timestamp, end_timestamp, jobid):
        self.paths_csv.writerow((archive_path,))
        if jobid is not None:
            self.joblevel_csv.writerow((archive_path, hostname, jobid, int(math.floor(start_timestamp)), int(math.ceil(end_timestamp))))
        else:
            self.nodelevel_csv.writerow((archive_path, hostname, int(math.floor(start_timestamp)), int(math.ceil(end_timestamp))))


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
    print "  -t --threads=NUM     Use the specified number of processes for parsing logs."
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
        "maxdate": datetime.now() - timedelta(minutes=10),
        "num_threads": 1
    }

    opts, _ = getopt(
        sys.argv[1:],
        "r:c:m:M:D:adqht:",
        ["resource=", "config=", "mindate=", "maxdate=", "debugfile", "all", "debug", "quiet", "help", "threads="]
    )

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
        elif opt[0] in ("-t", "--threads"):
            retdata["num_threads"] = int(opt[1])
        elif opt[0] in ("-h", "--help"):
            usage()
            sys.exit(0)

    return retdata


def runindexing():
    """ main script entry point """
    opts = getoptions()

    setuplogger(opts['log'], opts['debugfile'], logging.INFO)

    config = Config(opts['config'])

    logging.info("archive indexer starting")

    pool = None
    if opts['num_threads'] > 1:
        logging.debug("Using %s processes", opts['num_threads'])
        pool = Pool(opts['num_threads'])

    for resourcename, resource in config.resourceconfigs():

        if opts['resource'] in (None, resourcename, str(resource['resource_id'])):
            if not resource.get('pcp_log_dir'):
                continue

            acache = PcpArchiveProcessor(resource)
            afind = PcpArchiveFinder(opts['mindate'], opts['maxdate'])
            if pool is not None:
                index_resource_multiprocessing(config, resource, acache, afind, pool)
            else:
                fast_index_allowed = bool(resource.get("fast_index", False))
                with LoadFileIndexUpdater(config, resource) as index:
                    for archivefile, fast_index, hostname in itertools.islice(afind.find(resource['pcp_log_dir']), 10000):
                        start_time = time.time()
                        data = acache.processarchive(archivefile, fast_index and fast_index_allowed, hostname)
                        parse_end = time.time()
                        if data is not None:
                            index.insert(*data)
                        db_end = time.time()
                        logging.debug("processed archive %s (fileio %s, dbacins %s)", archivefile, parse_end - start_time, db_end - parse_end)

    logging.info("archive indexer complete")
    if pool is not None:
        pool.close()
        pool.join()


def processarchive_worker(parser, fast_index_allowed, parser_args):
    archive_file, fast_index, hostname = parser_args
    parser_start = time.time()
    data = parser.processarchive(archive_file, fast_index and fast_index_allowed, hostname)
    return data, time.time() - parser_start, archive_file


def index_resource_multiprocessing(config, resconf, acache, afind, pool):
    fast_index_allowed = bool(resconf.get("fast_index", False))

    worker = functools.partial(processarchive_worker, acache, fast_index_allowed)
    with LoadFileIndexUpdater(config, resconf) as index:
        for data, parse_time, archive_file in pool.imap_unordered(worker, itertools.islice(afind.find(resconf['pcp_log_dir']), 10000)):
            index_start = time.time()
            if data is not None:
                index.insert(*data)
            index_time = time.time() - index_start
            logging.debug("processed archive %s (fileio %s, dbacins %s)", archive_file, parse_time, index_time)


if __name__ == "__main__":
    runindexing()
