#!/usr/bin/env python3
""" Script that indexes the pc archives for a given resource.
"""

import logging
import math

import pytz
from pcp import pmapi
import cpmapi as c_pmapi
import time

from supremm.config import Config
from supremm.scripthelpers import parsetime, setuplogger

from supremm.account import DbArchiveCache
from supremm.xdmodaccount import XDMoDArchiveCache

import os
from datetime import datetime, timedelta, timezone
import re
from multiprocessing import Pool
import functools
import tempfile
import csv
import argparse


def datetime_to_timestamp(dt):
    return (dt - datetime.utcfromtimestamp(0)).total_seconds()


JOB_ARCHIVE_RE = re.compile(
    r"job-([^-]+)-(?:[a-z]+)-(?P<year>\d{4})(?P<month>\d{2})(?P<day>\d{2})\.(?P<hour>\d{2})\.(?P<minute>\d{2})\.(?P<second>\d{2})"
)

JOB_ID_REGEX = re.compile(r"^(?:(\d+)(?:[_\[](\d+)?\]?)?).*$")

class TimezoneAdjuster():
    def __init__(self, timezone_name, guess_early=True):
        self.timezone = pytz.timezone(timezone_name) if timezone_name is not None else datetime.now(timezone(timedelta(0))).astimezone().tzinfo
        self.guess_early = guess_early

    def adjust(self, dt):
        timestamp = datetime_to_timestamp(dt)
        try:
            return timestamp - self.timezone.utcoffset(dt).total_seconds()
        except pytz.exceptions.AmbiguousTimeError:
            return timestamp - self.timezone.utcoffset(dt, self.guess_early).total_seconds()


class PcpArchiveProcessor():
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
            jobtokens = JOB_ID_REGEX.match(fname.split("-")[1])

            if jobtokens:
                if jobtokens.group(2):
                    jobid = (int(jobtokens.group(1)), int(jobtokens.group(2)), -1)
                else:
                    jobid = (-1, -1, int(jobtokens.group(1)))

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
                hostname = str(mdata.hostname, 'ascii')
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
        arch_name = os.path.basename(arch_path)
        match = JOB_ARCHIVE_RE.match(arch_name)
        if not match:
            return None

        date_dict = {k: int(v) for k, v in match.groupdict().items()}
        start_datetime = datetime(**date_dict)
        return self.tz_adjuster.adjust(start_datetime)


class PcpArchiveFinder():
    """ Helper class that finds all pcp archive files in a directory
        mindate is the minimum datestamp of files that should be processed
    """

    def __init__(self, mindate, maxdate, all=False):
        self.mindate = mindate if not all else None
        self.maxdate = maxdate if not all else None
        if self.mindate is not None:
            self.minmonth = datetime(year=mindate.year, month=mindate.month, day=1) - timedelta(days=1)
        else:
            self.minmonth = None
        self.fregex = re.compile(
            r".*(\d{4})(\d{2})(\d{2})(?:\.(\d{2}).(\d{2})(?:[\.-](\d{2}))?)?\.index$")
        self.sregex = re.compile(r"^(\d{4})(\d{2})$")
        self.yearregex = re.compile(r"^\d{4}$")
        self.dateregex = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")

    def subdirok(self, subdir):
        """ check the name of a subdirectory and return whether to
            descend into it based on the name.
            @returns None if the name is not a datestamp
                     true if the name is a date that is >= the reference date
                     false if the name is a date that is < the reference
        """
        mtch = self.sregex.match(subdir)
        if mtch is None:
            return None

        if self.minmonth is None:
            return True

        subdirdate = datetime(year=int(mtch.group(1)), month=int(mtch.group(2)), day=1)

        return subdirdate > self.minmonth

    def filenameok(self, filename):
        """ parse filename to get the datestamp and compare with the reference datestamp
        """
        if self.mindate is None:
            return True

        mtch = self.fregex.match(filename)
        if mtch is None:
            logging.error(
                "Unparsable filename %s processing anyway.", filename)
            return True

        if mtch.group(4) is not None and mtch.group(5) is not None:
            filedate = datetime(year=int(mtch.group(1)), month=int(mtch.group(2)), day=int(mtch.group(3)), hour=int(mtch.group(4)), minute=int(mtch.group(5)))
        else:
            filedate = datetime(year=int(mtch.group(1)), month=int(mtch.group(2)), day=int(mtch.group(3)))

        if self.maxdate is None:
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

            if day is None:
                # Some datetime arithmetic to get the last day of the month
                tmpdate = datetime(year=yyyy, month=mm, day=28, hour=23, minute=59, second=59) + timedelta(days=4)
                dirdate = tmpdate - timedelta(days=tmpdate.day)
            else:
                dirdate = datetime(year=yyyy, month=mm, day=int(day), hour=23, minute=59, second=59)

        except ValueError:
            return None

        if self.mindate is None:
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
        """ main entry for the archive file finder. There are multiple different
            directory structures supported. The particular directory stucture
            is automatically detected based on the directory names. """

        if topdir == "":
            return

        dirs = self.listdir(topdir)

        yeardirs = []
        hostdirs = []
        for dirpath in dirs:
            if dirpath == 'README':
                continue

            if self.yearregex.match(dirpath):
                yeardirs.append(dirpath)
            else:
                hostdirs.append(dirpath)

        for archivefile, fast_index, hostname in self.parse_by_date(topdir, yeardirs):
            yield archivefile, fast_index, hostname

        for archivefile, fast_index, hostname in self.parse_by_host(topdir, hostdirs):
            yield archivefile, fast_index, hostname

    def parse_by_date(self, top_dir, year_dirs):
        """ find all archives that are organised in a directory
            structure like:
                [top_dir]/[YYYY]/[MM]/[HOSTNAME]/[YYYY-MM-DD]
        """

        for year_dir in year_dirs:
            year_dir_ok = self.ymdok(year_dir)
            if year_dir_ok is True:
                for month_dir in self.listdir(os.path.join(top_dir, year_dir)):
                    if self.ymdok(year_dir, month_dir) is True:
                        for host_dir in self.listdir(os.path.join(top_dir, year_dir, month_dir)):
                            for date_dir in self.listdir(os.path.join(top_dir, year_dir, month_dir, host_dir)):
                                date_match = self.dateregex.match(date_dir)
                                if date_match and self.ymdok(date_match.group(1), date_match.group(2), date_match.group(3)):
                                    dirpath = os.path.join(top_dir, year_dir, month_dir, host_dir, date_dir)
                                    filenames = self.listdir(dirpath)
                                    for filename in filenames:
                                        if filename.endswith(".index") and self.filenameok(filename):
                                            yield os.path.join(dirpath, filename), True, host_dir


    def parse_by_host(self, topdir, hosts):
        """ find all archive files that are organised in a directory
            structure like:
               [topdir]/[HOSTNAME]/[YYYY]/[MM]/[DD]/{archive files}

            also support:
               [topdir]/[HOSTNAME]/[YYYYMM]/{archive files}

            and:
               [topdir]/[HOSTNAME]/{archive files}
        """

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


class LoadFileIndexUpdater():
    def __init__(self, config, resconf, keep_csv=False, dry_run=False):
        self.config = config
        self.resource_id = resconf["resource_id"]
        self.batch_system = resconf['batch_system']
        self.keep_csv = keep_csv
        self.dry_run = dry_run

    def __enter__(self):
        self.paths_file = tempfile.NamedTemporaryFile('w', delete=not self.keep_csv, suffix=".csv", prefix="archive_paths")
        self.paths_csv = csv.writer(self.paths_file, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        self.joblevel_file = tempfile.NamedTemporaryFile('w', delete=not self.keep_csv, suffix=".csv", prefix="archives_joblevel")
        self.joblevel_csv = csv.writer(self.joblevel_file, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        self.nodelevel_file = tempfile.NamedTemporaryFile('w', delete=not self.keep_csv, suffix=".csv", prefix="archives_nodelevel")
        self.nodelevel_csv = csv.writer(self.nodelevel_file, lineterminator="\n", quoting=csv.QUOTE_MINIMAL, escapechar='\\')
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.keep_csv:
            logging.info(self.paths_file.name)
            logging.info(self.joblevel_file.name)
            logging.info(self.nodelevel_file.name)
        self.paths_file.file.flush()
        self.joblevel_file.file.flush()
        self.nodelevel_file.file.flush()
        if not self.dry_run:
            if self.batch_system == "XDMoD":
                dbac = XDMoDArchiveCache(self.config)
            else:
                dbac = DbArchiveCache(self.config)

            dbac.insert_from_files(self.paths_file.name, self.joblevel_file.name, self.nodelevel_file.name)
        self.paths_file.close()
        self.joblevel_file.close()
        self.nodelevel_file.close()

    def insert(self, hostname, archive_path, start_timestamp, end_timestamp, jobid):
        self.paths_csv.writerow((archive_path,))
        if jobid is not None:
            self.joblevel_csv.writerow((archive_path, hostname, jobid[0], jobid[1], jobid[2], int(math.floor(start_timestamp)), int(math.ceil(end_timestamp))))
        else:
            self.nodelevel_csv.writerow((archive_path, hostname, int(math.floor(start_timestamp)), int(math.ceil(end_timestamp))))


DAY_DELTA = 3

def getoptions():
    """ process comandline options """
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-r", "--resource", metavar="RES",
        help="Process only archive files for the specified resource, if absent then all resources are processed"
    )

    parser.add_argument("-c", "--config", help="Specify the path to the configuration directory")

    parser.add_argument(
        "-m", "--mindate", metavar="DATE", type=parsetime, default=datetime.now() - timedelta(days=DAY_DELTA),
        help="Specify the minimum datestamp of archives to process (default {} days ago)".format(DAY_DELTA)
    )

    parser.add_argument(
        "-M", "--maxdate", metavar="DATE", type=parsetime, default=datetime.now() - timedelta(minutes=10),
        help="Specify the maximum datestamp of archives to process (default now())"
    )

    parser.add_argument("-a", "--all", action="store_true", help="Process all archives regardless of age")

    parser.add_argument("-t", "--threads", dest="num_threads", metavar="NUM", type=int, default=1,
                        help="Use the specified number of processes for parsing logs")

    parser.add_argument("-k", "--keep-csv", dest="keep_csv", action="store_true",
                        help="Don't delete temporary csv files when indexing is done, and log filenames at INFO level. Used for debugging purposes")

    grp = parser.add_mutually_exclusive_group()
    grp.add_argument("-d", "--debug", dest="log", action="store_const", const=logging.DEBUG, default=logging.INFO,
                     help="Set log level to debug")
    grp.add_argument("-q", "--quiet", dest="log", action="store_const", const=logging.ERROR,
                     help="Only log errors")

    parser.add_argument(
        "-D", "--debugfile",
        help="""
        Specify the path to a log file. If this option is present the process will log a DEBUG level to this file.
        This logging is independent of the console log.
        """
    )

    parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="Process archives as normal but do not write results to the database.")

    args = parser.parse_args()
    return vars(args)


def runindexing():
    """ main script entry point """
    opts = getoptions()
    keep_csv = opts["keep_csv"]
    dry_run = opts["dry_run"]

    setuplogger(opts['log'], opts['debugfile'], filelevel=logging.INFO)

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
            afind = PcpArchiveFinder(opts['mindate'], opts['maxdate'], opts['all'])
            if pool is not None:
                index_resource_multiprocessing(config, resource, acache, afind, pool, keep_csv, dry_run)
            else:
                fast_index_allowed = bool(resource.get("fast_index", False))
                with LoadFileIndexUpdater(config, resource, keep_csv, dry_run) as index:
                    for archivefile, fast_index, hostname in afind.find(resource['pcp_log_dir']):
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


def index_resource_multiprocessing(config, resconf, acache, afind, pool, keep_csv, dry_run):
    fast_index_allowed = bool(resconf.get("fast_index", False))

    worker = functools.partial(processarchive_worker, acache, fast_index_allowed)
    with LoadFileIndexUpdater(config, resconf, keep_csv, dry_run) as index:
        for data, parse_time, archive_file in pool.imap_unordered(worker, afind.find(resconf['pcp_log_dir'])):
            index_start = time.time()
            if data is not None:
                index.insert(*data)
            index_time = time.time() - index_start
            logging.debug("processed archive %s (fileio %s, dbacins %s)", archive_file, parse_time, index_time)


if __name__ == "__main__":
    runindexing()
