#!/usr/bin/env python
""" Script that reads hardware info from the archives 
and outputs the data into a json file
"""

import argparse
from datetime import datetime, timedelta
import logging
import os
import re
import time

from supremm.config import Config
from supremm.scripthelpers import parsetime, setuplogger

from supremm.account import DbArchiveCache
from supremm.xdmodaccount import XDMoDArchiveCache

DAY_DELTA = 3

# No changes (except deleting tz_adjuster)
class PcpArchiveProcessor(object):
    """ Parses a pcp archive and adds the archive information to the index """

    def __init__(self, resconf):
        self.hostname_mode = resconf['hostname_mode']
        if self.hostname_mode == "fqdn":
            self.hostnameext = resconf['host_name_ext']
        # self.tz_adjuster = TimezoneAdjuster(resconf.get("timezone"))

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

# No changes
class PcpArchiveFinder(object):
    """ Helper class that finds all pcp archive files in a directory
        mindate is the minimum datestamp of files that should be processed
    """

    def __init__(self, mindate, maxdate, all=False):
        self.mindate = mindate if not all else None
        self.maxdate = maxdate if not all else None
        if self.mindate != None:
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


def getoptions():
    """ process comandline options """
    parser = argparse.ArgumentParser()

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

    args = parser.parse_args()
    return vars(args)

def getHardwareInfo():
    """Main entry point"""
    opts = getoptions()
    print('MY_CONFIG = ' + os.path.abspath(opts['config']))
    config = Config(opts['config'])

    for resourcename, resource in config.resourceconfigs():
        print('LOG_DIR = ' + resource['pcp_log_dir'])
        acache = PcpArchiveProcessor(resource)
        afind = PcpArchiveFinder(opts['mindate'], opts['maxdate'], opts['all'])
        for archivefile, fast_index, hostname in afind.find(resource['pcp_log_dir']):
            print(archivefile)

if __name__ == "__main__":
    getHardwareInfo()