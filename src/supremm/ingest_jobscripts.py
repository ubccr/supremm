#!/usr/bin/env python
""" script that ingests the job launch scripts """
from datetime import datetime, timedelta
import os
import re
from supremm.scripthelpers import getdbconnection
import sys
import logging
import glob

from getopt import getopt
from supremm.config import Config

MAX_SCRIPT_LEN = (64 * 1024) - 1


class DbHelper(object):
    """ Helper class to interact with the database """

    def __init__(self, dwconfig, tablename):

        # The database schema should be created with utf8-unicode encoding.
        self.con = getdbconnection(dwconfig, False, {'charset': 'utf8', 'use_unicode': True})
        self.tablename = tablename
        self.query = "INSERT IGNORE INTO " + tablename + " (resource_id,local_job_id,script) VALUES(%s,%s,%s)"
        self.buffered = 0

    def insert(self, data):
        """ try to insert a record """
        cur = self.con.cursor()
        cur.execute(self.query, data)

        self.buffered += 1
        if self.buffered > 100:
            self.con.commit()
            self.buffered = 0

    def postinsert(self):
        """ call this to flush connection """
        self.con.commit()

    def getmostrecent(self, resource_id):
        """ return the timestamp of the most recent entry for the resource """
        query = "SELECT coalesce(MAX(updated),MAKEDATE(1970, 1)) FROM " + self.tablename + " WHERE resource_id = %s"
        data = (resource_id, )

        cur = self.con.cursor()
        cur.execute(query, data)
        return cur.fetchone()[0]

def pathfilter(path, mindate):
    """ return whether path should not be processed based on mindate
        return value of False indicates no filtering
        return value of true indicates the path should be filtered
    """

    if mindate == None:
        return False

    subdir = os.path.basename(path)
    try:
        if datetime.strptime(subdir, "%Y%m%d") < mindate:
            logging.debug("Skip(1) subdir %s", subdir)
            return True
    except ValueError:
        logging.debug("Skip(2) subdir %s", subdir)
        return True

    return False


def processfor(resource_id, respath, dbif, timedeltadays):
    """ find and ingest all job scripts for the given resource """

    count = 0
    fglob = re.compile(r"^([0-9]*)\.savescript")

    logging.debug("Processing path %s", respath)

    if timedeltadays == None:
        mindate = None
    else:
        mindate = dbif.getmostrecent(resource_id) - timedelta(days=timedeltadays)

    logging.debug("Start date is %s", mindate)

    for path in glob.glob(respath + "/[0-9]*"):

        if pathfilter(path, mindate):
            continue

        logging.debug("processing files in %s", path)

        for root, _, files in os.walk(path, topdown=True):

            for filename in files:
                mtch = fglob.match(filename)
                if mtch == None:
                    logging.debug("Ignore file %s", filename)
                    continue

                with open(os.path.join(root, filename), "rb") as scriptfile:
                    # Note: if non utf-8 characters are present in the file, they are encoded
                    scriptdata = scriptfile.read(MAX_SCRIPT_LEN).decode("utf-8", "replace")
                    if len(scriptdata) > MAX_SCRIPT_LEN:
                        # Could happen if the script contains non-utf-8 chars
                        scriptdata = scriptdata[:MAX_SCRIPT_LEN]

                    dbif.insert([resource_id, int(mtch.group(1)), scriptdata])
                    count += 1

    return count

DAY_DELTA = 2


def usage():
    """ print usage """
    print "usage: {0} [OPTS]".format(os.path.basename(__file__))
    print "  -r --resource=RES    process only archive files for the specified resource, if absent then all resources are processed"
    print "  -c --config=PATH     specify the path to the configuration directory"
    print "  -D --daydelta=DAYS   specify the number of days overlap from the last ingest (default", DAY_DELTA, "days ago)"
    print "  -a --all             process all scripts regardless of age"
    print "  -d --debug           set log level to debug"
    print "  -q --quiet           only log errors"
    print "  -h --help            print this help message"


def getoptions():
    """ process comandline options """

    retdata = {
        "log": logging.INFO,
        "resource": None,
        "config": None,
        "deltadays": DAY_DELTA
    }

    opts, _ = getopt(sys.argv[1:], "r:c:D:adqh", ["resource=", "config=", "daydelta=", "all", "debug", "quiet", "help"])

    for opt in opts:
        if opt[0] in ("-r", "--resource"):
            retdata['resource'] = opt[1]
        if opt[0] in ("-d", "--debug"):
            retdata['log'] = logging.DEBUG
        if opt[0] in ("-q", "--quiet"):
            retdata['log'] = logging.ERROR
        elif opt[0] in ("-c", "--config"):
            retdata['config'] = opt[1]
        elif opt[0] in ("-D", "--daydelta"):
            retdata['deltadays'] = int(opt[1])
        elif opt[0] in ("-a", "--all"):
            retdata['deltadays'] = None
        if opt[0] in ("-h", "--help"):
            usage()
            sys.exit(0)

    return retdata


def main():
    """
    main entry point for script
    """
    opts = getoptions()

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S', level=opts['log'])
    if sys.version.startswith("2.7"):
        logging.captureWarnings(True)

    config = Config(opts['config'])

    dwconfig = config.getsection("datawarehouse")
    dbif = DbHelper(dwconfig, 'modw_supremm.batchscripts')

    for resourcename, settings in config.resourceconfigs():

        if opts['resource'] in (None, resourcename, str(settings['resource_id'])):

            logging.debug("Processing %s (id=%s)", resourcename, settings['resource_id'])

            if "script_dir" in settings:
                total = processfor(settings['resource_id'], settings['script_dir'], dbif, opts['deltadays'])

                logging.info("Processed %s files for %s", total, resourcename)
            else:
                logging.debug("Skip resource %s no script dir defined", resourcename)

    dbif.postinsert()

if __name__ == "__main__":
    main()
