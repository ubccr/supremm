#!/usr/bin/env python
"""
    Main script for converting host-based pcp archives to job-level summaries.
"""

import logging
from supremm.config import Config
from supremm.account import DbAcct
from supremm.xdmodaccount import XDMoDAcct
from supremm.pcparchive import extract_and_merge_logs
from supremm import outputter
from supremm.summarize import Summarize
from supremm.plugin import loadplugins, loadpreprocessors
from supremm.scripthelpers import parsetime

import sys
import os
from getopt import getopt
import traceback
import time
import datetime
import shutil
from multiprocessing import Process


def usage():
    """ print usage """
    print "usage: {0} [OPTS]".format(os.path.basename(__file__))
    print "  -j --localjobid JOBID process only the job with the provided local job id"
    print "                        (resource must also be specified)"
    print "  -r --resource RES     process only jobs on the specified resource"
    print "  -t --threads THEADS   number of concurrent processes to create"
    print "  -d --debug            set log level to debug"
    print "  -q --quiet            only log errors"
    print "  -s --start TIME       process all jobs that ended after the provided start"
    print "                        time (an end time must also be specified)"
    print "  -e --end TIME         process all jobs that ended before the provided end"
    print "                        time (a start time must also be specified)"
    print "  -T --timeout SECONDS  amount of elapsed time from a job ending to when it"
    print "                        can be marked as processed even if the raw data is"
    print "                        absent"
    print "  -D --delete T|F       whether to delete job-level archives after processing."
    print "  -E --extract-only     only extract the job-level archives (sets delete=False)"
    print "  -o --output DIR       override the output directory for the job archives."
    print "                        This directory will be emptied before used and no"
    print "                        subdirectories will be created. This option is ignored "
    print "                        if multiple jobs are to be processed."
    print "  -h --help             display this help message and exit."


def getoptions():
    """ process comandline options """

    localjobid = None
    resource = None
    starttime = None
    endtime = None
    joboutdir = None

    retdata = {
        "log": logging.INFO,
        "threads": 1,
        "dodelete": True,
        "extractonly": False,
        "job_output_dir": None,
        "force_timeout": 2 * 24 * 3600,
        "resource": None
    }

    opts, _ = getopt(sys.argv[1:], "j:r:t:dqs:e:T:D:Eo:h", 
                     ["localjobid=", 
                      "resource=", 
                      "threads=", 
                      "debug", 
                      "quiet", 
                      "start=", 
                      "end=", 
                      "timeout=", 
                      "delete=", 
                      "extract-only", 
                      "output=", 
                      "help"])

    for opt in opts:
        if opt[0] in ("-j", "--jobid"):
            localjobid = opt[1]
        if opt[0] in ("-r", "--resource"):
            resource = opt[1]
        if opt[0] in ("-d", "--debug"):
            retdata['log'] = logging.DEBUG
        if opt[0] in ("-q", "--quiet"):
            retdata['log'] = logging.ERROR
        if opt[0] in ("-t", "--threads"):
            retdata['threads'] = int(opt[1])
        if opt[0] in ("-s", "--start"):
            starttime = parsetime(opt[1])
        if opt[0] in ("-e", "--end"):
            endtime = parsetime(opt[1])
        if opt[0] in ("-T", "--timeout"):
            retdata['force_timeout'] = int(opt[1])
        if opt[0] in ("-D", "--delete"):
            retdata['dodelete'] = True if opt[1].upper().startswith("T") else False
        if opt[0] in ("-E", "--extract-only"):
            retdata['extractonly'] = True
        if opt[0] in ("-o", "--output"):
            joboutdir = opt[1]
        if opt[0] in ("-h", "--help"):
            usage()
            sys.exit(0)

    if retdata['extractonly']:
        # extract-only supresses archive delete
        retdata['dodelete'] = False

    if not (starttime == None and endtime == None):
        if starttime == None or endtime == None:
            usage()
            sys.exit(1)
        retdata.update({"mode": "timerange", "start": starttime, "end": endtime, "resource": resource})
        return retdata

    if localjobid == None and resource == None:
        retdata.update({"mode": "all"})
        return retdata

    if localjobid != None and resource != None:
        retdata.update({"mode": "single", "local_job_id": localjobid, "resource": resource, "job_output_dir": joboutdir})
        return retdata

    if resource != None:
        retdata.update({"mode": "resource", "resource": resource})
        return retdata

    usage()
    sys.exit(1)


def summarizejob(job, conf, resconf, plugins, preprocs, m, dblog, opts):
    """ Main job processing, Called for every job to be processed """

    success = False

    try:
        mergestart = time.time()
        mergeresult = extract_and_merge_logs(job, conf, resconf)
        mergeend = time.time()

        if opts['extractonly']: 
            return 0 == mergeresult

        preprocessors = [x(job) for x in preprocs]
        analytics = [x(job) for x in plugins]
        s = Summarize(preprocessors, analytics, job)

        if 0 == mergeresult:
            logging.info("Success for %s files in %s", job.job_id, job.jobdir)
            s.process()

        m.process(s, {"mergetime": mergeend - mergestart})

        success = s.complete()

        force_success = False
        if not success:
            force_timeout = opts['force_timeout']
            if (datetime.datetime.now() - job.end_datetime) > datetime.timedelta(seconds=force_timeout):
                force_success = True

        dblog.markasdone(job, success or force_success, time.time() - mergestart)

    except Exception as e:
        logging.error("Failure for job %s %s. Error: %s %s", job.job_id, job.jobdir, str(e), traceback.format_exc())

    if opts['dodelete'] and job.jobdir != None and os.path.exists(job.jobdir):
        # Clean up
        shutil.rmtree(job.jobdir)

    return success

def override_defaults(resconf, opts):
    """ Commandline options that override the configruation file settings """
    if 'job_output_dir' in opts and opts['job_output_dir'] != None:
        resconf['job_output_dir'] = opts['job_output_dir']

    return resconf


def processjobs(config, opts, procid):
    """ main function that does the work. One run of this function per process """

    preprocs = loadpreprocessors()
    logging.debug("Loaded %s preprocessors", len(preprocs))

    plugins = loadplugins()
    logging.debug("Loaded %s plugins", len(plugins))

    for r, resconf in config.resourceconfigs():
        if opts['resource'] == None or opts['resource'] == r or opts['resource'] == str(resconf['resource_id']):
            logging.info("Processing resource %s", r)
        else:
            continue

        resconf = override_defaults(resconf, opts)

        with outputter.factory(config, resconf) as m:

            if resconf['batch_system'] == "XDMoD":
                dbif = XDMoDAcct(resconf['resource_id'], config, opts['threads'], procid)
            else:
                dbif = DbAcct(resconf['resource_id'], config, opts['threads'], procid)

            if opts['mode'] == "single":
                for job in dbif.getbylocaljobid(opts['local_job_id']):
                    summarizejob(job, config, resconf, plugins, preprocs, m, dbif, opts)
            elif opts['mode'] == "timerange":
                for job in dbif.getbytimerange(opts['start'], opts['end']):
                    summarizejob(job, config, resconf, plugins, preprocs, m, dbif, opts)
            else:
                for job in dbif.get(None, None):
                    summarizejob(job, config, resconf, plugins, preprocs, m, dbif, opts)


def main():
    """
    main entry point for script
    """
    opts = getoptions()

    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%dT%H:%M:%S', level=opts['log'])
    logging.captureWarnings(True)

    config = Config()

    threads = opts['threads']

    if threads <= 1:
        processjobs(config, opts, None)
        return
    else:
        proclist = []
        for procid in xrange(threads):
            p = Process(target=processjobs, args=(config, opts, procid))
            p.start()
            proclist.append(p)

        for proc in proclist:
            p.join()


if __name__ == "__main__":
    main()
