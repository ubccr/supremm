#!/usr/bin/env python
"""
    Main script for converting host-based pcp archives to job-level summaries.
"""

from mpi4py import MPI

import logging
from supremm.config import Config
from supremm.account import DbAcct
from supremm.xdmodaccount import XDMoDAcct
from supremm.pcparchive import extract_and_merge_logs
from supremm import outputter
from supremm.summarize import Summarize
from supremm.plugin import loadplugins, loadpreprocessors
from supremm.scripthelpers import parsetime
from supremm.scripthelpers import setuplogger

import sys
import os
from getopt import getopt
import traceback
import time
import datetime
import shutil


def usage():
    """ print usage """
    print "usage: {0} [OPTS]".format(os.path.basename(__file__))
    print "  -j --localjobid JOBID process only the job with the provided local job id"
    print "                        (resource must also be specified)"
    print "  -r --resource RES     process only jobs on the specified resource"
    print "  -d --debug            set log level to debug"
    print "  -q --quiet            only log errors"
    print "  -s --start TIME       process all jobs that ended after the provided start"
    print "                        time (an end time must also be specified)"
    print "  -e --end TIME         process all jobs that ended before the provided end"
    print "                        time (a start time must also be specified)"
    print "  -N --new-only         when using a timerange, only look for unprocessed jobs"
    print "  -T --timeout SECONDS  amount of elapsed time from a job ending to when it"
    print "                        can be marked as processed even if the raw data is"
    print "                        absent"
    print "  -t --tag              tag to add to the summarization field in mongo"
    print "  -D --delete T|F       whether to delete job-level archives after processing."
    print "  -E --extract-only     only extract the job-level archives (sets delete=False)"
    print "  -L --use-lib-extract  use libpcp_pmlogextract.so.1 instead of pmlogextract"
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
        "dodelete": True,
        "extractonly": False,
        "libextract": False,
        "newonly": False,
        "job_output_dir": None,
        "tag": None,
        "force_timeout": 2 * 24 * 3600,
        "resource": None
    }

    opts, _ = getopt(sys.argv[1:], "j:r:dqs:e:LNT:t:D:Eo:h", 
                     ["localjobid=", 
                      "resource=", 
                      "debug", 
                      "quiet", 
                      "start=", 
                      "end=", 
                      "new-only",
                      "timeout=", 
                      "tag=",
                      "delete=", 
                      "extract-only", 
                      "use-lib-extract", 
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
        if opt[0] in ("-s", "--start"):
            starttime = parsetime(opt[1])
        if opt[0] in ("-e", "--end"):
            endtime = parsetime(opt[1])
        if opt[0] in ("-N", "--new-only"):
            retdata['newonly'] = True
        if opt[0] in ("-L", "--use-lib-extract"):
            retdata['libextract'] = True
        if opt[0] in ("-T", "--timeout"):
            retdata['force_timeout'] = int(opt[1])
        if opt[0] in ("-t", "--tag"):
            retdata['tag'] = str(opt[1])
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
            
        mdata = {}
        mergestart = time.time()
        if job.nodecount > 1 and job.walltime < 5 * 60:
            mergeresult = 1
            mdata["skipped"] = True
        else:
            mergeresult = extract_and_merge_logs(job, conf, resconf, opts)
        mergeend = time.time()

        if opts['extractonly']: 
            return 0 == mergeresult

        preprocessors = [x(job) for x in preprocs]
        analytics = [x(job) for x in plugins]
        s = Summarize(preprocessors, analytics, job)

        if 0 == mergeresult:
            logging.info("Success for %s files in %s", job.job_id, job.jobdir)
            s.process()

        mdata["mergetime"] = mergeend - mergestart
        
        if opts['tag'] != None:
            mdata['tag'] = opts['tag']

        m.process(s, mdata)

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

def filter_plugins(resconf, preprocs, plugins):
    """ Filter the list of plugins/preprocs to use on a resource basis """

    # Default is to use all
    filtered_preprocs=preprocs
    filtered_plugins=plugins

    if "plugin_whitelist" in resconf:
       filtered_preprocs = [x for x in preprocs if x.__name__ in resconf['plugin_whitelist']]
       filtered_plugins = [x for x in plugins if x.__name__ in resconf['plugin_whitelist']]
    elif "plugin_blacklist" in resconf:
       filtered_preprocs = [x for x in preprocs if x.__name__ not in resconf['plugin_blacklist']]
       filtered_plugins = [x for x in plugins if x.__name__ not in resconf['plugin_blacklist']]

    return filtered_preprocs, filtered_plugins

def processjobs(config, opts, procid, comm):
    """ main function that does the work. One run of this function per process """

    allpreprocs = loadpreprocessors()
    logging.debug("Loaded %s preprocessors", len(allpreprocs))

    allplugins = loadplugins()
    logging.debug("Loaded %s plugins", len(allplugins))

    for r, resconf in config.resourceconfigs():
        if opts['resource'] == None or opts['resource'] == r or opts['resource'] == str(resconf['resource_id']):
            logging.info("Processing resource %s", r)
        else:
            continue

        preprocs, plugins = filter_plugins(resconf, allpreprocs, allplugins)

        logging.debug("Using %s preprocessors", len(preprocs))
        logging.debug("Using %s plugins", len(plugins))


        resconf = override_defaults(resconf, opts)

        with outputter.factory(config, resconf) as m:

            if resconf['batch_system'] == "XDMoD":
                dbif = XDMoDAcct(resconf['resource_id'], config, None, None)
            else:
                dbif = DbAcct(resconf['resource_id'], config)

            # Master
            if procid==0:
                getjobs={}
                if opts['mode'] == "single":
                    getjobs['cmd']=dbif.getbylocaljobid
                    getjobs['opts']=[opts['local_job_id'],]
                elif opts['mode'] == "timerange":
                    getjobs['cmd']=dbif.getbytimerange
                    getjobs['opts']=[opts['start'], opts['end'], opts['newonly']]
                else:
                    getjobs['cmd']=dbif.get
                    getjobs['opts']=[None, None]

                logging.debug("MASTER STARTING")
                numworkers = opts['threads']-1
                numsent = 0
                numreceived = 0

                for job in getjobs['cmd'](*(getjobs['opts'])):
                    if numsent >= numworkers:
                        # Wait for a worker to be done and then send more work
                        process = comm.recv(source=MPI.ANY_SOURCE, tag=1)
                        numreceived += 1
                        comm.send(job, dest=process, tag=1)
                        numsent += 1
                        logging.debug("Sent new job: %d sent, %d received", numsent, numreceived)
                    else:
                        # Initial batch
                        comm.send(job, dest=numsent+1, tag=1)
                        numsent += 1
                        logging.debug("Initial Batch: %d sent, %d received", numsent, numreceived)

                logging.debug("After all jobs sent: %d sent, %d received", numsent, numreceived)

                # Get leftover results
                while numsent > numreceived:
                    comm.recv(source=MPI.ANY_SOURCE, tag=1)
                    numreceived += 1
                    logging.debug("Getting leftovers. %d sent, %d received", numsent, numreceived)

                # Shut them down
                for worker in xrange(numworkers):
                    logging.debug("Shutting down: %d", worker+1)
                    comm.send(None, dest=worker+1, tag=1)

            # Worker
            else:
                sendtime=time.time()
                midtime=time.time()
                recvtime=time.time()
                logging.debug("WORKER %d STARTING", procid)
                while True:
                    recvtries=0
                    while not comm.Iprobe(source=0, tag=1):
                        if recvtries < 1000:
                            recvtries+=1
                            continue
                        # Sleep so we can instrument how efficient we are
                        # Otherwise, workers spin on exit at the hidden mpi_finalize call.
                        # If you care about maximum performance and don't care about wasted cycles, remove the Iprobe/sleep loop
                        # Empirically, a tight loop with time.sleep(0.001) uses ~1% CPU
                        time.sleep(0.001)
                    job = comm.recv(source=0, tag=1)
                    recvtime=time.time()
                    mpisendtime = midtime-sendtime
                    mpirecvtime = recvtime-midtime
                    if (mpisendtime+mpirecvtime) > 2:
                        logging.warning("MPI send/recv took %s/%s", mpisendtime, mpirecvtime)
                    if job != None:
                        logging.debug("Rank: %s, Starting: %s", procid, job.job_id)
                        summarizejob(job, config, resconf, plugins, preprocs, m, dbif, opts)
                        logging.debug("Rank: %s, Finished: %s", procid, job.job_id)
                        sendtime=time.time()
                        comm.send(procid, dest=0, tag=1)
                        midtime=time.time()
                    else:
                        # Got shutdown message
                        break

def main():
    """
    main entry point for script
    """

    comm = MPI.COMM_WORLD

    opts = getoptions()

    opts['threads'] = comm.Get_size()

    logout = "mpiOutput-{}.log".format(comm.Get_rank())

    # For MPI jobs, do something sane with logging.
    setuplogger(logging.ERROR, logout, opts['log'])

    config = Config()

    if comm.Get_size() < 2:
        logging.error("Must run MPI job with at least 2 processes")
        sys.exit(1)

    processjobs(config, opts, comm.Get_rank(), comm)

    logging.debug("Rank: %s FINISHED", comm.Get_rank())

if __name__ == "__main__":
    main()
