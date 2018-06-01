#!/usr/bin/env python
"""
  Commandline options
"""
from supremm.scripthelpers import parsetime
from supremm.pcparchive import extract_and_merge_logs
from supremm.summarize import Summarize
from supremm.errors import ProcessingError

import sys
from getopt import getopt
import os
import time
import datetime
import logging

def usage(has_mpi):
    """ print usage """
    print "usage: {0} [OPTS]".format(os.path.basename(sys.argv[0]))
    print "  -j --localjobid JOBID process only the job with the provided local job id"
    print "                        (resource must also be specified)"
    print "  -r --resource RES     process only jobs on the specified resource"
    if not has_mpi:
        print "  -t --threads THEADS   number of concurrent processes to create"
    print "  -d --debug            set log level to debug"
    print "  -q --quiet            only log errors"
    print "  -s --start TIME       process all jobs that ended after the provided start"
    print "                        time (an end time must also be specified)"
    print "  -e --end TIME         process all jobs that ended before the provided end"
    print "                        time (a start time must also be specified)"
    print "  -A --process-all      when using a timerange, look for all jobs. Combines flags (BONC)"
    print "  -B --process-bad      when using a timerange, look for jobs that previously failed to process"
    print "  -O --process-old      when using a timerange, look for jobs that have an old process version"
    print "  -N --process-notdone  when using a timerange, look for unprocessed jobs"
    print "  -C --process-current  when using a timerange, look for jobs with the current process version"
    print "  -b --process-big      when using a timerange, look for jobs that were previously marked as being too big"
    print "  -P --process-error N  when using a timerange, look for jobs that were previously marked with error N"
    print "  -T --timeout SECONDS  amount of elapsed time from a job ending to when it"
    print "                        is marked as process even if the source data is not available"
    print "  -M --max-nodes NODES  only process jobs with fewer than this many nodes"
    print "                        can be marked as processed even if the raw data is"
    print "                        absent"
    print "     --min-duration SECONDS   only process jobs with a duration longer than SECONDS"
    print "                              (default no limit)"
    print "     --min-parallel-duration SECONDS   only process parallel jobs with a"
    print "                                       duration longer than SECONDS (default no limit)"
    print "     --max-duration SECONDS   only process jobs with a duration shorter than SECONDS"
    print "                              (default 864000 seconds)"
    print "     --tag              tag to add to the summarization field in mongo"
    if has_mpi:
        print "     --dump-proclist    whether to output the MPI process information periodically"
    print "  -D --delete T|F       whether to delete job-level archives after processing."
    print "  -E --extract-only     only extract the job-level archives (sets delete=False)"
    print "  -L --use-lib-extract  use libpcp_pmlogextract.so.1 instead of pmlogextract"
    print "  -o --output DIR       override the output directory for the job archives."
    print "                        This directory will be emptied before used and no"
    print "                        subdirectories will be created. This option is ignored "
    print "                        if multiple jobs are to be processed."
    print "  -n --dry-run          process jobs but do not write to database."
    print "  -h --help             display this help message and exit."


def getoptions(has_mpi):
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
        "libextract": False,
        "process_all": False,
        "process_bad": False,
        "process_old": False,
        "process_notdone": False,
        "process_current": False,
        "process_big": False,
        "process_error": 0,
        "max_nodes": 0,
        "max_nodetime": None,
        "min_duration": None,
        "min_parallel_duration": None,
        "max_duration": 864000,
        "job_output_dir": None,
        "tag": None,
        "dump_proclist": False,
        "force_timeout": 2 * 24 * 3600,
        "resource": None,
        "dry_run": False
    }

    opts, _ = getopt(sys.argv[1:], "ABONCbP:M:j:r:t:dqs:e:LT:t:D:Eo:hn",
                     ["localjobid=",
                      "resource=",
                      "threads=",
                      "debug",
                      "quiet",
                      "start=",
                      "end=",
                      "process-all",
                      "process-bad",
                      "process-old",
                      "process-notdone",
                      "process-current",
                      "process-big",
                      "process-error=",
                      "max-nodes=",
                      "max-nodetime=",
                      "min-duration=",
                      "min-parallel-duration=",
                      "max-duration=",
                      "timeout=",
                      "dump-proclist",
                      "tag=",
                      "delete=",
                      "extract-only",
                      "use-lib-extract",
                      "output=",
                      "help",
                      "dry-run"])

    for opt in opts:
        if opt[0] in ("-j", "--localjobid"):
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
        if opt[0] in ("-A", "--process-all"):
            retdata['process_all'] = True
        if opt[0] in ("-B", "--process-bad"):
            retdata['process_bad'] = True
        if opt[0] in ("-O", "--process-old"):
            retdata['process_old'] = True
        if opt[0] in ("-N", "--process-notdone"):
            retdata['process_notdone'] = True
        if opt[0] in ("-C", "--process-current"):
            retdata['process_current'] = True
        if opt[0] in ("-b", "--process-big"):
            retdata['process_big'] = True
        if opt[0] in ("-P", "--process-error"):
            retdata['process_error'] = int(opt[1])
        if opt[0] in ("-L", "--use-lib-extract"):
            retdata['libextract'] = True
        if opt[0] in ("-M", "--max-nodes"):
            retdata['max_nodes'] = int(opt[1])
        if opt[0] == "--max-nodetime":
            retdata['max_nodetime'] = int(opt[1])
        if opt[0] == "--min-duration":
            retdata['min_duration'] = int(opt[1])
        if opt[0] == "--min-parallel-duration":
            retdata['min_parallel_duration'] = int(opt[1])
        if opt[0] == "--max-duration":
            retdata['max_duration'] = int(opt[1])
        if opt[0] in ("-T", "--timeout"):
            retdata['force_timeout'] = int(opt[1])
        if opt[0] == "--tag":
            retdata['tag'] = str(opt[1])
        if opt[0] == '--dump-proclist':
            retdata['dump_proclist'] = True
        if opt[0] in ("-D", "--delete"):
            retdata['dodelete'] = True if opt[1].upper().startswith("T") else False
        if opt[0] in ("-E", "--extract-only"):
            retdata['extractonly'] = True
        if opt[0] in ("-o", "--output"):
            joboutdir = opt[1]
        if opt[0] in ("-n", "--dry-run"):
            retdata["dry_run"] = True
        if opt[0] in ("-h", "--help"):
            usage(has_mpi)
            sys.exit(0)

    if retdata['extractonly']:
        # extract-only supresses archive delete
        retdata['dodelete'] = False

    # If all options selected, treat as all to optimize the job selection query
    if retdata['process_bad'] and retdata['process_old'] and retdata['process_notdone'] and retdata['process_current']:
        retdata['process_all'] = True

    if not (starttime == None and endtime == None):
        if starttime == None or endtime == None:
            usage(has_mpi)
            sys.exit(1)
        retdata.update({"mode": "timerange", "start": starttime, "end": endtime, "resource": resource})
        # Preserve the existing mode where just specifying a timerange does all jobs
        if not retdata['process_bad'] and not retdata['process_old'] and not retdata['process_notdone'] and not retdata['process_current'] and not retdata['process_big'] and retdata['process_error'] == 0:
            retdata['process_all'] = True
        return retdata
    else:
        if not retdata['process_bad'] and not retdata['process_old'] and not retdata['process_notdone'] and not retdata['process_current'] and not retdata['process_big'] and retdata['process_error'] == 0:
            # Preserve the existing mode where unprocessed jobs are selected when no time range given
            retdata['process_bad'] = True
            retdata['process_old'] = True
            retdata['process_notdone'] = True
        if (retdata['process_bad'] and retdata['process_old'] and retdata['process_notdone'] and retdata['process_current']) or retdata['process_all']:
            # Sanity checking to not do every job in the DB
            logging.error("Cannot process all jobs without a time range")
            sys.exit(1)

    if localjobid == None and resource == None:
        retdata.update({"mode": "all"})
        return retdata

    if localjobid != None and resource != None:
        retdata.update({"mode": "single", "local_job_id": localjobid, "resource": resource, "job_output_dir": joboutdir})
        return retdata

    if resource != None:
        retdata.update({"mode": "resource", "resource": resource})
        return retdata

    usage(has_mpi)
    sys.exit(1)


def summarizejob(job, conf, resconf, plugins, preprocs, opts):
    """
    Main job processing, called for every job to be processed.
    Returns a tuple representing the results of summarizing:
    (Summarize object, metadata dict, boolean success, error code if any).
    """

    mdata = {}
    mergestart = time.time()

    summarizeerror = None

    if job.nodecount > 1 and opts['min_parallel_duration'] != None and job.walltime < opts['min_parallel_duration']:
        mergeresult = 1
        mdata["skipped_parallel_too_short"] = True
        summarizeerror = ProcessingError.PARALLEL_TOO_SHORT
        # Was "skipped"
        missingnodes = job.nodecount
        logging.info("Skipping %s, skipped_parallel_too_short", job.job_id)
    elif opts['min_duration'] != None and job.walltime < opts['min_duration']:
        mergeresult = 1
        mdata["skipped_too_short"] = True
        summarizeerror = ProcessingError.TIME_TOO_SHORT
        missingnodes = job.nodecount
        logging.info("Skipping %s, skipped_too_short", job.job_id)
    elif job.nodecount < 1:
        mergeresult = 1
        mdata["skipped_invalid_nodecount"] = True
        summarizeerror = ProcessingError.INVALID_NODECOUNT
        missingnodes = job.nodecount
        logging.info("Skipping %s, skipped_invalid_nodecount", job.job_id)
    elif not job.has_any_archives():
        mergeresult = 1
        mdata["skipped_noarchives"] = True
        summarizeerror = ProcessingError.NO_ARCHIVES
        missingnodes = job.nodecount
        logging.info("Skipping %s, skipped_noarchives", job.job_id)
    elif not job.has_enough_raw_archives():
        mergeresult = 1
        mdata["skipped_rawarchives"] = True
        summarizeerror = ProcessingError.RAW_ARCHIVES
        missingnodes = job.nodecount
        logging.info("Skipping %s, skipped_rawarchives", job.job_id)
    elif opts['max_nodes'] > 0 and job.nodecount > opts['max_nodes']:
        mergeresult = 1
        mdata["skipped_job_too_big"] = True
        summarizeerror = ProcessingError.JOB_TOO_BIG
        missingnodes = job.nodecount
        logging.info("Skipping %s, skipped_job_too_big", job.job_id)
    elif opts['max_nodetime'] != None and (job.nodecount * job.walltime) > opts['max_nodetime']:
        mergeresult = 1
        mdata["skipped_job_nodehours"] = True
        summarizeerror = ProcessingError.JOB_TOO_MANY_NODEHOURS
        missingnodes = job.nodecount
        logging.info("Skipping %s, skipped_job_too_big (node time)", job.job_id)
    elif job.walltime >= opts['max_duration']:
        mergeresult = 1
        mdata["skipped_too_long"] = True
        summarizeerror = ProcessingError.TIME_TOO_LONG
        missingnodes = job.nodecount
        logging.info("Skipping %s, skipped_too_long", job.job_id)
    else:
        mergeresult = extract_and_merge_logs(job, conf, resconf, opts)
        missingnodes = -1.0 * mergeresult
    mergeend = time.time()

    if opts['extractonly']:
        return 0 == mergeresult

    preprocessors = [x(job) for x in preprocs]
    analytics = [x(job) for x in plugins]
    s = Summarize(preprocessors, analytics, job, conf)

    enough_nodes = False

    if 0 == mergeresult or (job.nodecount != 0 and (missingnodes / job.nodecount < 0.05)):
        enough_nodes = True
        logging.info("Success for %s files in %s (%s/%s)", job.job_id, job.jobdir, missingnodes, job.nodecount)
        s.process()
    elif summarizeerror == None and job.nodecount != 0 and (missingnodes / job.nodecount >= 0.05):
        # Don't overwrite existing error
        # Don't have enough node data to even try summarization
        mdata["skipped_pmlogextract_error"] = True
        logging.info("Skipping %s, skipped_pmlogextract_error", job.job_id)
        summarizeerror = ProcessingError.PMLOGEXTRACT_ERROR

    mdata["mergetime"] = mergeend - mergestart

    if opts['tag'] != None:
        mdata['tag'] = opts['tag']

    if missingnodes > 0:
        mdata['missingnodes'] = missingnodes

    success = s.good_enough()

    if not success and enough_nodes:
        # We get here if the pmlogextract step gave us enough nodes but summarization didn't succeed for enough nodes
        # All other "known" errors should already be handled above.
        mdata["skipped_summarization_error"] = True
        logging.info("Skipping %s, skipped_summarization_error", job.job_id)
        summarizeerror = ProcessingError.SUMMARIZATION_ERROR

    force_success = False
    if not success:
        force_timeout = opts['force_timeout']
        if (datetime.datetime.now() - job.end_datetime) > datetime.timedelta(seconds=force_timeout):
            force_success = True

    return s, mdata, success or force_success, summarizeerror


def override_defaults(resconf, opts):
    """ Commandline options that override the configuration file settings """
    if 'job_output_dir' in opts and opts['job_output_dir'] != None:
        resconf['job_output_dir'] = opts['job_output_dir']

    return resconf

def filter_plugins(resconf, preprocs, plugins):
    """ Filter the list of plugins/preprocs to use on a resource basis """

    # Default is to use all
    filtered_preprocs = preprocs
    filtered_plugins = plugins

    if "plugin_whitelist" in resconf:
        filtered_preprocs = [x for x in preprocs if x.__name__ in resconf['plugin_whitelist']]
        filtered_plugins = [x for x in plugins if x.__name__ in resconf['plugin_whitelist']]
    elif "plugin_blacklist" in resconf:
        filtered_preprocs = [x for x in preprocs if x.__name__ not in resconf['plugin_blacklist']]
        filtered_plugins = [x for x in plugins if x.__name__ not in resconf['plugin_blacklist']]

    return filtered_preprocs, filtered_plugins
