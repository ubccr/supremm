#!/usr/bin/env python
"""
    Main script for converting host-based pcp archives to job-level summaries.
"""

import logging
import os
import shutil
import time
import traceback
import multiprocessing as mp
from supremm.config import Config
from supremm.account import DbAcct
from supremm.xdmodaccount import XDMoDAcct
from supremm import outputter
from supremm.plugin import loadplugins, loadpreprocessors
from supremm.proc_common import getoptions, summarizejob, override_defaults, filter_plugins
from supremm.scripthelpers import setuplogger


def get_jobs(opts, account):
    """
    Returns an iterable of Jobs from the appropriate method of Accounting,
    as specified by the options
    """
    if opts['mode'] == "single":
        return account.getbylocaljobid(opts['local_job_id'])
    elif opts['mode'] == "timerange":
        return account.getbytimerange(opts['start'], opts['end'], opts)
    else:
        return account.get(None, None)


def clean_jobdir(opts, job):
    if opts['dodelete'] and job.jobdir is not None and os.path.exists(job.jobdir):
        # Clean up
        shutil.rmtree(job.jobdir)


def process_summary(m, dbif, opts, job, summarize_time, summarize, mdata, success, summarize_error):
    try:
        # TODO: change behavior so markasdone only happens if this is successful
        outputter_start = time.time()
        m.process(summarize, mdata)
        outputter_time = time.time() - outputter_start

        if not opts['dry_run']:
            # TODO: this attempts to emulate the old timing behavior. Keep it?
            process_time = summarize_time + outputter_time
            dbif.markasdone(job, success, process_time, summarize_error)
    except Exception as e:
        logging.error("Failure processing summary for job %s %s. Error: %s %s", job.job_id, job.jobdir, str(e), traceback.format_exc())


def processjobs(config, opts, process_pool=None):
    """ main function that does the work. One run of this function per process """

    allpreprocs = loadpreprocessors()
    logging.debug("Loaded %s preprocessors", len(allpreprocs))

    allplugins = loadplugins()
    logging.debug("Loaded %s plugins", len(allplugins))

    for r, resconf in config.resourceconfigs():
        if opts['resource'] is None or opts['resource'] == r or opts['resource'] == str(resconf['resource_id']):
            logging.info("Processing resource %s", r)
        else:
            continue

        resconf = override_defaults(resconf, opts)

        preprocs, plugins = filter_plugins(resconf, allpreprocs, allplugins)

        logging.debug("Using %s preprocessors", len(preprocs))
        logging.debug("Using %s plugins", len(plugins))
        if process_pool is not None:
            process_resource_multiprocessing(resconf, preprocs, plugins, config, opts, process_pool)
        else:
            process_resource(resconf, preprocs, plugins, config, opts)


def process_resource(resconf, preprocs, plugins, config, opts):
    with outputter.factory(config, resconf, dry_run=opts["dry_run"]) as m:

        if resconf['batch_system'] == "XDMoD":
            dbif = XDMoDAcct(resconf['resource_id'], config)
        else:
            dbif = DbAcct(resconf['resource_id'], config)

        for job in get_jobs(opts, dbif):
            try:
                summarize_start = time.time()
                result = summarizejob(job, config, resconf, plugins, preprocs, opts)
                summarize_time = time.time() - summarize_start
            except Exception as e:
                logging.error("Failure for summarization of job %s %s. Error: %s %s", job.job_id, job.jobdir, str(e), traceback.format_exc())
                clean_jobdir(opts, job)
                continue

            process_summary(m, dbif, opts, job, summarize_time, *result)
            clean_jobdir(opts, job)


def process_resource_multiprocessing(resconf, preprocs, plugins, config, opts, pool):
    with outputter.factory(config, resconf, dry_run=opts['dry_run']) as m:
        if resconf['batch_system'] == "XDMoD":
            dbif = XDMoDAcct(resconf['resource_id'], config)
        else:
            dbif = DbAcct(resconf['resource_id'], config)

        jobs = get_jobs(opts, dbif)

        it = iter_jobs(jobs, config, resconf, plugins, preprocs, opts)
        for job, result, summarize_time in pool.imap_unordered(do_summarize, it):
            if result is not None:
                process_summary(m, dbif, opts, job, summarize_time, *result)
                clean_jobdir(opts, job)
            else:
                clean_jobdir(opts, job)


def iter_jobs(jobs, config, resconf, plugins, preprocs, opts):
    """
    Combines the db cursor job iterator with the other information needed to pass to summarizejob.
    """
    for job in jobs:
        yield job, config, resconf, plugins, preprocs, opts


def do_summarize(args):
    """
    used in a separate process
    """
    job, config, resconf, plugins, preprocs, opts = args
    try:
        summarize_start = time.time()
        result = summarizejob(job, config, resconf, plugins, preprocs, opts)
        summarize_time = time.time() - summarize_start
    except Exception as e:
        logging.error("Failure for summarization of job %s %s. Error: %s %s", job.job_id, job.jobdir, str(e), traceback.format_exc())
        return job, None, None

    return job, result, summarize_time


def main():
    """
    main entry point for script
    """
    opts = getoptions(False)

    setuplogger(opts['log'])

    config = Config()

    threads = opts['threads']

    process_pool = mp.Pool(threads) if threads > 1 else None
    processjobs(config, opts, process_pool)

    if process_pool is not None:
        # wait for all processes to finish
        process_pool.close()
        process_pool.join()


if __name__ == "__main__":
    main()
