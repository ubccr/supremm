#!/usr/bin/env python
"""
    Main script for converting host-based pcp archives to job-level summaries.
"""

import logging
import os
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
from supremm.datasource.factory import DatasourceFactory


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


def process_summary(m, dbif, opts, job, summarize_time, result):
    summary, mdata, success, summarize_error = result
    try:
        # TODO: change behavior so markasdone only happens if this is successful
        outputter_start = time.time()
        m.process(summary, mdata)
        outputter_time = time.time() - outputter_start

        if not opts['dry_run']:
            # TODO: this attempts to emulate the old timing behavior. Keep it?
            process_time = summarize_time + outputter_time
            dbif.markasdone(job, success, process_time, summarize_error)
    except Exception as e:
        logging.error("Failure processing summary for job %s %s. Error: %s %s", job.job_id, job.jobdir, str(e), traceback.format_exc())
        if opts["fail_fast"]:
            raise


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
        datasource = DatasourceFactory(preprocs, plugins, resconf)

        logging.debug("Using %s preprocessors", len(preprocs))
        logging.debug("Using %s plugins", len(plugins))
        if process_pool is not None:
            process_resource_multiprocessing(resconf, config, opts, datasource, process_pool)
        else:
            process_resource(resconf, config, opts, datasource)


def process_resource(resconf, config, opts, datasource):
    with outputter.factory(config, resconf, dry_run=opts["dry_run"]) as m:

        if resconf['batch_system'] == "XDMoD":
            dbif = XDMoDAcct(resconf, config)
        else:
            dbif = DbAcct(resconf, config)

        for job in get_jobs(opts, dbif):
            try:
                summarize_start = time.time()
                jobmeta = datasource.presummarize(job, config, resconf, opts)
                if not jobmeta:
                    continue # Extract-only mode for PCP datasource
                res = datasource.summarizejob(job, jobmeta, config, opts)
                s, mdata, success, s_err = res
                summarize_time = time.time() - summarize_start
                summary_dict = s.get()
            except Exception as e:
                logging.error("Failure for summarization of job %s %s. Error: %s %s", job.job_id, job.jobdir, str(e), traceback.format_exc())
                datasource.cleanup(opts, job)
                if opts["fail_fast"]:
                    raise
                else:
                    continue

            process_summary(m, dbif, opts, job, summarize_time, (summary_dict, mdata, success, s_err))
            datasource.cleanup(opts, job)


def process_resource_multiprocessing(resconf, config, opts, datasource, pool):
    with outputter.factory(config, resconf, dry_run=opts['dry_run']) as m:
        if resconf['batch_system'] == "XDMoD":
            dbif = XDMoDAcct(resconf, config)
        else:
            dbif = DbAcct(resconf, config)

        jobs = get_jobs(opts, dbif)

        it = iter_jobs(jobs, config, resconf, plugins, preprocs, opts)
        pool_iter = pool.imap_unordered(do_summarize, it)
        while True:
            try:
                job, result, summarize_time = pool_iter.next(timeout=600000)
            except StopIteration:
                break

            if result is not None:
                process_summary(m, dbif, opts, job, summarize_time, result)
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
        res = summarizejob(job, config, resconf, plugins, preprocs, opts)
        if res is None:
            return job, None, None  # Extract-only mode
        s, mdata, success, s_err = res
        summarize_time = time.time() - summarize_start
        # Ensure Summarize.get() is called on worker process since it is cpu-intensive
        summary_dict = s.get()
    except Exception as e:
        logging.error("Failure for summarization of job %s %s. Error: %s %s", job.job_id, job.jobdir, str(e), traceback.format_exc())
        if opts["fail_fast"]:
            raise
        return job, None, None

    return job, (summary_dict, mdata, success, s_err), summarize_time


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
