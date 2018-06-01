#!/usr/bin/env python
"""
    Main script for converting host-based pcp archives to job-level summaries.
"""

import logging
import os
import shutil
import time
import traceback
from multiprocessing import Process
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


def processjobs(config, opts, procid):
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

        with outputter.factory(config, resconf, dry_run=opts["dry_run"]) as m:

            if resconf['batch_system'] == "XDMoD":
                dbif = XDMoDAcct(resconf['resource_id'], config, opts['threads'], procid)
            else:
                dbif = DbAcct(resconf['resource_id'], config, opts['threads'], procid)

            for job in get_jobs(opts, dbif):
                try:
                    summarize_start = time.time()
                    summarize, mdata, success, summarize_error = summarizejob(job, config, resconf, plugins, preprocs, opts)
                    summarize_time = time.time() - summarize_start

                    # TODO: change behavior so markasdone only happens if this is successful
                    m.process(summarize, mdata)

                    if not opts['dry_run']:
                        dbif.markasdone(job, success, summarize_time, summarize_error)

                except Exception as e:
                    logging.error("Failure for job %s %s. Error: %s %s", job.job_id, job.jobdir, str(e), traceback.format_exc())

                finally:
                    if opts['dodelete'] and job.jobdir is not None and os.path.exists(job.jobdir):
                        # Clean up
                        shutil.rmtree(job.jobdir)


def main():
    """
    main entry point for script
    """
    opts = getoptions(False)

    setuplogger(opts['log'])

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
            proc.join()


if __name__ == "__main__":
    main()
