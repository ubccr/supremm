#!/usr/bin/env python
"""
    Main script for converting host-based pcp archives to job-level summaries.
"""

import logging
from multiprocessing import Process
from supremm.config import Config
from supremm.account import DbAcct
from supremm.xdmodaccount import XDMoDAcct
from supremm import outputter
from supremm.plugin import loadplugins, loadpreprocessors
from supremm.proc_common import getoptions, summarizejob, override_defaults, filter_plugins
from supremm.scripthelpers import setuplogger


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

        with outputter.factory(config, resconf) as m:

            if resconf['batch_system'] == "XDMoD":
                dbif = XDMoDAcct(resconf['resource_id'], config, opts['threads'], procid)
            else:
                dbif = DbAcct(resconf['resource_id'], config, opts['threads'], procid)

            if opts['mode'] == "single":
                for job in dbif.getbylocaljobid(opts['local_job_id']):
                    summarizejob(job, config, resconf, plugins, preprocs, m, dbif, opts)
            elif opts['mode'] == "timerange":
                for job in dbif.getbytimerange(opts['start'], opts['end'], opts):
                    summarizejob(job, config, resconf, plugins, preprocs, m, dbif, opts)
            else:
                for job in dbif.get(None, None):
                    summarizejob(job, config, resconf, plugins, preprocs, m, dbif, opts)


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
