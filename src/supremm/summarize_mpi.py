#!/usr/bin/env python
"""
    Main script for converting host-based pcp archives to job-level summaries.
"""
import os
import shutil
import traceback

from mpi4py import MPI

import logging
from supremm.config import Config
from supremm.account import DbAcct
from supremm.xdmodaccount import XDMoDAcct
from supremm import outputter
from supremm.plugin import loadplugins, loadpreprocessors
from supremm.proc_common import getoptions, summarizejob, override_defaults, filter_plugins
from supremm.scripthelpers import setuplogger

import sys
import time
import psutil
import json

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

        resconf = override_defaults(resconf, opts)

        preprocs, plugins = filter_plugins(resconf, allpreprocs, allplugins)

        logging.debug("Using %s preprocessors", len(preprocs))
        logging.debug("Using %s plugins", len(plugins))

        with outputter.factory(config, resconf, dry_run=opts["dry_run"]) as m:

            if resconf['batch_system'] == "XDMoD":
                dbif = XDMoDAcct(resconf['resource_id'], config)
            else:
                dbif = DbAcct(resconf['resource_id'], config)

            list_procs = 0

            # Master
            if procid == 0:
                getjobs = {}
                if opts['mode'] == "single":
                    getjobs['cmd'] = dbif.getbylocaljobid
                    getjobs['opts'] = [opts['local_job_id'],]
                elif opts['mode'] == "timerange":
                    getjobs['cmd'] = dbif.getbytimerange
                    getjobs['opts'] = [opts['start'], opts['end'], opts]
                else:
                    getjobs['cmd'] = dbif.get
                    getjobs['opts'] = [None, None]

                logging.debug("MASTER STARTING")
                numworkers = opts['threads']-1
                numsent = 0
                numreceived = 0


                for job in getjobs['cmd'](*(getjobs['opts'])):
                    if numsent >= numworkers:
                        list_procs += 1
                        if opts['dump_proclist'] and (list_procs == 1 or list_procs == 1000):
                            # Once all ranks are going, dump the process list for debugging
                            logging.info("Dumping process list")
                            allpinfo = {}
                            for proc in psutil.process_iter():
                                try:
                                    pinfo = proc.as_dict()
                                except psutil.NoSuchProcess:
                                    pass
                                else:
                                    allpinfo[pinfo['pid']] = pinfo

                            with open("rank-{}_{}.proclist".format(procid, list_procs), 'w') as outfile:
                                json.dump(allpinfo, outfile, indent=2)

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

                logging.info("After all jobs sent: %d sent, %d received", numsent, numreceived)

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
                sendtime = time.time()
                midtime = time.time()
                recvtime = time.time()
                logging.debug("WORKER %d STARTING", procid)
                while True:
                    recvtries = 0
                    while not comm.Iprobe(source=0, tag=1):
                        if recvtries < 1000:
                            recvtries += 1
                            continue
                        # Sleep so we can instrument how efficient we are
                        # Otherwise, workers spin on exit at the hidden mpi_finalize call.
                        # If you care about maximum performance and don't care about wasted cycles, remove the Iprobe/sleep loop
                        # Empirically, a tight loop with time.sleep(0.001) uses ~1% CPU
                        time.sleep(0.001)
                    job = comm.recv(source=0, tag=1)
                    recvtime = time.time()
                    mpisendtime = midtime-sendtime
                    mpirecvtime = recvtime-midtime
                    if (mpisendtime+mpirecvtime) > 2:
                        logging.warning("MPI send/recv took %s/%s", mpisendtime, mpirecvtime)
                    if job != None:
                        logging.debug("Rank: %s, Starting: %s", procid, job.job_id)
                        process_job(config, dbif, job, m, opts, plugins, preprocs, resconf)
                        logging.debug("Rank: %s, Finished: %s", procid, job.job_id)
                        sendtime = time.time()
                        comm.send(procid, dest=0, tag=1)
                        midtime = time.time()

                        list_procs += 1
                        if opts['dump_proclist'] and (list_procs == 1 or list_procs == 10):
                            # Once all ranks are going, dump the process list for debugging
                            logging.info("Dumping process list")
                            allpinfo = {}
                            for proc in psutil.process_iter():
                                try:
                                    pinfo = proc.as_dict()
                                except psutil.NoSuchProcess:
                                    pass
                                else:
                                    allpinfo[pinfo['pid']] = pinfo

                            with open("rank-{}_{}.proclist".format(procid, list_procs), 'w') as outfile:
                                json.dump(allpinfo, outfile, indent=2)
                    else:
                        # Got shutdown message
                        break


def process_job(config, dbif, job, m, opts, plugins, preprocs, resconf):
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

    comm = MPI.COMM_WORLD

    opts = getoptions(True)

    opts['threads'] = comm.Get_size()

    logout = "mpiOutput-{}.log".format(comm.Get_rank())

    # For MPI jobs, do something sane with logging.
    setuplogger(logging.ERROR, logout, opts['log'])

    config = Config()

    if comm.Get_size() < 2:
        logging.error("Must run MPI job with at least 2 processes")
        sys.exit(1)

    myhost = MPI.Get_processor_name()
    logging.info("Nodename: %s", myhost)

    processjobs(config, opts, comm.Get_rank(), comm)

    logging.info("Rank: %s FINISHED", comm.Get_rank())

if __name__ == "__main__":
    main()
