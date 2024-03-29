#!/usr/bin/env python3
""" Parse directory containing slurm log """

import argparse
import json
import datetime
import re
import gzip
import os
import logging

from ClusterShell.NodeSet import NodeSet
from supremm.config import Config
from supremm import outputter
from supremm.scripthelpers import setuplogger


import pymongo

RUNLOG_FILE='.slurm_stats.json'



def getfield(info, field):
    """ helper function to get a nested field name from an object
    """
    s = info
    for key in field.split('.'):
        if key in s:
            s = s[key]
        else:
            return None

    return s

def gettresvalue(info, rtype, rname = None):
    """ helper function to get a a TRES value from a slurm json accounting
        record. This follows a similar algorithm used by sacct. If the job
        has been allocated resource then the allocated value is used, otherwise
        the requested value is used. 0 is returned if no data found.
    """

    if 'tres' in info and 'allocated' in info['tres']:
        for record in info['tres']['allocated']:
            if record['type'] == rtype:
                if rname is None or rname == record['name']:
                    return record['count']

    if 'tres' in info and 'requested' in info['tres']:
        for record in info['tres']['requested']:
            if record['type'] == rtype:
                if rname is None or rname == record['name']:
                    return record['count']

    return 0

def getarrayjobinfo(info, _ = None):
    """ helper function to get the job_id, job_id_raw and job_array_index from a slurm
        json accounting record.
    """
    if 'array' in info and 'job_id' in info['array'] and info['array']['job_id'] != 0:
        return (str(info['array']['job_id']) + "_" + str(info['array']['task_id']), info['array']['job_id'], info['array']['task_id'])

    return (str(info['job_id']), str(info['job_id']), -1)

def durationseconds(info, field):
    """ helper function to get the time in seconds from a slurm minutes field
    """
    time_minutes = getfield(info, field)
    if time_minutes is None:
        return 0
    if isinstance(time_minutes, int):
        return time_minutes * 60
    elif 'number' in time_minutes:
        return time_minutes['number'] * 60
    else:
        raise Exception('syntax error')

def starttime_ts(info):
    """ helper function to get the start time. This follows the same algorithm
        as the slurm sacct code
    """
    ts = None
    for step in info['steps']:
        ts1 = step['time']['start']
        if ts is None:
            ts = ts1
        else:
            ts = min(ts, ts1)

    if ts is None:
        if info['time']['elapsed'] == 0:
            ts = info['time']['end']
        else:
            ts = info['time']['start']

    return ts

def getexitcode(info, field):
    """ helper function to ge the exit code from a slurm sacct json record
    """
    state = getfield(info, 'state.current')
    if state == 'FAILED':
        return "1:0"

    s = getfield(info, field)

    return_code = s['return_code']
    signal = s['signal']['signal_id'] if 'signal' in s else 0

    if return_code is None:
        return_code = signal
        signal = 0

    return f"{return_code}:{signal}"

def slurm_job_to_supremm(job, resource_id):
    """ generate a supremm job summary record from a slurm job accounting record
        formt he slurm sacct --json format.
    """
    if job['nodes'] == 'None assigned':
        return None, None

    now = datetime.datetime.now(datetime.timezone.utc)
    job_uniq_id, local_job_id, local_job_array_index = getarrayjobinfo(job)
    ncpus = gettresvalue(job, 'cpu')
    exit_status =  getfield(job, 'state.current')

    if exit_status == 'RUNNING':
        return None, None

    out = {
        "procDump" : {
            "unconstrained" : [ ],
            "constrained" : [ ],
            "cpusallowed" : {
            },
            "slurm": job
        },
        "created" : now,
        "acct" : {
            "exit_status" : exit_status,
            "qos": getfield(job, 'qos'),
            "resource_id" : resource_id,
            "ncpus" : ncpus,
            "host_list" : list(NodeSet(job['nodes'])),
            "id" : job_uniq_id,
            "local_job_id_raw" : getfield(job, 'job_id'),
            "local_job_id" : local_job_id,
            "local_job_array_index" : local_job_array_index,
            "group" : getfield(job, 'group'),
            "reqmem" : gettresvalue(job, 'mem'),
            "eligible" : getfield(job, 'time.eligible'),
            "exit_code" : getexitcode(job, 'derived_exit_code'),
            "submit" : getfield(job, 'time.submission'),
            "reqcpus" : getfield(job, 'required.CPUs'),
            "nodes" : gettresvalue(job, 'node'),
            "job_uniq_id" : job_uniq_id,
            "start_time" : starttime_ts(job),
            "jobname" : getfield(job, 'name'),
            "user" : getfield(job, 'user'),
            "gpus" : gettresvalue(job, 'gres', 'gpu'),
            "resource_manager" : "slurm",
            "account" : getfield(job, 'account'),
            "partition" : getfield(job, 'partition'),
            "timelimit": durationseconds(job, 'time.limit'),
            "end_time" : getfield(job, 'time.end')
        },
        "cpu" : {
            "jobcpus" : {
                "error" : 2
            },
            "nodecpus" : {
                "error" : 2
            },
            "effcpus" : {
                "error" : 2
            }
        },
        "process_memory": {
            "error": 2
        },
        "summarization": {
            "complete": True,
            "version" : "1.0.6"
        }
    }

    job_total_time = ncpus * getfield(job, 'time.elapsed')
    job_user_time = 0.0
    job_system_time = 0.0

    for step in job['steps']:
        if 'time' in step:
            job_user_time += step['time']['user']['seconds'] + step['time']['user']['microseconds'] / 1000000.0
            job_system_time += step['time']['system']['seconds'] + step['time']['system']['microseconds'] / 1000000.0

    if job_total_time > 0 and job_user_time + job_system_time <= job_total_time:
        out['cpu']['jobcpus'] = {
            'all': { 'cnt': ncpus },
            'user': { 'cnt': ncpus, 'avg': job_user_time / job_total_time },
            'system': { 'cnt': ncpus, 'avg': job_system_time / job_total_time },
            'idle': { 'cnt': ncpus, 'avg': (job_total_time - job_user_time - job_system_time) / job_total_time }
        }

    JOB_STEP_THRESHOLD = 500
    if len(out['procDump']['slurm']['steps']) > JOB_STEP_THRESHOLD:
        out['procDump']['notes'] = 'The job had {} job steps. Only the first {} shown.'.format(len(out['procDump']['slurm']['steps']), JOB_STEP_THRESHOLD)
        out['procDump']['slurm']['steps'] = out['procDump']['slurm']['steps'][0:100]
        logging.debug("Had to truncate data for %s", json.dumps(out['acct'], indent=4, default=str))

    endproc = datetime.datetime.now(datetime.timezone.utc)
    mdata = {
        "created" : now.timestamp(),
        "elapsed" : (endproc - now) / datetime.timedelta(milliseconds = 1) / 1000.0
    }

    return out, mdata

def process_file(entry, config, resconf, dryrun):
    """ process a slurm json file for a configured resource
    """
    job_count = 0

    with gzip.open(entry.path, "r") as jfile:
        alldata = json.load(jfile)
        with outputter.factory(config, resconf, dry_run=dryrun) as outdb:
            for data in alldata['jobs']:
                try:
                    job, mdata = slurm_job_to_supremm(data, resconf['resource_id'])
                except Exception as e:
                    logging.error(entry.path)
                    logging.error(json.dumps(data, indent=4))
                    raise e
                if job is None:
                    continue

                try:
                    outdb.process(job, mdata)
                except pymongo.errors.DocumentTooLarge as e:
                    logging.error(entry.path)
                    logging.error(json.dumps(mdata, indent=4, default=str))
                    logging.error(json.dumps(job, indent=4, default=str))
                    raise e

                job_count += 1

                if job_count % 500 == 0:
                    logging.info(f"Processed {job_count} jobs")


def getrunlog():
    """ Get the object containing the metadata from the most
        recent succesful run on the process
    """
    mlog = {'last_mtime': 0}

    try:
        with open(RUNLOG_FILE, 'r', encoding='utf8') as fp:
            mlog = json.load(fp)
    except FileNotFoundError:
        pass

    return mlog

def saverunlog(mlog):
    """ Save to file the object containing the run metadata
    """
    with open(RUNLOG_FILE, 'w', encoding='utf8') as fp:
        json.dump(mlog, fp, indent=4)

def main():
    """ main entry point
    """
    parser = argparse.ArgumentParser(
                    prog='slurm_stats',
                    description='Process the json output of slurm\'s sacct command')

    parser.add_argument('dirpath')
    parser.add_argument('--dryrun', '--dry-run', '--noop', '--no-op', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')  # on/off flag

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.WARNING
    setuplogger(log_level)

    exp = re.compile("^sacct_json_([a-z_]+)_([0-9]{4}-[0-9]{2}-[0-9]{2}).json.gz$")

    config = Config('../../config')
    resmap = {}
    for r, resconf in config.resourceconfigs():
        resmap[r.lower().replace(' ', '_')] = resconf

    entries = []
    with os.scandir(args.dirpath) as it:
        for entry in it:
            mtch = exp.match(entry.name)
            if mtch:
                entries.append((entry, mtch.group(1)))


    mlog = getrunlog()
    last_mtime = mlog['last_mtime']

    entries.sort(key=lambda x: x[0].name)

    for fp in entries:
        entry = fp[0]
        resource = fp[1]

        if resource in resmap:
            if entry.stat().st_mtime > last_mtime:
                process_file(entry, config, resmap[resource], args.dryrun)
                mlog['last_mtime'] = max(mlog['last_mtime'], entry.stat().st_mtime)
            else:
                logging.debug("Skip old file %s", entry.path)
        else:
            logging.debug("Skip unknown resource %s", entry.path)


    if not args.dryrun:
        saverunlog(mlog)

if __name__ == "__main__":
    main()
