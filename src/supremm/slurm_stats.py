#!/usr/bin/env python3
""" Parse directory containing slurm log """

import argparse
import json
import datetime
import math
import re
import gzip
import sys
import os
import pytz

from ClusterShell.NodeSet import NodeSet
from supremm.config import Config
from supremm import outputter


RUNLOG_FILE='.slurm_stats.json'

def formatmemory(value):

    unit = 'M'

    if value >= 1024 and value % 512 == 0:
        unit = 'G'
        value = value / 1024

    if math.floor(value) == value:
        return "{}{}".format(int(math.floor(value)), unit)

    return "{:.02f}{}".format(value, unit)

def getmemory(info, _):

    if 'tres' in info and 'allocated' in info['tres']:
        for record in info['tres']['allocated']:
            if record['type'] == 'mem':
                return formatmemory(record['count'])

    if 'tres' in info and 'requested' in info['tres']:
        for record in info['tres']['requested']:
            if record['type'] == 'mem':
                return formatmemory(record['count'])

    return formatmemory(0)

def getstrfield(info, field):
    return str(getfield(info, field))

def getfield(info, field):

    s = info
    for key in field.split('.'):
        if key in s:
            s = s[key]
        else:
            return "******** {} *********".format(field)

    return s

def serializetime(info, field):
    ts = getfield(info, field)
    if ts <= 0:
        return "Unknown"

    dt = datetime.datetime.utcfromtimestamp(ts)

    return dt.astimezone(pytz.timezone("Pacific/Samoa")).strftime("%Y-%m-%dT%H:%M:%S")

def gettresvaluestr(info, rtype, rname = None):
    return str(gettresvalue(info, rtype, rname))

def gettresvalue(info, rtype, rname = None):

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

def serializetres(info, ttype):

    out = []
    if 'tres' in info and ttype in info['tres']:
        for record in info['tres'][ttype]:
            if record['type'] == 'mem':
                out.append(record['type'] + "=" + formatmemory(record['count']))
            elif record['type'] in ['node', 'cpu', 'billing']:
                out.append(record['type'] + "=" + str(record['count']))
            elif record['type'] == 'gres':
                out.append( record['type'] + '/' + record['name'] + "=" + str(record['count']))

    out.sort()

    return ",".join(out)

def getarrayjobid(info, _ = None):
    if 'array' in info and 'job_id' in info['array'] and info['array']['job_id'] != 0:
        return str(info['array']['job_id']) + "_" + str(info['array']['task_id'])

    return str(info['job_id'])

def getarrayjobinfo(info, _ = None):
    if 'array' in info and 'job_id' in info['array'] and info['array']['job_id'] != 0:
        return (str(info['array']['job_id']) + "_" + str(info['array']['task_id']), info['array']['job_id'], info['array']['task_id'])

    return (str(info['job_id']), str(info['job_id']), -1)

def durationseconds(info, field):
    time_minutes = getfield(info, field)
    if time_minutes is None:
        return 0
    return time_minutes * 60

def mintostring(info, field):
    time_minutes = getfield(info, field)
    if time_minutes is None:
        return "Partition_Limit"
    return timetostr(time_minutes * 60)

def secondstostring(info, field):
    time_seconds = getfield(info, field)
    return timetostr(time_seconds)

def timetostr(intime):
    seconds = intime % 60
    time = intime // 60
    minutes = time % 60
    hours = time // 60 % 24
    days = time // 1440

    if days < 0 or hours < 0 or minutes < 0 or seconds < 0:
        return "INVALID"

    if days > 0:
        return "{}-{:0>2}:{:0>2}:{:0>2}".format(days, hours, minutes, seconds)

    return "{:0>2}:{:0>2}:{:0>2}".format(hours, minutes, seconds)

def starttime_ts(info):

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

def starttime(info, _ = None):

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

    dt = datetime.datetime.utcfromtimestamp(ts)

    return dt.astimezone(pytz.timezone("Pacific/Samoa")).strftime("%Y-%m-%dT%H:%M:%S")
    #return dt.strftime("%Y-%m-%dT%H:%M:%S")

def getnamefield(info, field):
    name = getstrfield(info, field)

    return name.strip('“”')

def getexitcode(info, field):

    state = getfield(info, 'state.current')
    if state == 'FAILED':
        return "1:0"

    s = getfield(info, field)

    return_code = s['return_code']
    signal = s['signal']['signal_id'] if 'signal' in s else 0

    if return_code is None:
        return_code = signal
        signal = 0

    return "{}:{}".format(return_code, signal)

def slurm_job_to_supremm(job, resource_id):

    if job['nodes'] == 'None assigned':
        return None, None

    now = datetime.datetime.now(datetime.timezone.utc)
    job_uniq_id, local_job_id, local_job_array_index = getarrayjobinfo(job)
    ncpus = gettresvalue(job, 'cpu')
    endtime = getfield(job, 'time.end')
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
            "reqmem" : getmemory(job, 'allocated'),
            "eligible" : getfield(job, 'time.eligible'),
            "exit_code" : getexitcode(job, 'derived_exit_code'),
            "submit" : getfield(job, 'time.submission'),
            "reqcpus" : getfield(job, 'required.CPUs'),
            "nodes" : gettresvalue(job, 'node'),
            "job_uniq_id" : job_uniq_id,
            "start_time" : starttime_ts(job),
            "jobname" : getnamefield(job, 'name'),
            "user" : getfield(job, 'user'),
            "gpus" : gettresvalue(job, 'gres', 'gpu'),
            "resource_manager" : "slurm",
            "account" : getfield(job, 'account'),
            "partition" : getfield(job, 'partition'),
            "timelimit": durationseconds(job, 'time.limit'),
            "end_time" : endtime
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

    endproc = datetime.datetime.now(datetime.timezone.utc)
    mdata = {
        "created" : now.timestamp(),
        "elapsed" : (endproc - now) / datetime.timedelta(milliseconds = 1) / 1000.0
    }

    return out, mdata

def process_file(entry, config, resconf, dryrun):

    job_count = 0

    with gzip.open(entry.path, "r") as jfile:
        alldata = json.load(jfile)
        with outputter.factory(config, resconf, dry_run=dryrun) as outdb:
            for data in alldata['jobs']:
                job, mdata = slurm_job_to_supremm(data, resconf['resource_id'])
                if job is None:
                    continue

                outdb.process(job, mdata)
                job_count += 1

                if job_count % 500 == 0:
                    print("Processed {} jobs".format(job_count))


# jobid,jobidraw,cluster,partition,qos,account,group,gid,user,uid,\
# submit,eligible,start,end,elapsed,exitcode,state,nnodes,ncpus,reqcpus,reqmem,\
# reqtres,alloctres,timelimit,nodelist,jobname

def process_file_sacct(filename):

    fields = [
            ('_', getarrayjobid),
            ('job_id', getstrfield),
            ('cluster', getfield),
            ('partition', getfield),
            # temp disabled ('qos', getfield),
            ('account', getfield),
            ('group', getfield),
            #('gid', not supported),
            ('user', getfield),
            #('uid', not supported),
            ('time.submission', serializetime),
            ('time.eligible', serializetime),
            ('time.start', starttime),
            ('time.end', serializetime),
            ('time.elapsed', secondstostring),
            ('derived_exit_code', getexitcode),
            ('state.current', getfield),
            ('node', gettresvaluestr),
            ('cpu', gettresvaluestr),
            ('required.CPUs', getstrfield),
            ('allocated', getmemory),
            ('requested', serializetres),
            ('allocated', serializetres),
            ('time.limit', mintostring),
            ('nodes', getfield),
            ('name', getnamefield)
    ]

    with gzip.open(filename, "r") as jfile:
        alldata = json.load(jfile)
        for data in alldata['jobs']:
            if getstrfield(data, 'state.current') in ['RUNNING', 'PENDING']:
                continue

            output = []
            for field, fn in fields:
                try:
                    output.append(fn(data, field))
                except TypeError as e:
                    print(e)
                    print(field)
                    print(data)
                    return

            print("|".join(output))

def print_help():
    print("Usage: {} PATH_TO_FILES")

def getrunlog():
    mlog = {'last_mtime': 0}

    try:
        with open(RUNLOG_FILE, 'r', encoding='utf8') as fp:
            mlog = json.load(fp)
    except FileNotFoundError:
        pass

    return mlog

def saverunlog(mlog):
    with open(RUNLOG_FILE, 'w', encoding='utf8') as fp:
        json.dump(mlog, fp, indent=4)

def main():

    parser = argparse.ArgumentParser(
                    prog='slurm_stats',
                    description='Process the json output of slurm\'s sacct command')

    parser.add_argument('dirpath')
    parser.add_argument('--dryrun', '--dry-run', '--noop', '--no-op', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')  # on/off flag

    args = parser.parse_args()

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
                print("Skip old file", entry.path)
        else:
            print("Skip unknown resource", entry.path)


    if not args.dryrun:
        saverunlog(mlog)

if __name__ == "__main__":
    main()
