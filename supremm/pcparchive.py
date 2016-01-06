#!/usr/bin/env python
"""
    pcp archive processing functions
"""
import logging
import datetime
import os
import shutil
import subprocess

from pcp import pmapi
import cpmapi as c_pmapi

def get_datetime_from_timeval(tv):
    """
    Converts a PCP timeval object into a datetime object.

    Args:
        tv: The timeval object to convert.
    Returns:
        A naive datetime object representing the timeval object's time in UTC.
    """
    while not isinstance(tv, pmapi.timeval):
        tv = tv.contents
    dt = datetime.datetime.utcfromtimestamp(tv.tv_sec)
    dt = dt.replace(microsecond=tv.tv_usec)
    return dt


def get_datetime_from_pmResult(result):
    """
    Converts the timestamp of a pmResult into a datetime object.

    Args:
        result: The pmResult whose timestamp is being converted.
    Returns:
        A naive datetime object representing the result's timestamp in UTC.
    """
    return get_datetime_from_timeval(result.contents.timestamp)


def process_slurm_metadata(context, job, nodename):
    """
    Check the archive for the slurm pmda data and use it to update the
    job end timestamp.
    """
    newbegin = None
    newend = None

    try:
        mdata = context.pmGetArchiveLabel()
        context.pmSetMode(c_pmapi.PM_MODE_FORW, mdata.start, 0)
        slurmmetric = context.pmLookupName("slurm.node.job.state")

        done = False

        while not done:
            result = context.pmFetch(slurmmetric)

            for i in xrange(result.contents.numpmid):
                if result.contents.get_pmid(i) != slurmmetric[0]:
                    continue
                for j in xrange(result.contents.get_numval(i)):
                    sjobid = str(result.contents.get_inst(i, j))
                    if sjobid == job.job_id:
                        atom = context.pmExtractValue(result.contents.get_valfmt(i),
                                                      result.contents.get_vlist(i, j),
                                                      c_pmapi.PM_TYPE_STRING,
                                                      c_pmapi.PM_TYPE_STRING)
                        value = atom.cp
                        if value == "begin":
                            newbegin = get_datetime_from_pmResult(result)
                        elif value == "end":
                            newend = get_datetime_from_pmResult(result)
                            done = True

            context.pmFreeResult(result)

    except pmapi.pmErr as e:
        if e.args[0] == c_pmapi.PM_ERR_EOL:
            # Ok, just reached the end of file
            pass
        elif e.args[0] == c_pmapi.PM_ERR_NAME:
            # No slurm metrics available
            return None
        else:
            logging.warning("Failed to process slurm metrics for job %s. %s", job.job_id, e.message())
            return None

    job.setnodebeginend(nodename, newbegin, newend)


def extract_and_merge_logs(job, conf, resconf):
    """ merge all of the raw pcp archives into one archive per node for each
        node in the job """

    for nodename, filepaths in job.rawarchives():
        for fname in filepaths:
            context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, fname)
            process_slurm_metadata(context, job, nodename)

    return pmlogextract(job, conf, resconf)


def getextractcmdline(startdate, enddate, inputarchives, outputarchive):
    """ build the pmlogextract commmandline """

    # The time format used by the archive merging tool.
    pcp_time_format = "@ %Y-%m-%d %H:%M:%S UTC"

    cmdline = ["pmlogextract",
               "-S", startdate.strftime(pcp_time_format),
               "-T", enddate.strftime(pcp_time_format)]

    cmdline.extend(inputarchives)

    cmdline.append(outputarchive)

    return cmdline

def genoutputdir(job, conf, resconf):
    """ compute the per job archive directory path based on config options """
    
    if 'job_output_dir' in resconf:
        jobdir = resconf['job_output_dir']
    else:
        pathconf = conf.getsection("summary")

        # %r means the resource name
        # %j the local job id
        # the rest is sent to strftime with the end time of the job
        subdir = pathconf['subdir_out_format'].replace("%r", resconf['name']) .replace("%j", job.job_id)
        subdir = job.end_datetime.strftime(subdir)

        jobdir = os.path.join(pathconf['archive_out_dir'], subdir)

    logging.debug("jobdir is %s", jobdir)

    return jobdir

def pmlogextract(job, conf, resconf):
    """
    Takes a job description and merges logs for the time it ran.

    Args:
        job: A Job object describing the job to process.
        pcp_job_dir: The directory per-job logs will be placed in.
        pcp_log_dir: The directory containing the source PCP archives, one subdir per host
    Returns:
        0 if the merge completed successfully. Otherwise, an error value.
    """


    logging.info("START resource=%s %s", resconf['name'], str(job))

    # Generate the path to the job's log directory.
    jobdir = genoutputdir(job, conf, resconf)

    if os.path.exists(jobdir):
        try:
            shutil.rmtree(jobdir)
            logging.debug("Job directory %s existed and was deleted.", jobdir)
        except EnvironmentError:
            pass

    # Create the directory the job logs will be stored in. If an error
    # occurs, log an error and stop.
    try:
        os.makedirs(jobdir)
    except EnvironmentError:
        logging.error("Job directory %s could not be created.", jobdir)
        return 1

    job.setjobdir(jobdir)

    # For every node the job ran on...
    node_error = 0
    for nodename, nodearchives in job.rawarchives():

        # Merge the job logs for the node.
        node_archive = os.path.join(jobdir, nodename)
        pcp_cmd = getextractcmdline(job.getnodebegin(nodename), job.getnodeend(nodename), nodearchives, node_archive)

        logging.debug("Calling %s", " ".join(pcp_cmd))
        proc = subprocess.Popen(pcp_cmd, stderr=subprocess.PIPE)

        (_, errdata) = proc.communicate()

        if errdata != None and len(errdata) > 0:
            logging.warning(errdata)
            job.record_error(errdata)

        if proc.returncode:
            errmsg = "pmlogextract return code: %s source command was: %s" % (proc.returncode, " ".join(pcp_cmd))
            logging.warning(errmsg)
            node_error -= 1
            job.record_error(errmsg)
        else:
            job.addnodearchive(nodename, node_archive)

    return node_error

