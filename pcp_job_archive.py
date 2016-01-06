#!/usr/bin/python

"""
Generate per job PCP archive logs

Usage: pcp_job_archive [OPTIONS]

        Job ID/Description Options: (must include at least one)
        -a, --allusers                  Generate logs for jobs by all users.
                                        (True by default, but must be
                                        specified if no other options are used
                                        to prevent accidental huge job sets.)
        -A [ACCTS], --accounts=[ACCTS]  Generate logs for jobs by accounts in a
                                        given comma-separated list.
        -i, --incompletejobs            Search for jobs that were incomplete at
                                        some point during the given time span,
                                        but finished a full portion during it.
                                        See below for division options.
        -j [JOBID], --job=[JOBID]       Generate logs for job with given ID.
                                        To specify a portion of a job,
                                        append "-[PORTION]" to the ID and
                                        see below for mandatory options.
                                        ID Format: [CLUSTER]/[ID #]
        -M [CLUS], --clusters=[CLUS]    Generate logs for jobs that ran on the
                                        given comma-separated list of clusters.
                                        -1 may be used to specify all clusters.
                                        (All clusters included by default.)
        -n [NODES], --nodes=[NODES]     Generate logs for jobs that ran on the
                                        given comma-separated list of nodes.
                                        Nodes may be ranged strings.
                                        See sacct documentation.
                                        (All nodes are included by default.)
        -r [PARTS], --partition=[PARTS] Generate logs for jobs that ran in the
                                        partitions in the given
                                        comma-separated list.
                                        (All partitions included by default.)
        -u [USERS], --users=[USERS]     Generate logs for jobs by users in a
                                        given comma-separated list.

        Job Description Options: (required if not using a job ID)
        -e [ETIME], --endtime=[ETIME]   Generate logs for jobs ending before
                                        the given time. Time should be
                                        given in the following format:
                                        YYYY-MM-DDTHH:MM:SS
        -s [STIME], --starttime=[STIME] Generate logs for jobs ending after
                                        the given time. See above for format.

        Job Division Options: (required if dividing jobs into portions)
        -m [TIME], --maxlength=[TIME]   The maximum length of a job portion.
                                        Format: [DD-]HH:MM:SS
        -t [TIME], --tolerance=[TIME]   The tolerance allowed before a job will
                                        be divided. The max length plus this is
                                        the length at which a job will start to
                                        be divided.
                                        Format: [DD-]HH:MM:SS

        Other Options:
        -c [FILE], --pcpconfig=[FILE]   A PCP config file that will restrict
                                        which metrics are copied to job logs.
                                        See pmlogextract documentation.
        -d [DEST], --destination=[DEST] Store logs in the given directory.
                                        Defaults to location given in script.
        -o, --overwritejobdirs          Overwrite existing log directories
                                        for all jobs being processed.
        -p, --printjobsonly             No archiving is performed. Instead,
                                        the jobs that would have been archived
                                        with the given parameters are printed.
        -P, --printjobdata              Similar to the above print option, but
                                        also prints additional data as follows:
                                        JOBID|ELAPSED|NUMNODES

Author: Andrew E. Bruno <aebruno2@buffalo.edu>
Author: Tom Yearke <tyearke@buffalo.edu>
"""
import subprocess
import os,sys,getopt,shutil,logging,glob
import datetime
import threading
import time
import re
import copy

import pytz
import tzlocal

from pcp import pmapi
import cpmapi as c_pmapi

# The maximum number of times to retry fetching the next record in an archive.
max_fetch_retries = 100

# The domains the nodes may belong to.
node_domains = (
    'cbls.ccr.buffalo.edu',
    'ccr.buffalo.edu',
)

# The timezone used by the system(s) PCP logging is set up on.
pcp_system_time_zone = pytz.timezone("America/New_York")

# The directory the per-node logs are located in.
pcp_log_dir = '/data/pcp-logs'

# The directory the per-job logs will be placed in by default.
default_pcp_job_dir = '/data/pcp-jobs'

# The directory temporary copies of corrupted archives will be stored in.
tmp_log_dir = "/tmp/pcp_job_archive"

# The path to the log extraction tool.
pmlogextract_path = "pmlogextract"

# The template for a command to merge a set of logs.
pcp_merge_template = [
    pmlogextract_path,
    "-S", None,
    "-T", None
]

# The template for a command to create a non-corrupt copy of a corrupt PCP log.
pcp_log_corruption_fix_cmd_template = [
    pmlogextract_path,
    "-T", None,
    None, 
    None
]

# A base template for querying SLURM for job information.
slurm_cmd_template = [
    "sacct",
    "--format", "jobid,cluster,nodelist,nnodes,start,end,user,account",
    "-P",
    "-X",
    "-n"
]

# A command for querying SLURM with a given job cluster and ID.
job_id_slurm_cmd = slurm_cmd_template[:]
job_id_slurm_cmd.extend([
    "-M", None,
    "-j", None
])

# A command for querying SLURM with a given job description.
job_desc_slurm_cmd = slurm_cmd_template[:]
job_desc_slurm_cmd.extend([
    "-S", None,
    "-E", None
])

# A command for expanding a ranged list of nodes into a comma-separated list.
node_expand = [
    "nodeset",
    "-e",
    "-S", ",",
    None
]

# The time format used by SLURM and by this script's command line options.
slurm_time_format = "%Y-%m-%dT%H:%M:%S"

# The time format used by the archive merging tool.
pcp_time_format = "@ %a %b %d %H:%M:%S %Y"

# The time format used by subdirectories containing job archives and summaries.
subdir_datetime_format = "%Y-%m-%d"

# The regular expression used to convert time strings into timedeltas.
timedelta_regex = re.compile(r"((?P<days>\d+)-)?(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)$")

# The maximum length of a portion of a divided job. Specified on command line.
job_portion_length = None

# The tolerance allowed before a job will be divided. The maximum portion
# length plus this is the length at which a job will start to be divided.
# Specified on command line.
job_division_tolerance = None

# The length at which a job will start to be divided into portions.
# Determined by job_portion_length and job_division_tolerance.
minimum_divided_job_length = None

# Contains the data for a job. It is designed to be instantiated using
# lines from the SLURM tool split into arrays, so any changes to what's
# output by the tool should be reflected here.
class Job(object):

    def __init__(self, job_id, cluster, node_list_str, num_nodes_str, start_str, end_str, user, account):
        """
        Constructor. Arguments are elements in the lines output by the SLURM tool.
        """
        self.job_id = job_id
        self.cluster = cluster
        self.node_list_str = node_list_str
        self.num_nodes_str = num_nodes_str
        self.start_str = start_str
        self.end_str = end_str
        self.user = user
        self.account = account

        self.portion = None

        self._converted_start_str = None
        self._converted_end_str = None
        self._start_datetime = None
        self._end_datetime = None
        self._portion_calculation_portion = None
        self._portion_calculation_job_start = None
        self._portion_calculation_job_end = None
        self._portion_start_datetime = None
        self._portion_end_datetime = None

    @property
    def identifier(self):
        """
        Gets the identifier for this job, which is based on whether or not a
        portion has been specified.

        Returns:
            "[CLUSTER]/[ID]-[PORTION]" if a portion was specified, 
            otherwise "[CLUSTER]/[ID]".
        """
        if self.portion is not None:
            return "{0}/{1}-{2}".format(self.cluster, self.job_id, self.portion)

        return "{0}/{1}".format(self.cluster, self.job_id)

    @property
    def start_datetime(self):
        """
        Gets a datetime object representing the job's start time, or None
        if the string representation can't be converted.

        Returns:
            A datetime object representing the job's start time, or None
            if the string representation can't be converted.
        """
        if self._converted_start_str == self.start_str:
            return self._start_datetime

        self._start_datetime = safe_strptime(self.start_str, slurm_time_format)
        self._converted_start_str = self.start_str
        return self._start_datetime

    @property
    def end_datetime(self):
        """
        Gets a datetime object representing the job's end time, or None
        if the string representation can't be converted.

        Returns:
            A datetime object representing the job's end time, or None
            if the string representation can't be converted.
        """
        if self._converted_end_str == self.end_str:
            return self._end_datetime

        self._end_datetime = safe_strptime(self.end_str, slurm_time_format)
        self._converted_end_str = self.end_str
        return self._end_datetime

    @property
    def division_eligibility_time(self):
        """
        Gets the time at which this job became or will become eligible for division.
        If not eligible for division, returns None.
        """
        if minimum_divided_job_length is None:
            return None

        job_start = self.start_datetime
        if job_start is None:
            return None

        job_division_eligibility_time = job_start + minimum_divided_job_length

        job_end = self.end_datetime
        if (job_end is not None) and (job_division_eligibility_time > job_end):
            return None

        return job_division_eligibility_time

    @property
    def portion_start_datetime(self):
        """
        Gets the start datetime for the set portion of the job, or None
        if one doesn't exist.
        """
        self._calculate_portion()
        return self._portion_start_datetime

    @property
    def portion_end_datetime(self):
        """
        Gets the end datetime for the set portion of the job, or None
        if one doesn't exist.
        """
        self._calculate_portion()
        return self._portion_end_datetime

    @property
    def elapsed_timedelta(self):
        """
        Gets the amount of time elapsed during this (portion of a) job as a
        timedelta. If it cannot be calculated, None is returned.
        """
        if self.portion is not None:
            elapsed_start = self.portion_start_datetime
            elapsed_end = self.portion_end_datetime
        else:
            elapsed_start = self.start_datetime
            elapsed_end = self.end_datetime

        if (elapsed_start is None) or (elapsed_end is None):
            return None

        return elapsed_end - elapsed_start

    @property
    def elapsed_string(self):
        """
        Calculates the amount of time elapsed during this (portion of a) job
        using elapsed_timedelta and returns a string in the format
        [DD-]HH:MM:SS. If elapsed_timedelta returns None, an empty string is
        returned.
        """
        elapsed_td = self.elapsed_timedelta
        if elapsed_td is None:
            return ""

        elapsed_hours, rem_min_secs = divmod(elapsed_td.seconds, 3600)
        elapsed_minutes, elapsed_seconds = divmod(rem_min_secs, 60)
        elapsed_str = "{0:02}:{1:02}:{2:02}".format(elapsed_hours, elapsed_minutes, elapsed_seconds)

        if elapsed_td.days != 0:
            elapsed_str = "{0}-{1}".format(elapsed_td.days, elapsed_str)

        return elapsed_str

    def _calculate_portion(self):
        """
        If the portion, job start time, or job end time has changed,
        calculate the start and end times for the portion specified
        for this job.
        """
        if (self._portion_calculation_portion == self.portion) and (self._portion_calculation_job_start == self.start_datetime) and (self._portion_calculation_job_end == self.end_datetime):
            return

        self._portion_calculation_portion = self.portion
        self._portion_calculation_job_start = self.start_datetime
        self._portion_calculation_job_end = self.end_datetime

        if (self.portion is None) or (self.portion < 0) or (self._portion_calculation_job_start is None) or (job_portion_length is None):
            self._portion_start_datetime = None
            self._portion_end_datetime = None
            return

        self._portion_start_datetime = self._portion_calculation_job_start + (self.portion * job_portion_length)
        if (self._portion_calculation_job_end is not None) and (self._portion_start_datetime >= self._portion_calculation_job_end):
            self._portion_start_datetime = None
            self._portion_end_datetime = None
            return
            
        self._portion_end_datetime = self._portion_start_datetime + job_portion_length
        if (self._portion_calculation_job_end is not None) and (self._portion_end_datetime > self._portion_calculation_job_end):
            self._portion_end_datetime = self._portion_calculation_job_end

    def create_portion_copy(self, portion):
        """
        Create a copy of this job with the given portion.

        Args:
            portion: The portion to use in the new job object.
        Returns:
            A copy of this job object with the given portion.
        """
        portion_copy = copy.copy(self)
        portion_copy.portion = portion
        return portion_copy

    def get_final_portion_job(self):
        """
        Create a Job object corresponding to the last portion of this job
        if it is a divisible job that has completed. Otherwise, return this Job.
        
        Returns:
            A copy of this Job object for its last portion if it exists,
            otherwise this Job object.
        """
        if job_portion_length is None:
            return self

        job_start = self.start_datetime
        if job_start is None:
            return self

        job_end = self.end_datetime
        if job_end is None:
            return self

        if self.division_eligibility_time is None:
            return self

        remaining_job_length = job_end - job_start
        portion_index = 0
        while remaining_job_length > job_portion_length:
            remaining_job_length -= job_portion_length
            portion_index += 1

        return self.create_portion_copy(portion_index)

def get_archive_files(archive):
    """
    Gets the paths for existing archive files at the given base archive path.

    Args:
        archive: The base path to an archive. (Do not include file extensions.)
    Returns:
        A list of paths to the given archive's existing files. May be empty.
    """
    archive_files = []

    archive_meta_file = "{0}.meta".format(archive)
    if os.path.isfile(archive_meta_file):
        archive_files.append(archive_meta_file)

    archive_index_file = "{0}.index".format(archive)
    if os.path.isfile(archive_index_file):
        archive_files.append(archive_index_file)

    data_file_index = 0
    while True:
        archive_data_file = "{0}.{1}".format(archive, data_file_index)
        if not os.path.isfile(archive_data_file):
            break
        archive_files.append(archive_data_file)
        data_file_index += 1

    return archive_files

def safe_strptime(time_string, format):
    """
    Attempts to convert a string representing a time using the given time format.
    If it can't be converted, None will be returned instead of throwing an error.

    Args:
        time_string: The string to convert.
        format: The datetime format to use for conversion.
    Returns:
        A datetime object representing the given time if successfully converted,
        otherwise None.
    """
    try:
        converted_time = datetime.datetime.strptime(time_string, format)
    except ValueError:
        return None

    return converted_time

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

def get_timedelta_from_string(time_string):
    """
    Converts a string representing a time into a timedelta object.

    Args:
        time_string: The string to convert. Should use format [DD-]HH:MM:SS.
    Returns:
        A timedelta object representing the given time,
        or None if the string couldn't be converted.
    """
    timedelta_match = timedelta_regex.match(time_string)
    if not timedelta_match:
        return None

    days_group = timedelta_match.group("days")
    if days_group:
        days_int = int(days_group)
    else:
        days_int = 0
    hours_int = int(timedelta_match.group("hours"))
    minutes_int = int(timedelta_match.group("minutes"))
    seconds_int = int(timedelta_match.group("seconds"))

    return datetime.timedelta(days=days_int, hours=hours_int, minutes=minutes_int, seconds=seconds_int)

def get_utc_datetime_from_local_string(local_string, time_format, is_aware=False):
    """
    Converts a string in the given format in the system's local time to a 
    datetime object in UTC.

    Args:
        local_string: The time string to convert.
        time_format: The time format to use to convert the string.
        is_aware: (Optional) Create a datetime object that is time zone aware.
                (Defaults to naive.)
    Returns:
        A UTC datetime object for the given time. Naive or aware is controlled
        by the is_aware argument.
    Throws:
        ValueError: If the string could not be converted.
    """
    local_naive_datetime = datetime.datetime.strptime(local_string, time_format)
    local_aware_datetime = tzlocal.get_localzone().localize(local_naive_datetime)

    utc_datetime = local_aware_datetime.astimezone(pytz.utc)
    if not is_aware:
        utc_datetime = utc_datetime.replace(tzinfo=None)

    return utc_datetime

def get_utc_environ():
    """
    Creates a copy of this process' environment variables with the timezone
    variable set to UTC and returns it.

    Returns:
        A copy of os.environ with "TZ" set to "UTC".
    """
    utc_environ = os.environ.copy()
    utc_environ["TZ"] = "UTC"
    return utc_environ

def log_pipe(pipe, logging_function, template="%s"):
    """
    Logs each non-empty line from a pipe (or other file-like object)
    using the given logging function. This will block until the end of
    the pipe is reached.

    Args:
        pipe: The pipe to read from.
        logging_function: The logging function to use.
        template: (Optional) A template string to place each line from pipe
                  inside.
    """
    if (not pipe) or (not logging_function):
        return

    for line in pipe:
        stripped_line = line.rstrip()
        if stripped_line:
            logging_function(template % stripped_line)

def exists_ok_makedirs(path):
    """
    A wrapper for os.makedirs that does not throw an exception
    if the given path points to an existing directory.

    Args:
        path: The path to the directory to create.
    Throws:
        EnvironmentError: Thrown if the directory could not be created.
    """

    try:
        os.makedirs(path)
    except EnvironmentError:
        if not os.path.isdir(path):
            raise

def merge_logs(job, pcp_job_dir, pcp_config_path=None, overwrite_logs=False):
    """
    Takes a job description and merges logs for the time it ran.

    Args:
        job: A Job object describing the job to process.
        pcp_job_dir: The directory per-job logs will be placed in.
        pcp_config_path: (Optional) A path to a PCP config file to be used by the merging tool. Defaults to none.
        overwrite_logs: (Optional) Indicates if existing logs should be overwritten. Defaults to false.
    Returns:
        0 if the merge completed successfully. Otherwise, an error value.
    """

    logging.info("START cluster=%s jobid=%s portion=%s nodes=%s user=%s account=%s" % 
            (job.cluster, job.job_id, job.portion, job.num_nodes_str, job.user, job.account))

    # If this job description is only for a portion of a job,
    # use the portion's start and end times. Otherwise, use the
    # job's start and end times.
    is_job_portion = job.portion is not None
    if is_job_portion:
        start = job.portion_start_datetime
        end = job.portion_end_datetime
    else:
        start = job.start_datetime
        end = job.end_datetime

    # If the start time isn't available, log an error and return.
    if start is None:
        logging.error("Starting time not found. (Job may not have started or portion is invalid.)")
        return 1

    # If the end time is not available, use the current time and log a warning.
    if end is None:
        end = datetime.datetime.utcnow()
        logging.warning("End time not found. (Job may not have finished or portion is invalid.) Using current time as end time.")
    
    # Calculate the length of this job (portion) and log it.
    delta = end - start
    logging.info("      duration=%s" % delta)

    # Find the start and end times in the time zone used by the PCP loggers.
    start_in_pcp_system_tz = pytz.utc.localize(start).astimezone(pcp_system_time_zone)
    end_in_pcp_system_tz = pytz.utc.localize(end).astimezone(pcp_system_time_zone)

    # Find the first and last log dates to check and the number of log dates.
    first_log_date = start_in_pcp_system_tz.date() - datetime.timedelta(days=1)
    last_log_date = end_in_pcp_system_tz.date() + datetime.timedelta(days=1)
    num_log_dates = (last_log_date - first_log_date).days + 1

    # Attempt to parse the list of nodes. If the resulting list doesn't match
    # the given number of nodes in length, log an error and stop.
    node_expand[-1] = job.node_list_str
    proc = subprocess.Popen(node_expand,stdout=subprocess.PIPE,stderr=subprocess.PIPE)
    
    pipe_logger = threading.Thread(target=log_pipe, args=(proc.stderr, logging.warning, "nodeset error: %s"))
    pipe_logger.start()
    
    nodes = None
    node_list_invalid = False
    for line in proc.stdout:
        if nodes:
            continue
        node_set = line.rstrip()
        nodes = node_set.split(',')
        if len(nodes) != int(job.num_nodes_str):
            node_list_invalid = True

    pipe_logger.join()

    proc.wait()
    nodeset_rc = proc.returncode
    if nodeset_rc:
        node_list_invalid = True
        logging.warning("Non-zero nodeset return code: %s" % nodeset_rc)

    if node_list_invalid:
        logging.error("Invalid node list: %s" % job.node_list_str)
        return 1

    # Generate the path to the job's log directory.
    jobdir = os.path.join(pcp_job_dir, end_in_pcp_system_tz.strftime(subdir_datetime_format), job.cluster, job.job_id)
    if is_job_portion:
        jobdir = os.path.join(jobdir, str(job.portion))

    # If overwriting logs is enabled...
    if overwrite_logs:

        # If it exists, delete the directory the job logs will be stored in
        # and log a warning that it was deleted.
        try:
            shutil.rmtree(jobdir)
            logging.warning("Job directory %s existed and was deleted." % jobdir)
        except EnvironmentError:
            pass

    # Otherwise, if the log directory exists, log a warning and stop.
    else:
        if os.path.exists(jobdir):
            logging.error("Job directory '%s' already exists. Job skipped." % jobdir)
            return 1

    # Create the directory the job logs will be stored in. If an error
    # occurs, log an error and stop.
    try:
        os.makedirs(jobdir)
    except EnvironmentError:
        logging.error("Job directory %s could not be created." % jobdir)
        return 1

    # For every node the job ran on...
    node_error = False
    for n in nodes:

        # If there are no logs for the node, skip to the next node.
        logging.info("Searching for logs for node %s..." % n)

        possible_node_log_dirs = [
            os.path.join(pcp_log_dir, n),
        ]
        if node_domains:
            for node_domain in node_domains:
                possible_node_log_dirs.append(
                    os.path.join(pcp_log_dir, "{0}.{1}".format(n, node_domain))
                )

        node_log_dirs = [nld for nld in possible_node_log_dirs if os.path.isdir(nld)]
        if not node_log_dirs:
            logging.warning("No log directories found for node %s." % n)
            node_error = True
            continue

        # Add each log containing data for the job to the list. Data may be
        # in logs a day before or after the job's dates, depending on when
        # the logs are split up.
        archives = []
        file_suffix = ".meta"
        for delta_day in xrange(num_log_dates):
            log_day = first_log_date + datetime.timedelta(days=delta_day)
            log_day_file_str = log_day.strftime("%Y%m%d")
            log_files = []

            for node_log_dir in node_log_dirs:
                file_prefix = os.path.join(node_log_dir, log_day_file_str)
                log_files.extend(glob.glob("%s*%s" % (file_prefix, file_suffix)))

            if not log_files:
                logging.warning("No log files found for node %s for %s." % (n, log_day.strftime("%Y-%m-%d")))
                node_error = True
                continue

            for log_file in log_files:
                archives.append(log_file[:-len(file_suffix)])

        # Check the validity of found archives and include only valid archives.
        valid_archives = []
        for archive in archives:
            try:
                context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, archive)
            except pmapi.pmErr:
                logging.warning("Unable to open archive '{0}'. It has been excluded.".format(archive))
                node_error = True
                continue

            archive_start_time = None
            archive_end_time = None
            archive_corrupted = False
            archive_error = False
            fetch_retries = 0
            while True:
                result = None
                try:
                    result = context.pmFetchArchive()
                except pmapi.pmErr as e:
                    if (e.args[0] == c_pmapi.PM_ERR_EOL):
                        break
                    elif (e.args[0] == c_pmapi.PM_ERR_LOGREC):
                        archive_corrupted = True
                        break
                    else:
                        if (fetch_retries >= max_fetch_retries):
                            archive_error = True
                            node_error = True
                            logging.warning("Error occurred processing archive '{0}'. It has been excluded. ({1})".format(archive, e))
                            archive_start_time = None
                            break
                        else:
                            fetch_retries += 1
                            continue

                archive_end_time = get_datetime_from_pmResult(result)
                if archive_start_time is None:
                    archive_start_time = archive_end_time

                context.pmFreeResult(result)

            if (archive_start_time is None) or (archive_end_time is None):
                if not archive_error:
                    logging.info("Archive '{0}' is empty. It has been excluded.".format(archive))
                continue

            if (archive_end_time < start) or (archive_start_time > end):
                logging.info("Archive '{0}' does not fall within the job's time window. It has been excluded.".format(archive))
                continue

            if archive_corrupted:
                tmp_archive_dir = os.path.join(tmp_log_dir, n)
                tmp_archive_path = os.path.join(tmp_archive_dir, os.path.basename(archive))
                if not get_archive_files(tmp_archive_path):
                    try:
                        exists_ok_makedirs(tmp_archive_dir)
                    except EnvironmentError:
                        node_error = True
                        logging.warning("Could not make temporary folder for non-corrupt copy of '{0}'. Skipping archive.".format(archive))
                        continue

                    corruption_fix_cmd = pcp_log_corruption_fix_cmd_template[:]
                    corruption_fix_cmd[-3] = archive_end_time.strftime(pcp_time_format)
                    corruption_fix_cmd[-2] = archive
                    corruption_fix_cmd[-1] = tmp_archive_path

                    corruption_fix_proc = subprocess.Popen(corruption_fix_cmd, stderr=subprocess.PIPE, env=get_utc_environ())

                    log_pipe(corruption_fix_proc.stderr, logging.warning)

                    corruption_fix_proc_error = corruption_fix_proc.wait()
                    if corruption_fix_proc_error and (not get_archive_files(tmp_archive_path)):
                        node_error = True
                        logging.warning("Error occurred while creating non-corrupt version of '{0}'. Skipping archive. (Error: {1})".format(archive, corruption_fix_proc_error))
                        continue

                archive = tmp_archive_path

            valid_archives.append(archive)

        archives = valid_archives
        context = None

        # If no log files were found, skip to the next node.
        if not archives:
            logging.warning("No applicable log files found for node %s." % n)
            node_error = True
            continue

        # Report the list of archives that will be used.
        for archive in archives:
            logging.info("Added log '{0}'.".format(archive))

        # Merge the job logs for the node.
        node_archive = os.path.join(jobdir, n)
        logging.info("Merging logs for node %s..." % n)
        pcp_cmd = pcp_merge_template[:]
        pcp_cmd[2] = start.strftime(pcp_time_format)
        pcp_cmd[4] = end.strftime(pcp_time_format)
        if pcp_config_path is not None:
            pcp_cmd.append("-c")
            pcp_cmd.append(pcp_config_path)
        pcp_cmd.extend(archives)
        pcp_cmd.append(node_archive)
        proc = subprocess.Popen(pcp_cmd, stderr=subprocess.PIPE, env=get_utc_environ())

        # If error output was given while merging, log the output.
        log_pipe(proc.stderr, logging.warning)

        # If the merge process ended with a non-zero error code, record it.
        proc.wait()
        proc_error = proc.returncode
        if proc_error:
            logging.warning("Non-zero pmlogextract return code: %s" % proc_error)
            node_error = True

    return int(node_error)

def get_jobs(cmd):
    """
    Executes a SLURM command and returns a list of jobs parsed from the output.

    Args:
        cmd: The SLURM command to execute.
    Returns:
        A possibly-empty list of Job objects containing data from the output.
    """

    # Execute the given command.
    proc = subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.PIPE,env=get_utc_environ())
    
    # For every line of output from the command (a job description), 
    # read the data into a job object and add the object to a list.
    pipe_logger = threading.Thread(target=log_pipe, args=(proc.stderr, logging.warning, "sacct error: %s"))
    pipe_logger.start()

    delimiter = '|'
    jobs = [Job(*line.rstrip().split(delimiter)) for line in proc.stdout if delimiter in line]

    pipe_logger.join()

    proc.wait()
    proc_error = proc.returncode
    if proc_error:
        logging.warning("Non-zero sacct return code: %s" % proc_error)

    # Return the list of jobs.
    return jobs

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv
    try:

        # Parse and validate command line arguments.
        try:
            opts, args = getopt.getopt(argv[1:], "j:d:u:aA:n:r:s:e:c:opPim:t:M:", ["job=", "destination=", "users=", "allusers", "accounts=", "nodes=", "partition=", "starttime=", "endtime=", "pcpconfig=", "overwritejobdirs", "printjobsonly", "printjobdata", "incompletejobs", "maxlength=", "tolerance=", "clusters="])
        except getopt.error, msg:
            raise Usage(msg)

        job_id = None
        job_portion = None
        job_cluster = None
        pcp_job_dir = default_pcp_job_dir
        using_job_desc = False
        users = None
        all_users = False
        accounts = None
        nodes = None
        partitions = None
        start_time = None
        start_time_datetime = None
        end_time = None
        end_time_datetime = None
        pcp_config_path = None
        overwrite_logs = False
        print_jobs_only = False
        print_job_data = False
        search_for_incomplete_jobs = False
        global job_portion_length
        job_portion_length = None
        global job_division_tolerance
        job_division_tolerance = None
        job_division_options_required = False
        for opt in opts:
            option_name = opt[0]
            option_value = opt[1]

            if option_name in ("-j", "--job"):
                job_match = re.match(r"(?P<cluster>\S+)/(?P<id>\d+)(-(?P<portion>\d+))?$", option_value)
                if not job_match:
                    raise Usage("Given job ID has an invalid format.")

                job_cluster = job_match.group("cluster")
                job_id = int(job_match.group("id"))
                job_match_portion = job_match.group("portion")
                if job_match_portion is not None:
                    job_portion = int(job_match_portion)
                    job_division_options_required = True

            elif option_name in ("-d", "--destination"):
                pcp_job_dir = option_value

            elif option_name in ("-u", "--users"):
                users = option_value
                using_job_desc = True

            elif option_name in ("-a", "--allusers"):
                all_users = True
                using_job_desc = True

            elif option_name in ("-A", "--accounts"):
                accounts = option_value
                using_job_desc = True

            elif option_name in ("-n", "--nodes"):
                nodes = option_value
                using_job_desc = True

            elif option_name in ("-r", "--partition"):
                partitions = option_value
                using_job_desc = True

            elif option_name in ("-M", "--clusters"):
                job_cluster = option_value
                using_job_desc = True

            elif option_name in ("-i", "--incompletejobs"):
                search_for_incomplete_jobs = True
                using_job_desc = True
                job_division_options_required = True

            elif option_name in ("-s", "--starttime"):
                try:
                    start_time_datetime = get_utc_datetime_from_local_string(option_value, slurm_time_format)
                    start_time = start_time_datetime.strftime(slurm_time_format)
                except ValueError:
                    raise Usage("Given start time does not match expected format.")

            elif option_name in ("-e", "--endtime"):
                try:
                    end_time_datetime = get_utc_datetime_from_local_string(option_value, slurm_time_format)
                    end_time = end_time_datetime.strftime(slurm_time_format)
                except ValueError:
                    raise Usage("Given end time does not match expected format.")

            elif option_name in ("-m", "--maxlength"):
                job_portion_length = get_timedelta_from_string(option_value)
                if job_portion_length is None:
                    raise Usage("Invalid format for job portion length.")
                if job_portion_length < datetime.timedelta(seconds=1):
                    raise Usage("Job portion length must be positive.")

            elif option_name in ("-t", "--tolerance"):
                job_division_tolerance = get_timedelta_from_string(option_value)
                if job_division_tolerance is None:
                    raise Usage("Invalid format for job division tolerance.")

            elif option_name in ("-c", "--pcpconfig"):
                if not os.path.isfile(option_value):
                    raise Usage("PCP config file %s is not an existing file." % option_value)

                pcp_config_path = option_value

            elif option_name in ("-o", "--overwritejobdirs"):
                overwrite_logs = True

            elif option_name in ("-p", "--printjobsonly"):
                print_jobs_only = True

            elif option_name in ("-P", "--printjobdata"):
                print_job_data = True

        # Ensure that either a job ID or a job description was specified.
        using_job_id = job_id is not None
        if using_job_id and using_job_desc:
            raise Usage("Please use only a job ID or only a job description.")
        elif (not using_job_id) and (not using_job_desc):
            raise Usage("No job id or description options used.")

        # If both a job ID and a time was specified, report an error.
        # Do the same if using a job description and a time is missing.
        if using_job_id:
            if (start_time is not None) or (end_time is not None):
                raise Usage("Times should not be specified when using a job ID.")
        elif using_job_desc:
            if (start_time is None) or (end_time is None):
                raise Usage("Start and end time must both be specified for runs using job descriptions.")

        # Ensure that either all or none of the division options are given.
        job_division_enabled = job_portion_length is not None
        if job_division_enabled and (job_division_tolerance is None):
            raise Usage("If dividing jobs, all division options must be given.")
        if (not job_division_enabled) and (job_division_tolerance is not None):
            raise Usage("If dividing jobs, all division options must be given.")

        # If an option was used that requires job division, ensure that all
        # job division options were given.
        if job_division_options_required and (not job_division_enabled):
            raise Usage("If using job division, job division options must be specified.")

        # Ensure that at most one print option was specified.
        if print_jobs_only and print_job_data:
            raise Usage("Only one print option may be specified.")

        # Check that the source and destination directories exist.
        if not os.path.isdir(pcp_log_dir):
            raise Usage("Log directory '%s' is missing." % pcp_log_dir)
        if not os.path.isdir(pcp_job_dir):
            raise Usage("Destination '%s' is not an existing directory." % pcp_job_dir)

        # Obtain information about the specified jobs.
        if using_job_id:
            cmd = job_id_slurm_cmd[:]
            cmd[-3] = job_cluster
            cmd[-1] = str(job_id)
            jobs = get_jobs(cmd)
            if job_portion is not None:
                for job in jobs:
                    job.portion = job_portion
        elif using_job_desc:
            cmd = job_desc_slurm_cmd[:]
            cmd[-3] = start_time
            cmd[-1] = end_time

            cmd.append("--state")
            if search_for_incomplete_jobs:
                cmd.append("CONFIGURING,COMPLETING,PENDING,RUNNING,RESIZING,SUSPENDED")
            else:
                cmd.append("CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT")

            if users is not None:
                if all_users:
                    raise Usage("Users and all users options cannot be used simultaneously.")
                cmd.append("-u")
                cmd.append(users)
            else:
                cmd.append("-a")
            if accounts is not None:
                cmd.append("-A")
                cmd.append(accounts)
            if nodes is not None:
                cmd.append("-N")
                cmd.append(nodes)
            if partitions is not None:
                cmd.append("-r")
                cmd.append(partitions)
            if job_cluster is not None:
                cmd.append("-M")
                cmd.append(job_cluster)
            else:
                cmd.append("-L")

            jobs = get_jobs(cmd)
        else:
            raise Usage("No job id or description options used.")
        
        # If dividing jobs, calculate the minimum job length at which
        # division is performed.
        global minimum_divided_job_length
        if job_division_enabled:
            minimum_divided_job_length = job_portion_length + job_division_tolerance
        else:
            minimum_divided_job_length = None

        # If searching for jobs that completed full portions during the given
        # time span, narrow the job list accordingly. Also include earlier
        # portions if the job hit the divisibility point during the time span.
        if search_for_incomplete_jobs:
            
            incomplete_job_portions = []
            for job in jobs:
                job_division_eligibility_time = job.division_eligibility_time
                if job_division_eligibility_time is None:
                    continue

                if job_division_eligibility_time >= end_time_datetime:
                    continue

                job_end = job.end_datetime
                include_earlier_portions = job_division_eligibility_time >= start_time_datetime
                current_portion_job_index = 0
                current_portion_job = job.create_portion_copy(current_portion_job_index)
                current_portion_job_end = current_portion_job.portion_end_datetime
                while (current_portion_job_end is not None) and (current_portion_job_end < end_time_datetime) and ((job_end is None) or (current_portion_job_end < job_end)):
                    if include_earlier_portions or (current_portion_job_end >= start_time_datetime):
                        incomplete_job_portions.append(current_portion_job)
                    current_portion_job_index += 1
                    current_portion_job = job.create_portion_copy(current_portion_job_index)
                    current_portion_job_end = current_portion_job.portion_end_datetime

            jobs = incomplete_job_portions

        # Otherwise, if searching for completed jobs...
        elif using_job_desc:

            # Remove jobs that ended exactly on the end time. 
            jobs = [job for job in jobs if job.end_datetime != end_time_datetime]

            # If using job division, replace divisible jobs with their final
            # portions.
            if job_division_enabled:
                jobs = [job.get_final_portion_job() for job in jobs]

        # If no jobs were found, stop the script and print a warning message.
        if not jobs:
            logging.warning("No valid jobs or job portions found.")
            return 0

        # If the jobs are only being printed, print each job id and stop.
        if print_jobs_only:
            for job in jobs:
                print job.identifier

            return 0

        # If the jobs are being printed with additional data,
        # print the data and stop.
        if print_job_data:
            for job in jobs:
                job_elapsed_str = job.elapsed_string
                if not job_elapsed_str:
                    logging.warning("Elapsed time unknown for '{0}'.".format(job.identifier))
                    job_elapsed_str = "00:00:00"

                job_num_nodes_str = job.num_nodes_str
                if not job_num_nodes_str:
                    logging.warning("Number of nodes unknown for '{0}'.".format(job.identifier))
                    job_num_nodes_str = "0"

                print "|".join((job.identifier, job_elapsed_str, job_num_nodes_str))

            return 0

        # Merge logs for each job that was found.
        for job in jobs:
            logging.info("-- BEGIN MERGE --")
            start = time.time()
            merge_error = merge_logs(job, pcp_job_dir, pcp_config_path, overwrite_logs)
            if merge_error:
                logging.error("MERGE FAILED")
                print "%s|FAILED" % job.identifier
            else:
                print "%s|SUCCEEDED" % job.identifier
            elapsed = (time.time() - start)
            logging.info("Elapsed: %s" % elapsed)
            logging.info("-- END MERGE --")
        
        return 0

    except Usage, err:
        print >>sys.stderr, err.msg

        print >>sys.stderr, "Usage: pcp_job_archive [OPTIONS]"
        print >>sys.stderr, ""
        print >>sys.stderr, "        Job ID/Description Options: (must include at least one)"
        print >>sys.stderr, "        -a, --allusers                  Generate logs for jobs by all users."
        print >>sys.stderr, "                                        (True by default, but must be"
        print >>sys.stderr, "                                        specified if no other options are used"
        print >>sys.stderr, "                                        to prevent accidental huge job sets.)"
        print >>sys.stderr, "        -A [ACCTS], --accounts=[ACCTS]  Generate logs for jobs by accounts in a"
        print >>sys.stderr, "                                        given comma-separated list."
        print >>sys.stderr, "        -i, --incompletejobs            Search for jobs that were incomplete at"
        print >>sys.stderr, "                                        some point during the given time span,"
        print >>sys.stderr, "                                        but finished a full portion during it."
        print >>sys.stderr, "                                        See below for division options."
        print >>sys.stderr, "        -j [JOBID], --job=[JOBID]       Generate logs for job with given ID."
        print >>sys.stderr, "                                        To specify a portion of a job,"
        print >>sys.stderr, "                                        append \"-[PORTION]\" to the ID and"
        print >>sys.stderr, "                                        see below for mandatory options."
        print >>sys.stderr, "                                        ID Format: [CLUSTER]/[ID #]"
        print >>sys.stderr, "        -M [CLUS], --clusters=[CLUS]    Generate logs for jobs that ran on the"
        print >>sys.stderr, "                                        given comma-separated list of clusters."
        print >>sys.stderr, "                                        -1 may be used to specify all clusters."
        print >>sys.stderr, "                                        (All clusters included by default.)"
        print >>sys.stderr, "        -n [NODES], --nodes=[NODES]     Generate logs for jobs that ran on the"
        print >>sys.stderr, "                                        given comma-separated list of nodes."
        print >>sys.stderr, "                                        Nodes may be ranged strings."
        print >>sys.stderr, "                                        See sacct documentation."
        print >>sys.stderr, "                                        (All nodes are included by default.)"
        print >>sys.stderr, "        -r [PARTS], --partition=[PARTS] Generate logs for jobs that ran in the"
        print >>sys.stderr, "                                        partitions in the given"
        print >>sys.stderr, "                                        comma-separated list."
        print >>sys.stderr, "                                        (All partitions included by default.)"
        print >>sys.stderr, "        -u [USERS], --users=[USERS]     Generate logs for jobs by users in a"
        print >>sys.stderr, "                                        given comma-separated list."
        print >>sys.stderr, ""
        print >>sys.stderr, "        Job Description Options: (required if not using a job ID)"
        print >>sys.stderr, "        -e [ETIME], --endtime=[ETIME]   Generate logs for jobs ending before"
        print >>sys.stderr, "                                        the given time. Time should be"
        print >>sys.stderr, "                                        given in the following format:"
        print >>sys.stderr, "                                        YYYY-MM-DDTHH:MM:SS"
        print >>sys.stderr, "        -s [STIME], --starttime=[STIME] Generate logs for jobs ending after"
        print >>sys.stderr, "                                        the given time. See above for format."
        print >>sys.stderr, ""
        print >>sys.stderr, "        Job Division Options: (required if dividing jobs into portions)"
        print >>sys.stderr, "        -m [TIME], --maxlength=[TIME]   The maximum length of a job portion."
        print >>sys.stderr, "                                        Format: [DD-]HH:MM:SS"
        print >>sys.stderr, "        -t [TIME], --tolerance=[TIME]   The tolerance allowed before a job will"
        print >>sys.stderr, "                                        be divided. The max length plus this is"
        print >>sys.stderr, "                                        the length at which a job will start to"
        print >>sys.stderr, "                                        be divided."
        print >>sys.stderr, "                                        Format: [DD-]HH:MM:SS"
        print >>sys.stderr, ""
        print >>sys.stderr, "        Other Options:"
        print >>sys.stderr, "        -c [FILE], --pcpconfig=[FILE]   A PCP config file that will restrict"
        print >>sys.stderr, "                                        which metrics are copied to job logs."
        print >>sys.stderr, "                                        See pmlogextract documentation."
        print >>sys.stderr, "        -d [DEST], --destination=[DEST] Store logs in the given directory."
        print >>sys.stderr, "                                        Defaults to location given in script."
        print >>sys.stderr, "        -o, --overwritejobdirs          Overwrite existing log directories"
        print >>sys.stderr, "                                        for all jobs being processed."
        print >>sys.stderr, "        -p, --printjobsonly             No archiving is performed. Instead,"
        print >>sys.stderr, "                                        the jobs that would have been archived"
        print >>sys.stderr, "                                        with the given parameters are printed."
        print >>sys.stderr, "        -P, --printjobdata              Similar to the above print option, but"
        print >>sys.stderr, "                                        also prints additional data as follows:"
        print >>sys.stderr, "                                        JOBID|ELAPSED|NUMNODES"

        return 2

if __name__ == "__main__":
    # Set up the logging module.
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

    # Run the main function.
    sys.exit(main())
