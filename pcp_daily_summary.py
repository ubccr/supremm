#!/usr/bin/python

"""
pcp_daily_summary.py
PCP Scripts

Performs daily generation and summarization of per-job PCP logs,
using the pcp_job_archive and pcp_summary scripts.

Usage: pcp_daily_summary [options]

        Options:
        -a [NUM_PROCS], --archiveprocs=[NUM_PROCS]  The number of archive
                                                    processes to run. Use <1
                                                    to disable this step.
                                                    Defaults to 1.
        -d [END_DATE], --date=[END_DATE]            The end date of jobs to be
                                                    summarized. May be a range
                                                    of dates. Defaults to
                                                    yesterday. Format:
                                                    YYYY-MM-DD[-YYYY-MM-DD]
        -m [NUM_PROCS], --mongoprocs=[NUM_PROCS]    The number of Mongo load
                                                    processes to run. Use <1
                                                    to disable this step.
                                                    Defaults to 1.
        -s [NUM_PROCS], --summaryprocs=[NUM_PROCS]  The number of summary
                                                    processes to run. Use <1
                                                    to disable this step.
                                                    Defaults to 1.


Author: Tom Yearke <tyearke@buffalo.edu>
"""

from datetime import date, datetime, timedelta
import getopt
import glob
import imp
import json
import logging
import multiprocessing
import os
import Queue
import re
import shutil
import StringIO
import subprocess
import sys
import threading

import pytz

# The timezone used by the system(s) PCP logging is set up on.
pcp_system_time_zone = pytz.timezone("America/New_York")

# The location job summaries will be stored in.
summaries_dir = "/data/pcp-summaries/"

# The location the job archive generation script stores temporary copies of
# corrupted archives. This will be deleted after all job archives have been created.
pcp_job_archive_tmp_dir = "/tmp/pcp_job_archive"

# The regular expression used to parse the job end date command line argument.
input_job_end_date_regex = re.compile(r"(?P<first_date>\d{4}-\d{1,2}-\d{1,2})(-(?P<second_date>\d{4}-\d{1,2}-\d{1,2}))?$")

# The date format for the job end date command line argument.
input_job_end_date_format = "%Y-%m-%d"

# The format used by subdirectories containing job archives and summaries.
subdir_datetime_format = "%Y-%m-%d"

# The format used by the summarization script to output times.
job_info_datetime_format = "%Y-%m-%dT%H:%M:%S.%fZ"

# The name of the job archive generation script.
pcp_job_archive_script_name = "pcp_job_archive.py"

# The directory of the job archive generation script.
# None may be used to indicate the same folder as this script.
pcp_job_archive_script_dir = None

# The name of the job summary script.
pcp_summary_script_name = "pcp_summary.py"

# The directory of the job summary script.
# None may be used to indicate the same folder as this script.
pcp_summary_script_dir = None

# The name of the Mongo load script.
mongo_load_script_name = "pcpprocess.py"

# The directory of the Mongo load script.
# None may be used to indicate the same folder as this script.
mongo_load_script_dir = None

# The maximum length of a job portion in a string format that the
# archival and summarization scripts can parse.
max_job_portion_length = "3-00:00:00"

# The tolerance allowed before a job will be divided into portions.
# The maximum portion length plus this is the length at which a job
# will start to be divided. This is in a string format that the
# archival script can parse.
job_division_tolerance = "00:05:00"

# The command to obtain a list of completed jobs to process from the 
# job archive generation script. The values for the script location,
# start time, and end time are set by this script.
pcp_job_archive_print_complete_cmd = [
    None,
    "-P",
    "-a",
    "-m", max_job_portion_length,
    "-t", job_division_tolerance,
    "-s", None,
    "-e", None
]

# A template command to run the job archive generation script on a job.
# The script location and job ID are set by this script.
pcp_job_archive_run_cmd_template = [
    None,
    "-m", max_job_portion_length,
    "-t", job_division_tolerance,
    "-j", None
]

# A template command to run the summary script.
# The script location and job ID are set by this script.
pcp_summary_cmd_template = [
    None,
    "-m", max_job_portion_length,
    "-j", None,
    summaries_dir
]

# A template command to load summary files into Mongo.
# The script location and number of threads are set by this script.
mongo_load_cmd_template = [
    None,
    "-N", None
]

# The location to store the output of the job archive generation script runs.
pcp_job_archive_log_path = "/data/script-logs/pcp-job-archive-logs/"

# The location to store the output of the summary script runs.
pcp_summary_log_path = "/data/script-logs/pcp-summary-logs/"

# The location to store error files before they can be placed with summaries.
unfiled_error_file_path = "/data/script-logs/unfiled-error-files/"

# The string format of time arguments for the job archive generation script.
pcp_job_archive_time_format = "%Y-%m-%dT%H:%M:%S"

# The string format of the time in the name of job archive generation logs.
pcp_script_log_time_format = "%Y-%m-%d-%H-%M-%S"

# The string used to store script logs in a time-based subdirectory.
# Set in main().
log_dir_time_str = ""

# A queue of jobs to be archived.
jobs_to_archive = Queue.PriorityQueue()

# A queue of jobs to be summarized.
jobs_to_summarize = Queue.PriorityQueue()

# A list of jobs that failed to be archived.
failed_archival_jobs = []

# The lock controlling access to the failed archival jobs list.
failed_archival_jobs_lock = threading.Lock()

# A list of jobs that failed to be summarized.
failed_summary_jobs = []

# The lock controlling access to the failed summary jobs list.
failed_summary_jobs_lock = threading.Lock()

# A cached mapping of jobs to their summary directories, used for filing logs.
job_summary_dirs = {}

# A cache containing the individual components for a job identifier as a tuple.
cached_job_components = {}

# The lock controlling access to the cached job components dictionary.
cached_job_components_lock = threading.Lock()

# A regular expression for parsing jobs with portions specified.
job_regex = re.compile(r"(?P<cluster>\S+)/(?P<id>\d+)(-(?P<portion>\d+))?$")

# The regular expression used to convert time strings into timedeltas.
timedelta_regex = re.compile(r"((?P<days>\d+)-)?(?P<hours>\d+):(?P<minutes>\d+):(?P<seconds>\d+)$")

# The amount of time, in seconds, to wait before checking if a process is alive.
process_timeout_time = 5

# An enumeration used to convey results between processes.
class WorkerResult:
    SUCCESS = 0
    SILENT_FAILURE = 1
    LOUD_FAILURE = 2

def setup_logging():
    """
    Sets up the logging module using its basicConfig function and a predefined
    output format.
    """
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

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

    return timedelta(days=days_int, hours=hours_int, minutes=minutes_int, seconds=seconds_int)

def get_job_components(job):
    """
    Uses the job regular expression to split up a given job identifier and
    return its components in a tuple.

    Args:
        job: An identifier for a job in the expected string format.
    Returns:
        A tuple containing strings for the job cluster, ID #, and portion. If any
        components could not be found, None will be used in their places.
    """
    with cached_job_components_lock:
        if job in cached_job_components:
            return cached_job_components[job]

    job_cluster = None
    job_id = None
    job_portion = None

    job_match = job_regex.match(job)
    if job_match:
        job_cluster = job_match.group("cluster")
        job_id = job_match.group("id")
        job_portion = job_match.group("portion")

    job_components_tuple = (job_cluster, job_id, job_portion)
    with cached_job_components_lock:
        cached_job_components[job] = job_components_tuple
    return job_components_tuple

def get_log_path_end(log_name, extension=".txt", include_script_time_dir=True):
    """
    Gets the end of the path for a log file with the given name.
    This can include log_dir_time_str, which should be set before calling this
    function.

    Args:
        log_name: The name of the log whose path is being generated.
        extension: (Optional) An additional string to be placed on the end of
                the returned string. Defaults to ".txt".
        include_script_time_dir: (Optional) Include the subdirectory this
                script run is storing its logs in. Defaults to True.
    Returns:
        The end of the path for the log with the given name.
    """
    log_path_end = log_name

    if extension:
        log_path_end += extension

    if include_script_time_dir:
        log_path_end = os.path.join(log_dir_time_str, log_path_end)

    return log_path_end

def get_job_log_path_end(job, extension=".txt", include_script_time_dir=True):
	"""
	Gets the end of a path for a job's log files from the given job identifier.
    This can include log_dir_time_str, which should be set before calling this
    function.

	Args:
		job: The job identifier for the job whose path is being generated.
		extension: (Optional) An additional string to be placed on the end of
				the returned string. Defaults to ".txt".
        include_script_time_dir: (Optional) Include the subdirectory this
                script run is storing its logs in. Defaults to True.
	Returns:
		The end of the path for a job's logs.
	"""
	(job_cluster, job_id, job_portion) = get_job_components(job)
	if job_id is not None:
		if job_portion is not None:
			job_log_name = os.path.join(job_cluster, "{0}-{1}".format(job_id, job_portion))
		else:
			job_log_name = os.path.join(job_cluster, job_id)
	else:
		job_log_name = job

	return get_log_path_end(job_log_name, extension=extension, include_script_time_dir=include_script_time_dir)

def try_create_dir_for_file(file_path):
    """
    Creates a directory for the file at the given path,
    if the directory doesn't already exist.

    Args:
        file_path: A path to a file to create the directory for.
    Throws:
        EnvironmentError: Thrown by os.makedirs if directory couldn't be created.
    """

    dir_path = os.path.dirname(file_path)
    try:
        os.makedirs(dir_path)
    except EnvironmentError:
        if not os.path.isdir(dir_path):
            raise

def record_job_error(job, message):
    """
    Records in an error file for the given job the given error message.

    Args:
        job: The job the error occurred with.
        message: The message to record.
    """

    job_error_path = os.path.join(unfiled_error_file_path, get_job_log_path_end(job))
    try:
        try_create_dir_for_file(job_error_path)
    except EnvironmentError:
        logging.error("Could not create unfiled error directory.")
        return

    try:
        error_file = open(job_error_path, "a")
    except EnvironmentError:
        logging.error("Could not open error file for job %s." % job)
        return

    try:
        print >>error_file, message
    except EnvironmentError:
        logging.error("Could not write to error file for job %s." % job)

    try:
        error_file.close()
    except EnvironmentError:
        pass

def start_worker_process(target, script_path, log_path_template):
    """
    Creates and starts a worker process that runs using the given function and
    imports the script at the given path. Wrapper functions should be called
    instead of using this function directly.

    Args:
        target: The function the worker process will use to run.
        script_path: The path to the script the worker process will import.
        log_path_template: The template for where the process will store its
            output from stdout and stderr. {0} will become the process name.
    Returns:
        A tuple containing the worker process and a bidirectional pipe
        connection to the process.
    """
    worker_conn, parent_conn = multiprocessing.Pipe()

    worker_proc = multiprocessing.Process(target=target, args=(script_path, worker_conn, os.getpid(), log_path_template))
    worker_proc.start()

    return (worker_proc, parent_conn)

def start_archiver_process():
    """
    Creates and starts an archiver process. This function only works after the
    archiver script has been found in the main function.

    Returns:
        A tuple containing the archiver process and a bidirectional pipe
        connection to the process.
    """
    log_path_template = os.path.join(pcp_job_archive_log_path, get_log_path_end("{0}"))
    return start_worker_process(archiver_process_function, pcp_job_archive_run_cmd_template[0], log_path_template)

def start_summarizer_process():
    """
    Creates and starts a summarizer process. This function only works after the
    summarizer script has been found in the main function.

    Returns:
        A tuple containing the summarizer process and a bidirectional pipe
        connection to the process.
    """
    log_path_template = os.path.join(pcp_summary_log_path, get_log_path_end("{0}"))
    return start_worker_process(summarizer_process_function, pcp_summary_cmd_template[0], log_path_template)

def archiver_process_function(script_path, conn_to_parent, parent_id, process_log_path_template):
    """
    The function run by archiver processes to continually archive job logs.
    Processes will import the archival script and use it to archive jobs
    as they are received from the main process.

    NOTE: The method for checking if the parent process is alive only works
    on Unix-based platforms.

    Args:
        script_path: The path to the archival script.
        conn_to_parent: A pipe connection to the parent process.
        parent_id: The process ID of the parent process.
        process_log_path_template: A path template for where the process will
            store its output from stdout and stderr. {0} will become the
            process name.
    """

    # Redirect stdout and stderr to a log file for the process.
    process_log_path = process_log_path_template.format(multiprocessing.current_process().name)
    process_log_file = open(process_log_path, "w")
    sys.stdout = process_log_file
    sys.stderr = process_log_file

    # Setup the logging module for this process.
    setup_logging()
    root_logger = logging.getLogger()
    root_logger_original_handlers = root_logger.handlers[:]
    root_logger_first_handler = root_logger_original_handlers[0]

    # Initialize variables.
    output_delimiter = "|"

    # Import the archival script.
    pcp_job_archive = imp.load_source("pcp_job_archive", script_path)

    # For the life of the process...
    while True:

        # Wait for the main process to send a job to archive. If a timeout
        # occurs, check if the parent is still alive, and if it is not,
        # stop this process. The method for checking on the parent was found
        # at the link below, and the method only works on Unix-based platforms.
        #
        # http://stackoverflow.com/questions/2542610/python-daemon-doesnt-kill-its-kids
        parent_died = False
        while not conn_to_parent.poll(process_timeout_time):
            if os.getppid() != parent_id:
                parent_died = True
                break

        if parent_died:
            break

        # Receive the job from the main process.
        job = conn_to_parent.recv()

        # If the job is a poison pill, end this process.
        if job is None:
            break

        # Unpack the job.
        job_args, job_log_path = job

        # Open a log file for processing the job.
        # If one can't be opened, report the error and skip this job.
        job_log_opened = False
        try:
            try_create_dir_for_file(job_log_path)
            job_log_file = open(job_log_path, "w")
            job_log_opened = True
        except EnvironmentError:
            pass

        if job_log_opened:
            # Switch stdout to a buffer and stderr to the job log file.
            stdout_buffer = StringIO.StringIO()
            sys.stdout = stdout_buffer
            sys.stderr = job_log_file

            # Replace the logger's handlers with one that outputs to the new
            # stderr location.
            job_stream_handler = logging.StreamHandler()
            job_stream_handler.setFormatter(root_logger_first_handler.formatter)
            job_stream_handler.setLevel(root_logger_first_handler.level)
            for logging_handler in root_logger_original_handlers:
                root_logger.removeHandler(logging_handler)
            root_logger.addHandler(job_stream_handler)

            # Process the job.
            archival_error = pcp_job_archive.main(job_args)

            # Check if the job was successful and create a result to return.
            archival_successful = False
            for line in stdout_buffer.getvalue().splitlines():
                if output_delimiter not in line:
                    continue

                archival_job, archival_status = line.rstrip().split(output_delimiter)
                if (archival_job == job) and (archival_status == "SUCCEEDED"):
                    archival_successful = True

            if archival_error:
                archival_successful = False

            if archival_successful:
                job_results = (WorkerResult.SUCCESS, "")
            else:
                job_results = (WorkerResult.SILENT_FAILURE, "pcp_job_archive reported failure.")

            # Return the original handlers to the logger.
            root_logger.removeHandler(job_stream_handler)
            for logging_handler in root_logger_original_handlers:
                root_logger.addHandler(logging_handler)

            # Return stdout and stderr to the process log file.
            sys.stdout = process_log_file
            sys.stderr = process_log_file

            # Close the job log file.
            try:
                job_log_file.close()
            except EnvironmentError:
                pass
        else:
            job_results = (WorkerResult.LOUD_FAILURE, "Could not create log at '{0}'.".format(job_log_path))

        # Send the results to the main process.
        conn_to_parent.send(job_results)

def archiver_thread_function():
    """
    The function run by archiver threads to continually archive job logs.
    Threads will take a job from a queue and send them to a worker process.
    """

    # Initialize variables.
    run_cmd = pcp_job_archive_run_cmd_template[:]

    # Start a worker process.
    worker_proc, conn_to_worker = start_archiver_process()

    # For the life of the thread...
    while True:

        # Get a job from the queue.
        job_queue_entry = jobs_to_archive.get()
        job = job_queue_entry[1]

        # If the job is a poison pill, stop the worker process and this thread.
        if job is None:
            conn_to_worker.send(None)
            worker_proc.join()
            jobs_to_archive.task_done()
            break

        # Generate the path to the job's log file.
        log_path = os.path.join(pcp_job_archive_log_path, get_job_log_path_end(job))

        # Generate main function arguments for the job.
        run_cmd[-1] = job

        # Send the job's details to the worker process.
        conn_to_worker.send((run_cmd, log_path))

        # Wait for results from the worker process. If the process dies,
        # start up a new process and assume failure.
        worker_died = False
        while not conn_to_worker.poll(process_timeout_time):
            if not worker_proc.is_alive():
                worker_died = True
                worker_proc, conn_to_worker = start_archiver_process()
                worker_results = (WorkerResult.SILENT_FAILURE, "Worker process died.")
                break

        if not worker_died:
            worker_results = conn_to_worker.recv()

        # If failure occurred, report it. If it was a loud failure, send the
        # error message to the logger.
        worker_result_type = worker_results[0]
        if worker_result_type != WorkerResult.SUCCESS:
            if worker_result_type == WorkerResult.LOUD_FAILURE:
                logging.error(worker_results[1])

            record_job_error(job, "Error occurred while creating job-specific archive(s).")
            with failed_archival_jobs_lock:
                failed_archival_jobs.append(job)

        # Add the job to the summary queue.
        jobs_to_summarize.put(job_queue_entry)

        # Report that the job has been handled.
        jobs_to_archive.task_done()

def summarizer_process_function(script_path, conn_to_parent, parent_id, process_log_path_template):
    """
    The function run by summarizer processes to continually summarize job logs.
    Processes will import the summarizer script and use it to summarize jobs
    as they are received from the main process.

    NOTE: The method for checking if the parent process is alive only works
    on Unix-based platforms.

    Args:
        script_path: The path to the archival script.
        conn_to_parent: A pipe connection to the parent process.
        parent_id: The process ID of the parent process.
        process_log_path_template: A path template for where the process will
            store its output from stdout and stderr. {0} will become the
            process name.
    """

    # Redirect stdout and stderr to a log file for the process.
    process_log_path = process_log_path_template.format(multiprocessing.current_process().name)
    process_log_file = open(process_log_path, "w")
    sys.stdout = process_log_file
    sys.stderr = process_log_file

    # Setup the logging module for this process.
    setup_logging()
    root_logger = logging.getLogger()
    root_logger_original_handlers = root_logger.handlers[:]
    root_logger_first_handler = root_logger_original_handlers[0]

    # Import the archival script.
    pcp_summary = imp.load_source("pcp_summary", script_path)

    # For the life of the process...
    while True:

        # Wait for the main process to send a job to summarize. If a timeout
        # occurs, check if the parent is still alive, and if it is not,
        # stop this process. The method for checking on the parent was found
        # at the link below, and the method only works on Unix-based platforms.
        #
        # http://stackoverflow.com/questions/2542610/python-daemon-doesnt-kill-its-kids
        parent_died = False
        while not conn_to_parent.poll(process_timeout_time):
            if os.getppid() != parent_id:
                parent_died = True
                break

        if parent_died:
            break

        # Receive the job from the main process.
        job = conn_to_parent.recv()

        # If the job is a poison pill, end this process.
        if job is None:
            break

        # Unpack the job.
        job_args, job_log_path = job

        # Open a log file for processing the job.
        # If one can't be opened, report the error and skip this job.
        job_log_opened = False
        try:
            try_create_dir_for_file(job_log_path)
            job_log_file = open(job_log_path, "w")
            job_log_opened = True
        except EnvironmentError:
            pass

        if job_log_opened:
            # Switch stdout and stderr to the job log file.
            sys.stdout = job_log_file
            sys.stderr = job_log_file

            # Replace the logger's handlers with one that outputs to the new
            # stderr location.
            job_stream_handler = logging.StreamHandler()
            job_stream_handler.setFormatter(root_logger_first_handler.formatter)
            job_stream_handler.setLevel(root_logger_first_handler.level)
            for logging_handler in root_logger_original_handlers:
                root_logger.removeHandler(logging_handler)
            root_logger.addHandler(job_stream_handler)

            # Process the job.
            summary_error = pcp_summary.main(job_args)

            # Check if the job was successful and create a result to return.
            if not summary_error:
                job_results = (WorkerResult.SUCCESS, "")
            else:
                job_results = (WorkerResult.SILENT_FAILURE, "pcp_summary reported failure.")

            # Return the original handlers to the logger.
            root_logger.removeHandler(job_stream_handler)
            for logging_handler in root_logger_original_handlers:
                root_logger.addHandler(logging_handler)

            # Return stdout and stderr to the process log file.
            sys.stdout = process_log_file
            sys.stderr = process_log_file

            # Close the job log file.
            try:
                job_log_file.close()
            except EnvironmentError:
                pass
        else:
            job_results = (WorkerResult.LOUD_FAILURE, "Could not create log at '{0}'.".format(job_log_path))

        # Send the results to the main process.
        conn_to_parent.send(job_results)

def summarizer_thread_function():
    """
    The function run by summarizer threads to continually summarize job logs.
    """

    # Initialize variables.
    summary_cmd = pcp_summary_cmd_template[:]

    # Start a worker process.
    worker_proc, conn_to_worker = start_summarizer_process()

    # For the life of the thread...
    while True:

        # Get a job from the queue.
        job_queue_entry = jobs_to_summarize.get()
        job = job_queue_entry[1]

        # If the job is a poison pill, stop the worker process and this thread.
        if job is None:
            conn_to_worker.send(None)
            worker_proc.join()
            jobs_to_summarize.task_done()
            break

        # Generate the path to the job's log file.
        log_path = os.path.join(pcp_summary_log_path, get_job_log_path_end(job))

        # Generate main function arguments for the job.
        summary_cmd[-2] = job

        # Send the job's details to the worker process.
        conn_to_worker.send((summary_cmd, log_path))

        # Wait for results from the worker process. If the process dies,
        # start up a new process and assume failure.
        worker_died = False
        while not conn_to_worker.poll(process_timeout_time):
            if not worker_proc.is_alive():
                worker_died = True
                worker_proc, conn_to_worker = start_summarizer_process()
                worker_results = (WorkerResult.SILENT_FAILURE, "Worker process died.")
                break

        if not worker_died:
            worker_results = conn_to_worker.recv()

        # If failure occurred, report it. If it was a loud failure, send the
        # error message to the logger.
        worker_result_type = worker_results[0]
        if worker_result_type != WorkerResult.SUCCESS:
            if worker_result_type == WorkerResult.LOUD_FAILURE:
                logging.error(worker_results[1])

            record_job_error(job, "Error occurred while summarizing job-specific archives.")
            with failed_summary_jobs_lock:
                failed_summary_jobs.append(job)

        # Report that the job has been handled.
        jobs_to_summarize.task_done()

def get_job_summary_dir(job):
    """
    Attempts to find the summary directory for a given job.

    Args:
        job: The job whose summary directory is being searched for.
    Returns:
        A path to the summary directory for the given job, or None if not found.
    """

    if job in job_summary_dirs:
        return job_summary_dirs[job]

    (job_cluster, job_id, job_portion) = get_job_components(job)

    if job_id is None:
        return None

    if job_portion:
        job_summary_search_path = os.path.join(summaries_dir, "*", job_cluster, job_id, job_portion, "")
    else:
        job_summary_search_path = os.path.join(summaries_dir, "*", job_cluster, job_id, "")

    found_job_summary_dirs = glob.glob(job_summary_search_path)
    if found_job_summary_dirs:
        found_job_summary_dir = found_job_summary_dirs[0]
        job_summary_dirs[job] = found_job_summary_dir
        return found_job_summary_dir

    return None

def copy_job_logs_to_summary_dirs(jobs, job_log_path_base, dest_file_name, alt_dest_file_template):
    """
    Copies a set of job logs to their corresponding job summary directories.

    Args:
        jobs: A set of jobs whose logs are being copied.
        job_log_path_base: The base path for the paths to the jobs' log files.
        dest_file_name: The name of the log in its destination directory.
        alt_dest_file_template: An alternate name template to use for the 
            destination name if a file exists at the primary destination.
            Should include "{0}", which is where the time will be inserted.
    """

    for job in jobs:
        dest_dir = get_job_summary_dir(job)
        if dest_dir:
            dest_path = os.path.join(dest_dir, dest_file_name)
            if os.path.exists(dest_path):
                dest_path = os.path.join(dest_dir, alt_dest_file_template.format(log_dir_time_str))
            source_path = os.path.join(job_log_path_base, get_job_log_path_end(job))
            try:
                shutil.copyfile(source_path, dest_path)
            except EnvironmentError:
                logging.error("Could not copy log '{0}' to '{1}'.".format(source_path, dest_path))
        else:
            logging.error("Could not find summary folder for job {0}.".format(job))

def get_jobs_from_archive_proc(proc_cmd, proc_log_path):
    """
    Runs the given command for the job archival script and returns the jobs
    printed by it as a list of tuples (along with the process' return code).
    The tuples contain the job ID, the job's length, and the number of nodes
    the job ran on.

    Args:
        proc_cmd: The command to run. Should be a list that calls the job
            archive script with the print flag enabled.
        proc_log_path: The location to log the script's error output to.
    Returns:
        A tuple containing the list of job tuples first, followed by the
        return code output by the process.
    Throws:
        EnvironmentError: If the log file cannot be opened at the given path,
            or an error occurs while opening the subprocess.
    """
    proc_log_file = open(proc_log_path, "w")
    proc = subprocess.Popen(proc_cmd, stdout=subprocess.PIPE, stderr=proc_log_file)

    job_list = []
    for line in proc.stdout:
        stripped_line = line.rstrip()
        if not stripped_line:
            continue

        job = stripped_line.split("|")
        if len(job) == 3:
            job_list.append(tuple(job))
        else:
            logging.warning("Invalid job definition: {0}".format(stripped_line))

    proc.wait()
    proc_error = proc.returncode

    try:
        proc_log_file.close()
    except EnvironmentError:
        pass

    return (job_list, proc_error)

def write_list_to_lines_of_file(output_list, output_list_path):
    """
    Writes a list to lines of a file at the given path. If the list is empty,
    an empty file is produced. If a file exists at the given path, it will be
    overwritten.

    Args:
        output_list: The list to write to the file.
        output_list_path: The location of the file.
    Throws:
        EnvironmentError: If an error occurred while writing the file.
    """
    with open(output_list_path, "w") as output_list_file:
        if output_list:
            print >>output_list_file, "\n".join(output_list)

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        # Parse and validate command line options
        try:
            opts, args = getopt.getopt(argv[1:], "a:d:m:s:", ["archiveprocs=", "date=", "mongoprocs=", "summaryprocs="])
        except getopt.error, msg:
            raise Usage(msg)

        num_archive_processes = 1
        num_summary_processes = 1
        num_mongo_load_processes = 1
        input_first_job_end_date = None
        input_last_job_end_date = None
        for opt in opts:
            option_name = opt[0]
            option_value = opt[1]

            if option_name in ("-a", "--archiveprocs"):
                try:
                    num_archive_processes = int(option_value)
                except ValueError:
                    raise Usage("Number of archive processes must be an integer.")

            elif option_name in ("-d", "--date"):
                input_job_end_date_match = input_job_end_date_regex.match(option_value)
                if not input_job_end_date_match:
                    raise Usage("Could not parse input job end date.")

                try:
                    input_first_job_end_date = datetime.strptime(input_job_end_date_match.group("first_date"), input_job_end_date_format).date()
                except ValueError:
                    raise Usage("First date given is not valid.")

                input_last_job_end_date_str = input_job_end_date_match.group("second_date")
                if input_last_job_end_date_str:
                    try:
                        input_last_job_end_date = datetime.strptime(input_last_job_end_date_str, input_job_end_date_format).date()
                    except ValueError:
                        raise Usage("Second date given is not valid.")

                    if input_last_job_end_date <= input_first_job_end_date:
                        raise Usage("Second date given must come after first date.")

            elif option_name in ("-m", "--mongoprocs"):
                try:
                    num_mongo_load_processes = int(option_value)
                except ValueError:
                    raise Usage("Number of Mongo load processes must be an integer.")

            elif option_name in ("-s", "--summaryprocs"):
                try:
                    num_summary_processes = int(option_value)
                except ValueError:
                    raise Usage("Number of summary processes must be an integer.")

        # Find and store the locations of the scripts used by this script.
        this_script_dir = None
        try:
            this_script_dir = os.path.dirname(os.path.realpath(__file__))
        except NameError:
            pass

        global pcp_job_archive_script_dir
        global pcp_summary_script_dir
        global mongo_load_script_dir

        if pcp_job_archive_script_dir is None:
            if this_script_dir is None:
                raise Usage("Could not find job archival script.")
            pcp_job_archive_script_dir = this_script_dir

        if pcp_summary_script_dir is None:
            if this_script_dir is None:
                if num_summary_processes > 0:
                    raise Usage("Could not find job summarization script.")
                else:
                    pcp_summary_script_dir = ""
            else:
                pcp_summary_script_dir = this_script_dir

        if mongo_load_script_dir is None:
            if this_script_dir is None:
                if num_mongo_load_processes > 0:
                    raise Usage("Could not find Mongo load script.")
                else:
                    mongo_load_script_dir = ""
            else:
                mongo_load_script_dir = this_script_dir

        pcp_job_archive_script_path = os.path.join(pcp_job_archive_script_dir, pcp_job_archive_script_name)
        pcp_summary_script_path = os.path.join(pcp_summary_script_dir, pcp_summary_script_name)
        mongo_load_script_path = os.path.join(mongo_load_script_dir, mongo_load_script_name)

        if not os.path.isfile(pcp_job_archive_script_path):
            raise Usage("Could not find job archival script.")
        if (num_summary_processes > 0) and (not os.path.isfile(pcp_summary_script_path)):
            raise Usage("Could not find job summarization script.")
        if (num_mongo_load_processes > 0) and (not os.path.isfile(mongo_load_script_path)):
            raise Usage("Could not find Mongo load script.")

        pcp_job_archive_print_complete_cmd[0] = pcp_job_archive_script_path
        pcp_job_archive_run_cmd_template[0] = pcp_job_archive_script_path
        pcp_summary_cmd_template[0] = pcp_summary_script_path
        mongo_load_cmd_template[0] = mongo_load_script_path

        # Create directories for the logs, if they don't exist.
        current_time = datetime.now()
        global log_dir_time_str
        log_dir_time_str = current_time.strftime(pcp_script_log_time_format)
        test_path_end = get_log_path_end("test", extension=None)
        try_create_dir_for_file(os.path.join(pcp_job_archive_log_path, test_path_end))
        try_create_dir_for_file(os.path.join(pcp_summary_log_path, test_path_end))
        try_create_dir_for_file(os.path.join(unfiled_error_file_path, test_path_end))

        # If a range of job end dates was given, use midnight of the first day
        # to midnight of the day after the last day as the start and end times.
        if input_last_job_end_date is not None:
            start_time = input_first_job_end_date
            end_time = input_last_job_end_date + timedelta(days=1)

        # If only one job end date was given, use midnight of that day to
        # midnight of the next day as the start and end times. 
        elif input_first_job_end_date is not None:
            start_time = input_first_job_end_date
            end_time = start_time + timedelta(days=1)

        # Otherwise, use midnight yesterday to midnight today as the start and
        # end times.
        else:
            end_time = current_time.date()
            start_time = end_time - timedelta(days=1)

        # Obtain a list of all jobs and job portions finishing between the
        # start and end times, and add each job to the queue of jobs to archive.
        # Priority is calculated using the length of the job or job portion
        # times the number of nodes the job ran on.
        start_time_str = start_time.strftime(pcp_job_archive_time_format)
        end_time_str = end_time.strftime(pcp_job_archive_time_format)

        pcp_job_archive_print_complete_cmd[-3] = start_time_str
        pcp_job_archive_print_complete_cmd[-1] = end_time_str

        pcp_job_archive_print_incomplete_cmd = pcp_job_archive_print_complete_cmd[:]
        pcp_job_archive_print_incomplete_cmd.append("-i")

        pcp_job_archive_complete_log_path = os.path.join(pcp_job_archive_log_path, get_log_path_end("job_list_complete_proc"))
        pcp_job_archive_incomplete_log_path = os.path.join(pcp_job_archive_log_path, get_log_path_end("job_list_incomplete_proc"))
        
        (complete_jobs, complete_proc_error) = get_jobs_from_archive_proc(pcp_job_archive_print_complete_cmd, pcp_job_archive_complete_log_path)
        if complete_proc_error:
            logging.error("Error occurred while retrieving list of completed jobs. Retrieval process error code: %s" % complete_proc_error)
            return 1
        
        (incomplete_jobs, incomplete_proc_error) = get_jobs_from_archive_proc(pcp_job_archive_print_incomplete_cmd, pcp_job_archive_incomplete_log_path)
        if incomplete_proc_error:
            logging.error("Error occurred while retrieving completed portions of incomplete jobs. Retrieval process error code: %s" % incomplete_proc_error)
            return 1

        job_list = []
        job_list.extend(incomplete_jobs)
        job_list.extend(complete_jobs)

        opposite_priority_queue_job_list = []
        max_node_time = timedelta(days=0, seconds=0)
        for job in job_list:
            job_length = get_timedelta_from_string(job[1])
            job_number_of_nodes = int(job[2])

            job_node_time = job_length * job_number_of_nodes
            if job_node_time > max_node_time:
                max_node_time = job_node_time

            opposite_priority_queue_job_list.append((job_node_time, job[0]))

        priority_queue_job_list = [(max_node_time - job[0], job[1]) for job in opposite_priority_queue_job_list]

        for job in priority_queue_job_list:
            jobs_to_archive.put(job)

        # Record the lists of jobs that will be handled in files.
        write_list_to_lines_of_file([job[0] for job in job_list], os.path.join(pcp_job_archive_log_path, get_log_path_end("job_list")))
        write_list_to_lines_of_file([job[0] for job in complete_jobs], os.path.join(pcp_job_archive_log_path, get_log_path_end("job_list_complete")))
        write_list_to_lines_of_file([job[0] for job in incomplete_jobs], os.path.join(pcp_job_archive_log_path, get_log_path_end("job_list_incomplete")))

        # If enabled, create and start threads to handle archival of jobs.
        # Also, add a poison pill to the end of the queue for each thread.
        poison_pill = (timedelta.max, None)
        for i in xrange(num_archive_processes):
            archiver_thread = threading.Thread(target=archiver_thread_function)
            archiver_thread.daemon = True
            archiver_thread.start()

            jobs_to_archive.put(poison_pill)

        # If step is enabled, wait for archival of all jobs to finish.
        # Otherwise, dump all jobs into the summarization queue.
        if num_archive_processes > 0:
            jobs_to_archive.join()
        else:
            while not jobs_to_archive.empty():
                jobs_to_summarize.put(jobs_to_archive.get())
                jobs_to_archive.task_done()

        # Record all jobs that failed to archive in a log file.
        write_list_to_lines_of_file(failed_archival_jobs, os.path.join(pcp_job_archive_log_path, get_log_path_end("failed_jobs")))

        # Delete the temporary archives created by the job archival script.
        shutil.rmtree(pcp_job_archive_tmp_dir, ignore_errors=True)

        # Create and start threads to handle summarization of jobs.
        # Also, add a poison pill to the end of the queue for each thread.
        for i in xrange(num_summary_processes):
            summarizer_thread = threading.Thread(target=summarizer_thread_function)
            summarizer_thread.daemon = True
            summarizer_thread.start()

            jobs_to_summarize.put(poison_pill)

        # If step is enabled, wait for summarization of all jobs to finish.
        if num_summary_processes > 0:
            jobs_to_summarize.join()

        # Record all jobs that failed to summarize in a log file.
        write_list_to_lines_of_file(failed_summary_jobs, os.path.join(pcp_summary_log_path, get_log_path_end("failed_jobs")))

        # Move all per-job error files to the corresponding summary directory.
        for job_tuple in job_list:
            job = job_tuple[0]
            error_file_path = os.path.join(unfiled_error_file_path, get_job_log_path_end(job))
            if not os.path.isfile(error_file_path):
                continue

            error_file_dest_folder = get_job_summary_dir(job)
            if error_file_dest_folder:
                error_file_dest = os.path.join(error_file_dest_folder, "ERROR.txt")
                if os.path.exists(error_file_dest):
                    error_file_dest = os.path.join(error_file_dest_folder, "ERROR-{0}.txt".format(log_dir_time_str))
                try:
                    os.rename(error_file_path, error_file_dest)
                except EnvironmentError:
                    logging.error("Could not move error file '%s' to job's summary folder." % error_file_path)
            else:
                logging.error("Could not find summary folder for job %s." % job)

        # Attempt to delete the temporary folder for error files.
        # If files remain in the folder, signal an error.
        unfiled_error_date_folder = os.path.join(unfiled_error_file_path, log_dir_time_str)
        remaining_error_file_paths = glob.glob(os.path.join(unfiled_error_date_folder, "*", "*"))
        if not remaining_error_file_paths:
            remaining_error_dir_paths = glob.glob(os.path.join(unfiled_error_date_folder, "*"))
            for remaining_error_dir in remaining_error_dir_paths:
                try:
                    os.rmdir(remaining_error_dir)
                except EnvironmentError:
                    pass
            try:
                os.rmdir(unfiled_error_date_folder)
            except EnvironmentError:
                logging.error("Failed to delete unfiled error folder '%s'." % unfiled_error_date_folder)
        else:
            logging.error("Some error files were not filed with summaries. See '%s'." % unfiled_error_date_folder)

        # For each job that failed the archival step,
        # copy the archival log into the summary directory.
        copy_job_logs_to_summary_dirs(failed_archival_jobs, pcp_job_archive_log_path, "pcp_job_archive_log.txt", "pcp_job_archive_log_{0}.txt")
        
        # For each job that failed the summarization step,
        # copy the summarization log into the summary directory.
        copy_job_logs_to_summary_dirs(failed_summary_jobs, pcp_summary_log_path, "pcp_summary_log.txt", "pcp_summary_log_{0}.txt")

        # For every divided job that completed, move all of its summaries to
        # the subfolder for the date it completed and make a copy of the last
        # job_info.json file for the job's main folder.
        for job_tuple in complete_jobs:
            job = job_tuple[0]
            (job_cluster, job_id, job_portion) = get_job_components(job)
            if not job_portion:
                continue

            job_portion_summary_dir = get_job_summary_dir(job)
            if not job_portion_summary_dir:
                continue

            from_job_summary_dir = os.path.dirname(os.path.normpath(job_portion_summary_dir))

            from_last_job_info_path = os.path.join(job_portion_summary_dir, "job_info.json")
            to_last_job_info_path = os.path.join(from_job_summary_dir, "job_info.json")
            try:
                shutil.copyfile(from_last_job_info_path, to_last_job_info_path)
            except EnvironmentError:
                continue

            try:
                with open(to_last_job_info_path, "r") as job_info_json_file:
                    job_info_json = json.load(job_info_json_file)
            except (EnvironmentError, ValueError):
                continue

            if "end" not in job_info_json:
                continue

            job_end_time_utc_str = job_info_json["end"]
            if not job_end_time_utc_str:
                continue

            try:
                job_end_time_utc = datetime.strptime(job_end_time_utc_str, job_info_datetime_format)
            except ValueError:
                continue

            job_end_time_system = pytz.utc.localize(job_end_time_utc).astimezone(pcp_system_time_zone)
            to_job_summary_dir = os.path.join(summaries_dir, job_end_time_system.strftime(subdir_datetime_format), job_cluster, job_id)

            if from_job_summary_dir == to_job_summary_dir:
                continue
            if os.path.exists(to_job_summary_dir):
                continue

            try_create_dir_for_file(to_job_summary_dir)
            shutil.move(from_job_summary_dir, to_job_summary_dir)

        # Load the summaries into Mongo, if enabled.
        if num_mongo_load_processes > 0:
            num_summary_days = (end_time - start_time).days
            summary_dirs = []
            for i in xrange(num_summary_days):
                summary_day = start_time + timedelta(days=i)
                summary_day_search_path = os.path.join(summaries_dir, summary_day.strftime(subdir_datetime_format), "*", "*", "job_info.json")
                summary_day_job_info_files = glob.iglob(summary_day_search_path)
                summary_day_dirs = (os.path.dirname(summary_file_path) for summary_file_path in summary_day_job_info_files)
                summary_dirs.extend(summary_day_dirs)

            mongo_load_log_path = os.path.join(pcp_summary_log_path, get_log_path_end("mongo_load"))
            with open(mongo_load_log_path, "w") as mongo_load_log_file:
                mongo_load_cmd = mongo_load_cmd_template[:]
                mongo_load_cmd[-1] = str(num_mongo_load_processes)
                mongo_load_proc = subprocess.Popen(mongo_load_cmd, stdin=subprocess.PIPE, stdout=mongo_load_log_file, stderr=subprocess.STDOUT)

                for summary_dir in summary_dirs:
                    print >>mongo_load_proc.stdin, summary_dir
                mongo_load_proc.stdin.close()

                mongo_load_proc_error = mongo_load_proc.wait()
                if mongo_load_proc_error:
                    logging.error("Mongo load process ended with error code: {0}".format(mongo_load_proc_error))

        # Return success.
        return 0

    except Usage, err:
        print >>sys.stderr, err.msg

        print >>sys.stderr, "Usage: pcp_daily_summary [options]"
        print >>sys.stderr, ""
        print >>sys.stderr, "        Options:"
        print >>sys.stderr, "        -a [NUM_PROCS], --archiveprocs=[NUM_PROCS]  The number of archive"
        print >>sys.stderr, "                                                    processes to run. Use <1"
        print >>sys.stderr, "                                                    to disable this step."
        print >>sys.stderr, "                                                    Defaults to 1."
        print >>sys.stderr, "        -d [END_DATE], --date=[END_DATE]            The end date of jobs to be"
        print >>sys.stderr, "                                                    summarized. May be a range"
        print >>sys.stderr, "                                                    of dates. Defaults to"
        print >>sys.stderr, "                                                    yesterday. Format:"
        print >>sys.stderr, "                                                    YYYY-MM-DD[-YYYY-MM-DD]"
        print >>sys.stderr, "        -m [NUM_PROCS], --mongoprocs=[NUM_PROCS]    The number of Mongo load"
        print >>sys.stderr, "                                                    processes to run. Use <1"
        print >>sys.stderr, "                                                    to disable this step."
        print >>sys.stderr, "                                                    Defaults to 1."
        print >>sys.stderr, "        -s [NUM_PROCS], --summaryprocs=[NUM_PROCS]  The number of summary"
        print >>sys.stderr, "                                                    processes to run. Use <1"
        print >>sys.stderr, "                                                    to disable this step."
        print >>sys.stderr, "                                                    Defaults to 1."

        return 2

if __name__ == "__main__":
    # Setup the logging module.
    setup_logging()

    # Run the main function.
    sys.exit(main())
