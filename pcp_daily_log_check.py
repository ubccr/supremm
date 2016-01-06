#!/usr/bin/python

"""
pcp_daily_log_check.py
PCP Scripts

Checks that all per-node logs exist for a day, and if not, outputs the nodes
with missing logs. Also reports log directories for unknown nodes.

Usage: pcp_daily_log_check [options]

        Options:
        -d [DATE], --date=[DATE]        The date of logs to check for.
                                        Defaults to yesterday.
        -n, --nagios                    Output errors in a format friendly
                                        to Nagios.

Author: Tom Yearke <tyearke@buffalo.edu>
"""

import datetime
import getopt
import glob
import os
import subprocess
import sys
import threading

# The directory per-node logs are stored in.
node_logs_dir = "/data/pcp-logs"

# The location of a list of non-job nodes to check for logs.
non_job_node_list_path = "/data/config/non_job_nodes.txt"

# The domains the nodes being checked may belong to.
#
# Sub-domains of other domains in this list should be listed before those
# domains, as the domain check is a simple "ends with" check that stops at the
# first match.
node_domains = (
    'cbls.ccr.buffalo.edu',
    'ccr.buffalo.edu',
)

# The format for the command line date option.
input_date_format = "%Y-%m-%d"

# The format used for printing dates.
print_date_format = "%Y-%m-%d"

# The format for the date in the log files' names.
log_date_format = "%Y%m%d"

# The suffix to check for when searching for log files.
log_file_suffix = ".meta"

# Tracks if the error output header has been printed yet.
error_header_printed = False

# The date whose per-node logs are being checked for. Defaults to yesterday.
log_date = datetime.date.today() - datetime.timedelta(days=1)

# Indicates whether or not the output is being formatted for Nagios.
using_nagios_output = False

# A command for retrieving lists of nodes from sinfo.
sinfo_cmd = [
    "sinfo",
    "-a",
    "-M", "all",
    "-h",
    "-o", "%N"
]

# A template command for expanding a ranged list of nodes into
# a comma-separated list.
nodeset_cmd_template = [
    "nodeset",
    "-e",
    "-S", ",",
    None
]

# A set of error levels which corresponds to the error codes Nagios uses.
class ErrorLevel(object):
    NONE = 0
    WARNING = 1
    CRITICAL = 2

# Tracks the maximum level of a reported error.
maximum_error_reported = ErrorLevel.NONE

def log_pipe(pipe, logging_function, template="{0}", print_empty_footer=False):
    """
    Logs each non-empty line from a pipe (or other file-like object)
    using the given logging function. This will block until the end of
    the pipe is reached.

    Args:
        pipe: The pipe to read from.
        logging_function: The logging function to use.
        template: (Optional) A template string to place each line from pipe
                inside.
        print_empty_footer: (Optional) Controls whether an empty line is
                printed at the end if anything was printed. Defaults to False.
    """
    if not pipe:
        return

    if not logging_function:
        for line in pipe:
            continue
        return

    output_printed = False
    for line in pipe:
        stripped_line = line.rstrip()
        if stripped_line:
            logging_function(template.format(stripped_line))
            output_printed = True

    if output_printed and print_empty_footer:
        logging_function("")

def report_error(error_level, error_message):
    """
    Tracks the maximum error level reported and prints the given message to
    stdout. If the header has not been printed yet (and is enabled), that is
    printed first.

    Args:
        error_level: The level of the error being reported.
        error_message: The error message to print.
    """
    global maximum_error_reported
    if error_level > maximum_error_reported:
        maximum_error_reported = error_level

    global error_header_printed
    if not error_header_printed:
        if not using_nagios_output:
            print "Error occurred for per-node logs on {0}:\n".format(log_date.strftime(print_date_format))
        error_header_printed = True
    else:
        if using_nagios_output:
            sys.stdout.write("; ")

    if using_nagios_output:
        sys.stdout.write(error_message)
    else:
        print error_message

def report_error_warning(error_message):
    """
    Calls report_error with the warning error level and the given message.
    If Nagios output is enabled, the message is prepended with a label.

    Args:
        error_message: The error message to print.
    """
    if using_nagios_output:
        error_message = "PCP LOG WARNING: {0}".format(error_message)
    report_error(ErrorLevel.WARNING, error_message)

def report_error_critical(error_message):
    """
    Calls report_error with the critical error level and the given message.
    If Nagios output is enabled, the message is prepended with a label.

    Args:
        error_message: The error message to print.
    """
    if using_nagios_output:
        error_message = "PCP LOG CRITICAL: {0}".format(error_message)
    report_error(ErrorLevel.CRITICAL, error_message)

def get_subproc_error_logging_function():
    """
    Gets the error logging function to use for a subprocess' error output.

    Returns:
        If outputting for Nagios, None. Otherwise, report_error_critical.
    """
    if using_nagios_output:
        return None
    else:
        return report_error_critical

def divide_nodes_by_name_type(node_set):
    """
    Divide a set of nodes by the way the host name is specified.

    This function checks nodes against the domains specified in node_domains.

    All returned sets are in short name format. There will be duplicates
    between the sets if multiple formats were specified in the initial set.

    Args:
        node_set: The set of nodes to divide.
    Returns:
        A dictionary whose keys are the domains checked (the empty string is
        for no match) and the values are sets of nodes specified by the
        corresponding domain.
    """
    node_sets = {
        "": set(),
    }
    full_name_suffixes = {}
    full_name_suffix_lengths = {}
    for node_domain in node_domains:
        node_sets[node_domain] = set()
        full_name_suffixes[node_domain] = ".{0}".format(node_domain)
        full_name_suffix_lengths[node_domain] = len(full_name_suffixes[node_domain])

    for node in node_set:
        full_name_match_found = False
        for node_domain in node_domains:
            if node.endswith(full_name_suffixes[node_domain]):
                node_sets[node_domain].add(node[:-full_name_suffix_lengths[node_domain]])
                full_name_match_found = True
                break

        if full_name_match_found:
            continue

        node_sets[""].add(node)

    return node_sets

def expand_node_list_string(node_list_string):
    """
    Expands a ranged list string of nodes into an array using nodeset.

    Args:
        node_list_string: The ranged list string to convert.
    Returns:
        A list of individual nodes from the given string.
    """
    if not node_list_string:
        return []

    nodeset_cmd = nodeset_cmd_template[:]
    nodeset_cmd[-1] = node_list_string
    nodeset_proc = subprocess.Popen(nodeset_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    nodeset_error_logger = threading.Thread(target=log_pipe, args=(nodeset_proc.stderr, get_subproc_error_logging_function(), "nodeset error: {0}", True))
    nodeset_error_logger.start()

    nodes = []
    for line in nodeset_proc.stdout:
        if nodes:
            continue
        nodes = line.rstrip().split(",")

    nodeset_error_logger.join()

    nodeset_proc.wait()
    nodeset_error = nodeset_proc.returncode
    if nodeset_error:
        if using_nagios_output:
            report_error_critical("nodeset not working properly")
        else:
            report_error_critical("nodeset returned with code {0}.".format(nodeset_error))

    return nodes

def get_return_code():
    """
    Gets the script's return code based on the maximum reported error level
    and the output format.

    Returns:
        If Nagios output is enabled, the corresponding error code.
        Otherwise, 1 if an error occurred and 0 if not.
    """
    if using_nagios_output:
        return maximum_error_reported

    if maximum_error_reported != ErrorLevel.NONE:
        return 1

    return 0

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        # Parse and validate command line options
        try:
            opts, args = getopt.getopt(argv[1:], "d:n", ["date=", "nagios"])
        except getopt.error, msg:
            raise Usage(msg)

        global log_date
        global using_nagios_output
        for opt in opts:
            option_name = opt[0]
            option_value = opt[1]

            if option_name in ("-d", "--date"):
                try:
                    log_date = datetime.datetime.strptime(option_value, input_date_format).date()
                except ValueError:
                    raise Usage("Could not parse input date.")

            elif option_name in ("-n", "--nagios"):
                using_nagios_output = True

        # Generate the string used to search for logs for the date being checked.
        log_date_str = log_date.strftime(log_date_format)
        log_search_str = "{0}*{1}".format(log_date_str, log_file_suffix)

        # If the per-node log directory is missing, report the error and stop.
        if not os.path.isdir(node_logs_dir):
            if using_nagios_output:
                report_error_critical("per-node log directory missing")
            else:
                report_error_critical("The per-node log directory is missing.")
            return get_return_code()

        # List the contents of the per-node log directory. If empty,
        # report the error and stop.
        node_logs_dir_contents = os.listdir(node_logs_dir)
        if not node_logs_dir_contents:
            if using_nagios_output:
                report_error_critical("per-node log directory empty")
            else:
                report_error_critical("The per-node log directory is empty.")
            return get_return_code()

        # For each directory in the per-node log directory, check if there
        # is at least one log file for the date being checked.
        checked_nodes = set()
        missing_log_nodes = set()
        for node in node_logs_dir_contents:
            node_dir = os.path.join(node_logs_dir, node)
            if not os.path.isdir(node_dir):
                continue

            checked_nodes.add(node)
            node_log_search_path = os.path.join(node_dir, log_search_str)
            node_logs_for_date = glob.glob(node_log_search_path)
            if not node_logs_for_date:
                missing_log_nodes.add(node)

        # If no nodes were checked, report the error and stop.
        if not checked_nodes:
            if using_nagios_output:
                report_error_critical("no node log directories found")
            else:
                report_error_critical("No node log directories were found.")
            return get_return_code()

        # If node domains have been specified, treat full names and short names
        # as representing the same node.
        if node_domains:
            # Remove duplicates from the set of checked nodes.
            checked_node_sets = divide_nodes_by_name_type(checked_nodes)
            checked_nodes = set()
            for checked_node_set in checked_node_sets.itervalues():
                checked_nodes |= checked_node_set

            # Remove the duplicates from the set of missing log nodes.
            missing_log_node_sets = divide_nodes_by_name_type(missing_log_nodes)
            missing_log_nodes = set()
            for missing_log_node_set in missing_log_node_sets.itervalues():
                missing_log_nodes |= missing_log_node_set

            # Remove entries from the set of missing log nodes if logs were
            # available under at least one name for the node.
            for (node_domain, checked_node_set) in checked_node_sets.iteritems():
                missing_log_node_set = missing_log_node_sets[node_domain]
                missing_log_nodes -= checked_node_set - missing_log_node_set

        # Get the list of nodes in all job clusters from sinfo.
        known_nodes = set()
        sinfo_proc = subprocess.Popen(sinfo_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

        sinfo_error_logger = threading.Thread(target=log_pipe, args=(sinfo_proc.stderr, get_subproc_error_logging_function(), "sinfo error: {0}", True))
        sinfo_error_logger.start()

        sinfo_node_lists = []
        for line in sinfo_proc.stdout:
            stripped_line = line.rstrip()
            if not stripped_line:
                continue

            if "CLUSTER:" in stripped_line:
                continue

            sinfo_node_lists.append(stripped_line)

        sinfo_node_list = ",".join(sinfo_node_lists)

        sinfo_error_logger.join()

        sinfo_proc.wait()
        sinfo_error = sinfo_proc.returncode
        if sinfo_error:
            if using_nagios_output:
                report_error_critical("sinfo not working properly")
            else:
                report_error_critical("sinfo returned with code {0}.".format(sinfo_error))

        job_nodes = expand_node_list_string(sinfo_node_list)
        known_nodes.update(job_nodes)

        # Get a list of non-job nodes from the config file, if any.
        non_job_node_list = None
        if non_job_node_list_path:
            try:
                with open(non_job_node_list_path, "r") as non_job_node_list_file:
                    for line in non_job_node_list_file:
                        stripped_line = line.rstrip()
                        if stripped_line:
                            non_job_node_list = stripped_line
                            break
            except EnvironmentError:
                if using_nagios_output:
                    report_error_warning("couldn't open non-job node list")
                else:
                    report_error_warning("Could not open non-job node list file at '{0}'.".format(non_job_node_list_path))

        non_job_nodes = expand_node_list_string(non_job_node_list)
        known_nodes.update(non_job_nodes)

        # Find the set of known nodes that are missing log directories and
        # the set of log directories for unknown nodes.
        missing_log_dir_known_nodes = known_nodes.difference(checked_nodes)
        unknown_log_dirs = checked_nodes.difference(known_nodes)

        # Find the set of known nodes whose logs are missing.
        missing_log_known_nodes = missing_log_nodes.intersection(known_nodes)

        # If known nodes were missing logs for the checked date, list them.
        if missing_log_known_nodes:
            checked_known_nodes = checked_nodes.intersection(known_nodes)

            num_checked_known_nodes = len(checked_known_nodes)
            num_missing_log_known_nodes = len(missing_log_known_nodes)

            if num_checked_known_nodes == num_missing_log_known_nodes:
                if using_nagios_output:
                    report_error_critical("all {0} per-node logs missing".format(log_date.strftime(print_date_format)))
                else:
                    report_error_critical("All per-node logs are missing for this date.")
            else:
                sorted_missing_log_known_nodes = sorted(missing_log_known_nodes)
                if using_nagios_output:
                    report_error_critical("nodes missing {0} logs: {1}".format(log_date.strftime(print_date_format), ", ".join(sorted_missing_log_known_nodes)))
                else:
                    report_error_critical("{0} of {1} known nodes with directories are missing logs for this date:".format(num_missing_log_known_nodes, num_checked_known_nodes))
                    for node in sorted_missing_log_known_nodes:
                        report_error_critical(node)

            if not using_nagios_output:
                report_error_critical("")

        # If any known nodes are missing log directories, list them.
        if missing_log_dir_known_nodes:
            num_known_nodes = len(known_nodes)
            num_missing_log_dir_known_nodes = len(missing_log_dir_known_nodes)

            sorted_missing_log_dir_known_nodes = sorted(missing_log_dir_known_nodes)
            if using_nagios_output:
                report_error_critical("nodes missing log dirs: {0}".format(", ".join(sorted_missing_log_dir_known_nodes)))
            else:
                report_error_critical("{0} of {1} known nodes are missing log directories:".format(num_missing_log_dir_known_nodes, num_known_nodes))
                for node in sorted_missing_log_dir_known_nodes:
                    report_error_critical(node)
                report_error_critical("")

        # If any node log directories are not in the set of known nodes, list them.
        if unknown_log_dirs:
            sorted_unknown_log_dirs = sorted(unknown_log_dirs)
            if using_nagios_output:
                report_error_warning("unknown nodes with log dirs: {0}".format(", ".join(sorted_unknown_log_dirs)))
            else:
                report_error_warning("{0} log directories are not in the known set of nodes:".format(len(unknown_log_dirs)))
                for node in sorted_unknown_log_dirs:
                    report_error_warning(node)
                report_error_warning("")

        # If outputting for Nagios, print an OK message if no errors occurred
        # and print a newline character.
        if using_nagios_output:
            if maximum_error_reported == ErrorLevel.NONE:
                report_error(ErrorLevel.NONE, "PCP LOG OK: all {0} per-node logs found".format(log_date.strftime(print_date_format)))
            sys.stdout.write("\n")

        # Return an error code based on how the script ran.
        return get_return_code()

    except Usage, err:
        print >>sys.stderr, err.msg

        print >>sys.stderr, "Usage: pcp_daily_log_check [options]"
        print >>sys.stderr, ""
        print >>sys.stderr, "        Options:"
        print >>sys.stderr, "        -d [DATE], --date=[DATE]        The date of logs to check for."
        print >>sys.stderr, "                                        Defaults to yesterday."
        print >>sys.stderr, "        -n, --nagios                    Output errors in a format friendly"
        print >>sys.stderr, "                                        to Nagios."

        return 2

if __name__ == "__main__":
    sys.exit(main())
