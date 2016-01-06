#!/usr/bin/python

"""
slurm_tool_dump.py
SLURM Scripts

Dumps the output of a SLURM tool to a file named for the current time
in a folder set inside the script.

Usage: slurm_tool_dump tools...

        Arguments:
        tools                           A space-separated list of tools that
                                        should be run and have their output
                                        stored. May include:
                                        sdiag, sinfo, sprio, squeue, sshare

Author: Tom Yearke <tyearke@buffalo.edu>
"""

from datetime import datetime
import getopt
import os
import subprocess
import sys

# The clusters to check when running a tool once for each cluster.
slurm_clusters = [
    "ub-hpc",
    "chemistry",
    "mae",
    "physics"
]

# Command line arguments for each of the supported SLURM tools.
# A tool to be used once for each cluster should include the cluster option
# followed by None.

sdiag_cmd = [
    "sdiag"
]

sinfo_cmd = [
    "sinfo",
    "-a",
    "-M", "all"
]

sprio_cmd = [
    "sprio",
    "-l",
    "-M", None
]

squeue_cmd = [
    "squeue",
    "-o", "%.7i %.9P %.8j %.8u %.8T %.10M %.9l %.6D %R %c %C %p %S %f %Y",
    "-S",
    "-p",
    "-a",
    "-M", "all"
]

sshare_cmd = [
    "sshare",
    "-a",
    "-l"
]

# A mapping of command-line arguments for tools to their parameters.
# Parameters are tuples containing the command line arguments for the tool,
# the base directory for that tool's output, and the index in the command
# line arguments where a single cluster should be specified (or None if none).
slurm_tool_mapping = {
    "sdiag": (sdiag_cmd, "/data/slurm-tool-logs/sdiag", None),
    "sinfo": (sinfo_cmd, "/data/slurm-tool-logs/sinfo", None),
    "sprio": (sprio_cmd, "/data/slurm-tool-logs/sprio", 3),
    "squeue": (squeue_cmd, "/data/slurm-tool-logs/squeue", None),
    "sshare": (sshare_cmd, "/data/slurm-tool-logs/sshare", None)
}

# The datetime format used to generate date subdirectories.
output_file_subdir_time_format = "%Y-%m-%d"

# The datetime format used to generate file names.
output_file_time_format = "%H-%M-%S"

class Usage(Exception):
    def __init__(self, msg):
        self.msg = msg

def main(argv=None):
    if argv is None:
        argv = sys.argv

    try:
        # Get command line options and arguments.
        try:
            opts, args = getopt.getopt(argv[1:], "", [])
        except getopt.error, msg:
            raise Usage(msg)

        # If any options were given, stop.
        if opts:
            raise Usage("No options are used by this script.")

        # If no arguments were given, stop.
        if not args:
            raise Usage("At least one SLURM tool must be specified.")

        # Remove duplicate tools by placing the arguments in a set.
        tool_set = set(args)

        # For each tool given, verify that it is a valid tool.
        for tool in tool_set:
            if not tool in slurm_tool_mapping:
                raise Usage("Tool '{0}' not defined.".format(tool))

        # For each tool given and each configuration for that tool, create a
        # folder for its output (if necessary) and run the tool with its output
        # redirected to a file in the folder.
        tool_returned_with_error = False
        for tool in tool_set:
            tool_base_cmd, tool_base_output_dir, tool_single_cluster_index = slurm_tool_mapping[tool]

            if tool_single_cluster_index is not None:
                tool_configs = []
                for cluster in slurm_clusters:
                    tool_cmd = tool_base_cmd[:]
                    tool_cmd[tool_single_cluster_index] = cluster
                    tool_output_dir = os.path.join(tool_base_output_dir, cluster)
                    tool_configs.append((tool_cmd, tool_output_dir))
            else:
                tool_configs = [(tool_base_cmd, tool_base_output_dir)]

            for tool_config in tool_configs:
                tool_cmd, tool_output_dir = tool_config

                current_time = datetime.now()
                output_file_dir = os.path.join(tool_output_dir, current_time.strftime(output_file_subdir_time_format))

                try:
                    os.makedirs(output_file_dir)
                except EnvironmentError:
                    if not os.path.isdir(output_file_dir):
                        raise

                output_file_name = "{0}.txt".format(current_time.strftime(output_file_time_format))
                output_file_path = os.path.join(output_file_dir, output_file_name)

                with open(output_file_path, "w") as output_file:
                    tool_returncode = subprocess.call(tool_cmd, stdout=output_file, stderr=subprocess.STDOUT)
                    if tool_returncode:
                        tool_returned_with_error = True

        return int(tool_returned_with_error)

    except Usage, err:
        print >>sys.stderr, err.msg

        print >>sys.stderr, "Usage: slurm_tool_dump tools..."
        print >>sys.stderr, ""
        print >>sys.stderr, "        Arguments:"
        print >>sys.stderr, "        tools                           A space-separated list of tools that"
        print >>sys.stderr, "                                        should be run and have their output"
        print >>sys.stderr, "                                        stored. May include:"
        print >>sys.stderr, "                                        sdiag, sinfo, sprio, squeue, sshare"

        return 2

if __name__ == "__main__":
    sys.exit(main())
