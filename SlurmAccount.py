import subprocess
import threading
import logging

from ProcessHelpers import get_utc_environ,log_pipe
from Job import Job

class SlurmAccount(object):

    def __init__(self):
        # A base template for querying SLURM for job information.
        slurm_cmd_template = [
            "sacct",
            "--format", "jobid,cluster,nodelist,nnodes,start,end,user,account",
            "-P",
            "-X",
            "-n"
        ]

        # A command for querying SLURM with a given job cluster and ID.
        self.job_id_slurm_cmd = slurm_cmd_template[:]
        self.job_id_slurm_cmd.extend([
            "-M", None,
            "-j", None
        ])

        # A command for querying SLURM with a given job description.
        self.job_desc_slurm_cmd = slurm_cmd_template[:]
        self.job_desc_slurm_cmd.extend([
            "-S", None,
            "-E", None
        ])


    # This function uses a bit of a stupid way of generating the command
    def get_job_from_jobid(self, cluster, job_id):

        cmd = self.job_id_slurm_cmd[:]
        cmd[-3] = cluster
        cmd[-1] = str(job_id)
        return self.execute_cmd(cmd)
    
    def get_jobs(self, start_time, end_time, allow_incomplete, users, accounts, nodes, partitions, cluster):

        cmd = self.job_desc_slurm_cmd[:]
        cmd[-3] = start_time
        cmd[-1] = end_time

        cmd.append("--state")
        if allow_incomplete:
            cmd.append("CONFIGURING,COMPLETING,PENDING,RUNNING,RESIZING,SUSPENDED")
        else:
            cmd.append("CANCELLED,COMPLETED,FAILED,NODE_FAIL,PREEMPTED,TIMEOUT")

        if users is not None:
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

        return self.execute_cmd(cmd)



    def process_nodelist(self, nodelist):
        open_brace_flag = False
        close_brace_flag = False
        host_list = []
        tmp_host = ""
        # get a list of all the nodes and store them in host_host
        for c in nodelist:
            if c == '[':
                open_brace_flag = True
            elif c == ']':
                close_brace_flag = True
            if ( c == ',' and not close_brace_flag and not open_brace_flag ) or (c == ',' and close_brace_flag):
                host_list.append(tmp_host)
                tmp_host = ""
                close_brace_flag = False
                open_brace_flag = False
            else:
                tmp_host += c
        if tmp_host:
            host_list.append(tmp_host)
    
        # parse through host_list and expand the hostnames
        host_list_expanded = []
        for h in host_list:
            if '[' in h:
                node_head = h.split('[')[0]
                node_tail = h.split('[')[1][:-1].split(',')
                for n in node_tail:
                    if '-' in n:
                        num = n.split('-')
                        for x in range(int(num[0]), int(num[1])+1):
                            host_list_expanded.append(node_head + str("%02d" % x))
                    else:
                        host_list_expanded.append(node_head + n)
            else:
                host_list_expanded.append(h)
    
        return host_list_expanded

    def execute_cmd(self, cmd):
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
    
        # TODO - convert the nodelist before constructing the job - after all the nodelist_str is
        # a slurm specific artefact and does not need to be seen outside of the slurm-specific
        # code
        for job in jobs:
            job.set_nodes(self.process_nodelist(job.node_list_str) )

        # Return the list of jobs.
        return jobs
    
