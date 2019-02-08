""" Container class for an HPC job """
import datetime
from collections import OrderedDict

def safe_strptime(time_string, fmt):
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
        converted_time = datetime.datetime.strptime(time_string, fmt)
    except ValueError:
        return None

    return converted_time

class JobNode(object):
    """ simple container class that contains information about the pcp archives associated
       with a node in the job """
    def __init__(self, nodename, nodeidx):
        self._nodename = nodename
        self._nodeidx = nodeidx
        self._rawarchives = []
        self._archive = None

    nodeindex = property(lambda self: self._nodeidx)
    nodename = property(lambda self: self._nodename)

    def set_rawarchives(self, archivelist):
        """ raw archives are the list of any pcp archives that may contain data
            for a job """
        self._rawarchives = archivelist

    @property
    def rawarchives(self):
        """ accessor """
        return self._rawarchives

    def remove(self, archive):
        """ Remove an archive from the list """
        self._rawarchives.remove(archive)

    def set_combinedarchive(self, archive):
        """ The combined archive is the one that contains all data for the job on this node """
        self._archive = archive

    @property
    def archive(self):
        """ accessor """
        return self._archive


def datetimeconvert(intime):
    """ allow some flexibility in specifying the time: either a unixtimestamp
        or a string """

    if isinstance(intime, long) or isinstance(intime, int):
        return datetime.datetime.utcfromtimestamp(intime)
    else:
        return safe_strptime(intime, "%Y-%m-%dT%H:%M:%S")

class Job(object):
    """ Contains the data for a job. """
    # pylint: disable=too-many-instance-attributes

    def __init__(self, job_pk_id, job_id, acct):
        # pylint: disable=too-many-arguments

        self.job_pk_id = job_pk_id
        self.job_id = job_id
        self.acct = acct
        self._nodecount = acct['nodes']
        self._start_datetime = datetimeconvert(acct['start_time'])
        self._end_datetime = datetimeconvert(acct['end_time'])

        # It is neccessary to set the end time to be one second past because the time
        # precision is only per-second
        self._end_datetime += datetime.timedelta(seconds=1)

        self.walltime = acct['end_time'] - acct['start_time']
        self._nodes = OrderedDict()

        self._data = {}
        self.jobdir = None
        self._nodebegin = {}
        self._nodeend = {}

        self._errors = {}

    def __str__(self):
        """ Return a summary string describing the job """
        return "jobid=%s nodes=%s walltime=%s" % (self.job_id, self._nodecount, self.walltime)

    def setjobdir(self, jobdir):
        """
        Set job dir
        """
        self.jobdir = jobdir

    def addnodearchive(self, nodename, node_archive):
        """
        Add the path to the node archive to the list archives for the job
        """
        self._nodes[nodename].set_combinedarchive(node_archive)

    def set_rawarchives(self, node_ar_map):
        """
        Store the list of raw archives that comprise the node
        """
        for nodename, archivelist in node_ar_map.iteritems():
            self._nodes[nodename].set_rawarchives(archivelist)

    def mark_bad_rawarchive(self, nodename, archive_path, reason):
        """
            Mark an archive as bad and remove it from the list of archives to process
        """
        self.record_error(reason)
        self._nodes[nodename].remove(archive_path)

    def rawarchives(self):
        """ iterator for the raw archives for the nodes in the job """
        for nodename, nodedata in self._nodes.iteritems():
            if len(nodedata.rawarchives) > 0:
                yield nodename, nodedata.rawarchives

    def nodearchives(self):
        """ iterator for the combined archives for the nodes in the job """
        for nodename, nodedata in self._nodes.iteritems():
            if nodedata.archive != None:
                yield nodename, nodedata.nodeindex, nodedata.archive

    def has_any_archives(self):
        """ are there any archives for this job """

        for _, nodedata in self._nodes.iteritems():
            if len(nodedata.rawarchives) > 0:
                return True

        return False

    def has_enough_raw_archives(self):
        """ are there enough raw archives for this job to try pmlogextract"""

        num_archives = 0

        for _, nodedata in self._nodes.iteritems():
            if len(nodedata.rawarchives) > 0:
                num_archives += 1

        if float(num_archives)/float(self._nodecount) > 0.95:
            return True
        else:
            return False

    def has_enough_combined_archives(self):
        """ are there enough combined archives for this job to try summarization"""

        num_archives = 0

        for _, nodedata in self._nodes.iteritems():
            if nodedata.archive != None:
                num_archives += 1

        if float(num_archives)/float(self._nodecount) > 0.95:
            return True
        else:
            return False

    def setnodebeginend(self, node, begin, end):
        """
        Set the begin and end times for the given node. If either
        begin or end is None then the default time from the accounting
        data is used
        """
        if begin != None:
            self._nodebegin[node] = begin
        if end != None:
            self._nodeend[node] = end

    def getnodebegin(self, node):
        """
        Get the start time for job data on the given node
        """
        if node in self._nodebegin:
            return self._nodebegin[node]
        else:
            return self.start_datetime

    def getnodeend(self, node):
        """
        Get end time for job data on the given node
        """
        if node in self._nodeend:
            return self._nodeend[node]
        else:
            return self.end_datetime

    @property
    def nodecount(self):
        """ Total number of nodes assigned to the job """
        return self._nodecount

    @property
    def start_datetime(self):
        """
        Gets a datetime object representing the job's start time, or None
        if the string representation can't be converted.

        Returns:
            A datetime object representing the job's start time, or None
            if the string representation can't be converted.
        """
        return self._start_datetime

    def set_nodes(self, nodelist):
        """ Set the list of nodes assigned to the job.  The First entry in the
        list should be the head node """
        for nodeid, node in enumerate(nodelist):
            self._nodes[node] = JobNode(node, nodeid)

    @property
    def end_datetime(self):
        """
        Gets a datetime object representing the job's end time, or None
        if the string representation can't be converted.

        Returns:
            A datetime object representing the job's end time, or None
            if the string representation can't be converted.
        """
        return self._end_datetime

    def get_errors(self):
        """ Return the list of processing errors """
        return self._errors.keys()

    def record_error(self, msg):
        """ record a processing error for the job """
        if msg in self._errors:
            self._errors[msg] += 1
        else:
            self._errors[msg] = 1

    def data(self):
        """ return all job metadata """
        return self._data

    def adddata(self, name, data):
        """ Add job metadata """
        self._data[name] = data

    def getdata(self, name):
        """ return job metadata for name """
        if name in self._data:
            return self._data[name]
        return None
