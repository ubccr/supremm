""" Implementation for account reader that gets data from the XDMoD datawarehouse """

from supremm.config import Config
from supremm.accounting import Accounting, ArchiveCache
from supremm.scripthelpers import getdbconnection
from supremm.Job import Job
import logging

class XDMoDAcct(Accounting):
    """ account reader that gets data from xdmod datawarehouse """
    def __init__(self, resource_id, config, nthreads, threadidx):
        super(XDMoDAcct, self).__init__(resource_id, config, nthreads, threadidx)

        self._query = """
            SELECT 
                jf.`job_id` as `job_id`,
                jf.`resource_id` as `resource_id`, 
                COALESCE(jf.`local_job_id_raw`, jf.`local_jobid`) as `local_job_id`,
                jf.`start_time_ts` as `start_time`,
                jf.`end_time_ts` as `end_time`,
                jf.`submit_time_ts` as `submit`,
                jf.`eligible_time_ts` as `eligible`,
                jf.`queue_id` as `partition`,
                jf.`uid_number` as `uid`,
                aa.`charge_number` as `account`,
                sa.`username` as `user`,
                jf.`name` as `jobname`,
                jf.`nodecount` as `nodes`,
                jf.`processors` as `ncpus`,
                jf.`group_name` as `group`,
                jf.`gid_number` as `gid`,
                jf.`exit_code` as `exit_code`,
                jf.`exit_state` as `exit_status`,
                jf.`cpu_req` as `reqcpus`,
                jf.`mem_req` as `reqmem`,
                jf.`timelimit` as `timelimit`
            FROM 
                modw.jobfact jf
            LEFT JOIN 
                modw_supremm.`process` p ON jf.job_id = p.jobid
            INNER JOIN 
                modw.systemaccount sa ON jf.systemaccount_id = sa.id
            INNER JOIN
                modw.account aa ON jf.account_id = aa.id
            WHERE
                jf.resource_id = %s 
              """

        self.hostquery = """SELECT
                           h.hostname, a.filename
                       FROM
                           modw.`hosts` h,
                           modw_supremm.`archive` a,
                           modw.`jobhosts` jh,
                           modw.`jobfact` j
                       WHERE
                           j.job_id = jh.job_id 
                           AND jh.job_id = %s 
                           AND jh.host_id = h.id
                           AND a.hostid = h.id
                           AND (
                               (j.start_time_ts BETWEEN a.start_time_ts AND a.end_time_ts)
                               OR (j.end_time_ts BETWEEN a.start_time_ts AND a.end_time_ts)
                               OR (j.start_time_ts < a.start_time_ts and j.end_time_ts > a.end_time_ts)
                               OR (CAST(j.local_job_id_raw AS CHAR) = a.jobid)
                           )
                           AND (a.jobid = CAST(j.local_job_id_raw AS CHAR) OR a.jobid IS NULL)
                       GROUP BY 1, 2 ORDER BY 1 ASC, a.start_time_ts ASC """

        self.dbsettings = config.getsection("datawarehouse")
        self.con = None
        self.hostcon = None
        self.madcon = None

    def getbylocaljobid(self, localjobid):
        """ Yields one or more Jobs that match the localjobid """
        query = self._query + " AND jf.local_job_id_raw = %s"
        data = (self._resource_id, localjobid)

        for job in  self.executequery(query, data):
            yield job

    def getbytimerange(self, start, end, onlynew):
        """ Search for all jobs based on the time interval. Matches based on the end
        timestamp of the job. Will process all jobs in time interval whether or not
        they have already been processed """

        query = self._query + " AND jf.end_time_ts BETWEEN unix_timestamp(%s) AND unix_timestamp(%s)"
        data = (self._resource_id, start, end)

        if onlynew != None and onlynew != False:
            logging.info("Processing only unprocessed jobs by timerange")
            query += " AND (p.process_version != %s OR p.process_version IS NULL)"
            data = data + (Accounting.PROCESS_VERSION, )

        if self._nthreads != None and self._threadidx != None:
            query += " AND (CRC32(jf.local_job_id_raw) %% %s) = %s"
            data = data + (self._nthreads, self._threadidx)
        query += " ORDER BY jf.nodecount DESC"

        for job in  self.executequery(query, data):
            yield job

    def get(self, start, end):
        """ Yields all unprocessed jobs. Optionally specify a time interval to process"""

        query = self._query

        query += " AND (p.process_version != %s OR p.process_version IS NULL)"

        data = (self._resource_id, Accounting.PROCESS_VERSION)
        if start != None:
            query += " AND jf.end_time_ts >= %s "
            data = data + (start, )
        if end != None:
            query += " AND jf.end_time_ts < %s "
            data = data + (end, )
        if self._nthreads != None and self._threadidx != None:
            query += " AND (CRC32(jf.local_job_id_raw) %% %s) = %s"
            data = data + (self._nthreads, self._threadidx)
        query += " ORDER BY jf.end_time_ts ASC"

        for job in  self.executequery(query, data):
            yield job

    def executequery(self, query, data):
        """ run the sql queries and yield a job object for each result """
        if self.con == None:
            self.con = getdbconnection(self.dbsettings, True)
        if self.hostcon == None:
            self.hostcon = getdbconnection(self.dbsettings, False)

        cur = self.con.cursor()
        cur.execute(query, data)

        for record in cur:

            hostcur = self.hostcon.cursor()
            hostcur.execute(self.hostquery, (record['job_id'], ))

            hostarchives = {}
            hostlist = []
            for h in hostcur:
                if h[0] not in hostarchives:
                    hostlist.append(h[0])
                    hostarchives[h[0]] = []
                hostarchives[h[0]].append(h[1])

            jobpk = record['job_id']
            del record['job_id']
            record['host_list'] = hostlist
            job = Job(jobpk, str(record['local_job_id']), record)
            job.set_nodes(hostlist)
            job.set_rawarchives(hostarchives)

            yield job

    def markasdone(self, job, success, elapsedtime):
        """ log a job as being processed (either successfully or not) """
        query = """
            INSERT INTO modw_supremm.`process` 
                (jobid, process_version, process_timestamp, process_time) VALUES (%s, %s, NOW(), %s)
            ON DUPLICATE KEY UPDATE process_version = %s, process_timestamp = NOW(), process_time = %s
            """

        version = Accounting.PROCESS_VERSION if success else -1 * Accounting.PROCESS_VERSION
        data = (job.job_pk_id, version, elapsedtime, version, elapsedtime)

        if self.madcon == None:
            self.madcon = getdbconnection(self.dbsettings, False)

        cur = self.madcon.cursor()
        cur.execute(query, data)
        self.madcon.commit()


class XDMoDArchiveCache(ArchiveCache):
    """ Helper class that adds job accounting records to the database """

    def __init__(self, config):
        super(XDMoDArchiveCache, self).__init__(config)

        self.con = getdbconnection(config.getsection("datawarehouse"))
        self.buffered = 0
        self._hostnamecache = {}

        cur = self.con.cursor()
        cur.execute("SELECT hostname FROM modw.hosts")
        for host in cur:
            self._hostnamecache[host[0]] = 1

    def insert(self, resource_id, hostname, filename, start, end, jobid):
        """ Insert a job record """
        cur = self.con.cursor()
        if hostname not in self._hostnamecache:
            logging.debug("Ignoring archive for host %s", hostname)
            return

        query = """INSERT INTO modw_supremm.archive (hostid, filename, start_time_ts, end_time_ts, jobid) 
                       VALUES( (SELECT id FROM modw.hosts WHERE hostname = %s),%s,%s,%s,%s) 
                       ON DUPLICATE KEY UPDATE start_time_ts=%s, end_time_ts=%s"""

        cur.execute(query, [hostname, filename, start, end, jobid, start, end])

        self.buffered += 1
        if self.buffered > 100:
            self.con.commit()
            self.buffered = 0

    def postinsert(self):
        """
        Must be called after insert.
        """
        self.con.commit()


def test():
    """ simple test function """

    config = Config()
    xdm = XDMoDAcct(13, config, None, None)
    for job in xdm.get(1444151688, None):
        print job


if __name__ == "__main__":
    test()
