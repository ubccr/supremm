""" Implementation for account reader that gets data from the XDMoD datawarehouse """
from pymysql import OperationalError, ProgrammingError
from supremm.config import Config
from supremm.accounting import Accounting, ArchiveCache
from supremm.scripthelpers import getdbconnection
from supremm.Job import Job
from supremm.errors import ProcessingError
import logging

class XDMoDAcct(Accounting):
    """ account reader that gets data from xdmod datawarehouse """
    def __init__(self, resource_id, hostname_mode, config):
        super(XDMoDAcct, self).__init__(resource_id, config)

        self.dbsettings = config.getsection("datawarehouse")
        self.hostnamemode = hostname_mode

        xdmod_schema_version = self.detectXdmodSchema()

        if xdmod_schema_version == 9 or xdmod_schema_version == 8:
            jobfacttable = 'job_tasks'

            self._query = """
                SELECT
                    jf.`job_id` AS `job_id`,
                    jf.`resource_id` AS `resource_id`,
                    COALESCE(jf.`local_job_id_raw`, jf.`local_jobid`) AS `local_job_id`,
                    jf.`start_time_ts` AS `start_time`,
                    jf.`end_time_ts` AS `end_time`,
                    jf.`submit_time_ts` AS `submit`,
                    jf.`eligible_time_ts` AS `eligible`,
                    jr.`queue` AS `partition`,
                    jf.`uid_number` AS `uid`,
                    aa.`charge_number` AS `account`,
                    sa.`username` AS `user`,
                    jf.`name` AS `jobname`,
                    jf.`node_count` AS `nodes`,
                    jf.`processor_count` AS `ncpus`,
            """
            if xdmod_schema_version == 9:
                self._query += "    jf.`gpu_count` AS `gpus`,"
            self._query += """
                    jf.`group_name` AS `group`,
                    jf.`gid_number` AS `gid`,
                    jf.`exit_code` AS `exit_code`,
                    jf.`exit_state` AS `exit_status`,
                    jf.`cpu_req` AS `reqcpus`,
                    jf.`mem_req` AS `reqmem`,
                    jf.`timelimit` AS `timelimit`,
                    sj.`source_format` AS `resource_manager`
                FROM
                    modw.job_tasks jf
                        INNER JOIN
                    modw.job_records jr ON jf.job_record_id = jr.job_record_id
                        INNER JOIN
                    modw.systemaccount sa ON jf.systemaccount_id = sa.id
                        INNER JOIN
                    modw.account aa ON jr.account_id = aa.id
                        LEFT JOIN
                    modw_supremm.`process` p ON jf.job_id = p.jobid
                        LEFT JOIN
                    mod_shredder.`shredded_job` sj ON jf.job_id = sj.shredded_job_id
                WHERE
                    jf.resource_id = %s
            """
        else:
            jobfacttable = 'jobfact'

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
                    jf.`timelimit` as `timelimit`,
                    sj.`source_format` AS `resource_manager`
                FROM
                    modw.jobfact jf
                LEFT JOIN
                    modw_supremm.`process` p ON jf.job_id = p.jobid
                INNER JOIN
                    modw.systemaccount sa ON jf.systemaccount_id = sa.id
                INNER JOIN
                    modw.account aa ON jf.account_id = aa.id
                LEFT JOIN
                    mod_shredder.`shredded_job` sj ON jf.job_id = sj.shredded_job_id
                WHERE
                    jf.resource_id = %s
            """

        self.hostquery = """
            SELECT 
                tt.hostname, tt.filename
            FROM (
            SELECT 
                h.hostname, ap.filename, na.start_time_ts
            FROM
                modw_supremm.`archive_paths` ap,
                modw_supremm.`archives_nodelevel` na,
                modw.`hosts` h,
                modw.`jobhosts` jh,
                modw.`{0}` j
            WHERE
                j.job_id = jh.job_id
                    AND jh.job_id = %s
                    AND jh.host_id = h.id
                    AND na.host_id = h.id
                    AND ((j.start_time_ts BETWEEN na.start_time_ts AND na.end_time_ts)
                    OR (j.end_time_ts BETWEEN na.start_time_ts AND na.end_time_ts)
                    OR (j.start_time_ts < na.start_time_ts
                    AND j.end_time_ts > na.end_time_ts))
                    AND ap.id = na.archive_id 
            UNION 
            SELECT 
                h.hostname, ap.filename, ja.start_time_ts
            FROM
                modw_supremm.`archive_paths` ap,
                modw_supremm.`archives_joblevel` ja,
                modw.`hosts` h,
                modw.`jobhosts` jh,
                modw.`{0}` j
            WHERE
                j.job_id = jh.job_id
                    AND jh.job_id = %s
                    AND jh.host_id = h.id
                    AND ja.host_id = h.id
                    AND ja.local_job_id_raw = j.local_job_id_raw
                    AND ja.archive_id = ap.id
            ) tt ORDER BY 1 ASC, tt.start_time_ts ASC
        """.format(jobfacttable)

        self.nodenamequery = """
            SELECT
                h.hostname
            FROM
                modw.`hosts` h,
                modw.`jobhosts` jh,
                modw.`{0}` j
            WHERE
                j.job_id = jh.job_id
                AND jh.job_id = %s
                AND jh.host_id = h.id;
        """.format(jobfacttable)

        self.con = None
        self.hostcon = None
        self.madcon = None
        self.nodenamecon = None

    def detectXdmodSchema(self):
        """ Query the XDMoD datawarehouse to determine which version of the data schema
            is in use """

        xdmod_schema_version = 7

        testconnection = getdbconnection(self.dbsettings, True)
        curs = testconnection.cursor()
        try:
            curs.execute('SELECT 1 FROM `modw`.`job_tasks` LIMIT 1')
            xdmod_schema_version = 8
            try:
                curs.execute('SELECT gpu_count FROM `modw`.`job_tasks` LIMIT 1')
                xdmod_schema_version = 9
            except OperationalError:
                # Operational Error is set if the column does not exist
                pass
        except ProgrammingError:
            # Programming Error is thrown if the job_tasks table does not exist
            pass

        curs.close()
        testconnection.close()

        return xdmod_schema_version

    def getbylocaljobid(self, localjobid):
        """ Yields one or more Jobs that match the localjobid """
        query = self._query + " AND jf.local_job_id_raw = %s"
        data = (self._resource_id, localjobid)

        for job in  self.executequery(query, data):
            yield job

    def getbytimerange(self, start, end, opts):
        """ Search for all jobs based on the time interval. Matches based on the end
        timestamp of the job. Will process jobs in time interval based on the process
        flags"""

        query = self._query + " AND jf.end_time_ts BETWEEN unix_timestamp(%s) AND unix_timestamp(%s)"
        data = (self._resource_id, start, end)

        logging.info("Using time interval: %s - %s", start, end)

        process_selectors=[]
        # ALL & NONE will select the same jobs, simplify the query
        if opts['process_all']:
            logging.info("Processing all jobs")
        else:
            if opts['process_bad']:
                logging.info("Processing bad jobs")
                process_selectors.append("(p.process_version < 0 AND p.process_version > -1000)")
            if opts['process_old']:
                logging.info("Processing old jobs")
                process_selectors.append("(p.process_version > 0 AND p.process_version != %s)")
                data = data + (Accounting.PROCESS_VERSION, )
            if opts['process_notdone']:
                logging.info("Processing unprocessed jobs")
                process_selectors.append("p.process_version IS NULL")
            if opts['process_current']:
                logging.info("Processing processed jobs")
                process_selectors.append("p.process_version = %s")
                data = data + (Accounting.PROCESS_VERSION, )
            if opts['process_big']:
                logging.info("Processing jobs marked previously as too big")
                process_selectors.append("p.process_version = %s")
                data = data + (-1000-ProcessingError.JOB_TOO_BIG, )
            if opts['process_error'] != 0:
                logging.info("Processing jobs marked previously with %s", opts['process_error'])
                process_selectors.append("p.process_version = %s")
                data = data + (opts['process_error'], )

        # Add a "AND ( cond1 OR cond2 ...) clause
        if len(process_selectors) > 0:
            job_selector=" OR ".join(process_selectors)
            job_selector = " AND( " + job_selector + " )"
            query += job_selector

        query += " ORDER BY jf.end_time_ts ASC"

        for job in  self.executequery(query, data):
            yield job

    def get(self, start, end):
        """ Yields all unprocessed jobs. Optionally specify a time interval to process"""

        query = self._query

        query += " AND p.process_version IS NULL"

        data = (self._resource_id, )
        if start != None:
            query += " AND jf.end_time_ts >= %s "
            data = data + (start, )
        if end != None:
            query += " AND jf.end_time_ts < %s "
            data = data + (end, )
        query += " ORDER BY jf.end_time_ts ASC"

        for job in  self.executequery(query, data):
            yield job

    def executequery(self, query, data):
        """ run the sql queries and yield a job object for each result """
        if self.con == None:
            self.con = getdbconnection(self.dbsettings, True)
        if self.hostcon == None:
            self.hostcon = getdbconnection(self.dbsettings, False)
        if self.nodenamecon == None:
            self.nodenamecon = getdbconnection(self.dbsettings, False)

        cur = self.con.cursor()
        cur.execute(query, data)

        rows_returned=cur.rowcount
        logging.info("Processing %s jobs", rows_returned)

        for record in cur:
            hostcur = self.hostcon.cursor()
            hostcur.execute(self.hostquery, (record['job_id'], record['job_id']))

            nodenamecur = self.nodenamecon.cursor()
            nodenamecur.execute(self.nodenamequery, record['job_id'])

            hostarchives = {}
            hostlist = []
            for n in nodenamecur:
                if self.hostnamemode == "hostname":
                    name = n[0].split(".")[0]
                    hostlist.append(name)
                else:
                    hostlist.append(h[0])

            for h in hostcur:
                if h[0] not in hostarchives:
                    hostarchives[h[0]] = []
                hostarchives[h[0]].append(h[1])

            jobpk = record['job_id']
            del record['job_id']
            record['host_list'] = hostlist
            job = Job(jobpk, str(record['local_job_id']), record)
            job.set_nodes(hostlist)
            job.set_rawarchives(hostarchives)

            yield job

    def markasdone(self, job, success, elapsedtime, error=None):
        """ log a job as being processed (either successfully or not) """
        query = """
            INSERT INTO modw_supremm.`process` 
                (jobid, process_version, process_timestamp, process_time) VALUES (%s, %s, NOW(), %s)
            ON DUPLICATE KEY UPDATE process_version = %s, process_timestamp = NOW(), process_time = %s
            """

        if error != None:
            version = -1000 - error
        else:
            version = Accounting.PROCESS_VERSION if success else -1 * Accounting.PROCESS_VERSION

        data = (job.job_pk_id, version, elapsedtime, version, elapsedtime)

        if self.madcon == None:
            self.madcon = getdbconnection(self.dbsettings, False, {'autocommit': True})

        cur = self.madcon.cursor()

        try:
            cur.execute(query, data)
        except OperationalError as e:
            logging.warning("Lost MySQL Connection. " + str(e))
            cur.close()
            self.madcon.close()
            logging.warning("Attempting reconnect")
            self.madcon = getdbconnection(self.dbsettings, False, {'autocommit': True})
            cur = self.madcon.cursor()
            cur.execute(query, data)

class XDMoDArchiveCache(ArchiveCache):
    """ Helper class that adds job accounting records to the database """

    def __init__(self, config):
        super(XDMoDArchiveCache, self).__init__(config)

        self.dbconfig = config.getsection("datawarehouse")
        self.con = getdbconnection(self.dbconfig)
        self._hostnamecache = {}

        cur = self.con.cursor()
        cur.execute("SELECT hostname FROM modw.hosts")
        for host in cur:
            self._hostnamecache[host[0]] = 1

    def insert(self, resource_id, hostname, filename, start, end, jobid):
        """ Insert an archive record """
        try:
            self.insertImpl(resource_id, hostname, filename, start, end, jobid)
        except OperationalError:
            logging.warning("Lost MySQL Connection. Attempting single reconnect")
            self.con = getdbconnection(self.dbconfig)
            self.insertImpl(resource_id, hostname, filename, start, end, jobid)

    def insertImpl(self, resource_id, hostname, filename, start, end, jobid):
        """ Main implementation of archive record insert """
        cur = self.con.cursor()
        if hostname not in self._hostnamecache:
            logging.debug("Ignoring archive for host \"%s\" because there are no jobs in the XDMoD datawarehouse that ran on this host.", hostname)
            return

        filenamequery = """INSERT INTO `modw_supremm`.`archive_paths` (`filename`) VALUES (%s) ON DUPLICATE KEY UPDATE id = id """

        cur.execute(filenamequery, [filename])
        if cur.lastrowid != 0:
            filenamequery = "%s"
            filenameparam = cur.lastrowid
        else:
            filenamequery = "(SELECT id FROM `modw_supremm`.`archive_paths` WHERE `filename` = %s)"
            filenameparam = filename

        if jobid != None:
            query = """INSERT INTO `modw_supremm`.`archives_joblevel`
                            (archive_id, host_id, local_jobid, local_job_array_index, local_job_id_raw, start_time_ts, end_time_ts)
                       VALUES (
                            {0},
                            (SELECT id FROM modw.hosts WHERE hostname = %s),
                            %s,
                            FLOOR(%s),
                            CEILING(%s)
                       )
                       ON DUPLICATE KEY UPDATE start_time_ts = VALUES(start_time_ts), end_time_ts = VALUES(end_time_ts)
                    """.format(filenamequery)

            cur.execute(query, [filenameparam, hostname, jobid[0], jobid[1], jobid[2], start, end])
        else:
            query = """INSERT INTO `modw_supremm`.`archives_nodelevel`
                            (archive_id, host_id, start_time_ts, end_time_ts)
                       VALUES (
                            {0},
                            (SELECT id FROM modw.hosts WHERE hostname = %s),
                            FLOOR(%s),
                            CEILING(%s)
                       )
                       ON DUPLICATE KEY UPDATE start_time_ts = VALUES(start_time_ts), end_time_ts = VALUES(end_time_ts)
                    """.format(filenamequery)

            cur.execute(query, [filenameparam, hostname, start, end])

        self.postinsert()

    def postinsert(self):
        """
        Must be called after insert.
        """
        self.con.commit()

    def insert_from_files(self, paths_file, joblevel_file, nodelevel_file):
        cur = self.con.cursor()

        paths_tmp_table = """
        CREATE TEMPORARY TABLE `modw_supremm`.`archive_paths_load`
        (`filename` varchar(255) COLLATE utf8_unicode_ci NOT NULL, UNIQUE KEY `filename` (`filename`)) DEFAULT CHARSET=utf8;
        """
        cur.execute(paths_tmp_table)

        paths_load = """
        LOAD DATA LOCAL INFILE '{}' IGNORE INTO TABLE `modw_supremm`.`archive_paths_load`
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\\''
        LINES TERMINATED BY '\n'
        (filename);
        """.format(paths_file)
        cur.execute(paths_load)

        paths_query = """
        INSERT INTO `modw_supremm`.`archive_paths`
        (filename)
        SELECT `filename`
        FROM `modw_supremm`.`archive_paths_load`
        ON DUPLICATE KEY UPDATE id = id;
        """
        cur.execute(paths_query)

        joblevel_tmp_table = """
        CREATE TEMPORARY TABLE `modw_supremm`.`joblevel_load` (
        `arch_path` VARCHAR(255) NOT NULL,
        `host_name` VARCHAR(255) NOT NULL,
        `local_jobid` int(11) NOT NULL,
        `local_job_array_index` int(11) NOT NULL,
        `local_job_id_raw` int(11) NOT NULL,
        `start_time_ts` int(11) NOT NULL,
        `end_time_ts` int(11) NOT NULL) COLLATE=utf8_unicode_ci;
        """
        cur.execute(joblevel_tmp_table)

        joblevel_load = """
        LOAD DATA LOCAL INFILE '{}' REPLACE INTO TABLE `modw_supremm`.`joblevel_load`
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\\''
        LINES TERMINATED BY '\n'
        (arch_path, host_name, local_jobid, local_job_array_index, local_job_id_raw, start_time_ts, end_time_ts)
        """.format(joblevel_file)
        cur.execute(joblevel_load)

        joblevel_query = """
        INSERT INTO `modw_supremm`.`archives_joblevel`
        (archive_id, host_id, local_jobid, local_job_array_index, local_job_id_raw, start_time_ts, end_time_ts)
        SELECT p.id, h.id, jl.local_jobid, jl.local_job_array_index, jl.local_job_id_raw, jl.start_time_ts, jl.end_time_ts
        FROM `modw_supremm`.`joblevel_load` jl, `modw`.`hosts` h, `modw_supremm`.`archive_paths` p
        WHERE h.hostname = jl.host_name AND p.filename = jl.arch_path
        ON DUPLICATE KEY UPDATE start_time_ts = VALUES(start_time_ts), end_time_ts = VALUES(end_time_ts)
        """
        cur.execute(joblevel_query)

        nodelevel_tmp_table = """
        CREATE TEMPORARY TABLE `modw_supremm`.`nodelevel_load` (
        `arch_path` VARCHAR(255) NOT NULL,
        `host_name` VARCHAR(255) NOT NULL,
        `start_time_ts` int(11) NOT NULL,
        `end_time_ts` int(11) NOT NULL) COLLATE=utf8_unicode_ci;
        """
        cur.execute(nodelevel_tmp_table)

        nodelevel_load = """
        LOAD DATA LOCAL INFILE '{}' REPLACE INTO TABLE `modw_supremm`.`nodelevel_load`
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\\'' ESCAPED BY '\\\\'
        LINES TERMINATED BY '\n'
        (arch_path, host_name, start_time_ts, end_time_ts)
        """.format(nodelevel_file)
        cur.execute(nodelevel_load)

        nodelevel_query = """
        INSERT INTO `modw_supremm`.`archives_nodelevel`
        (archive_id, host_id, start_time_ts, end_time_ts)
        SELECT p.id, h.id, nl.start_time_ts, nl.end_time_ts
        FROM `modw_supremm`.`nodelevel_load` nl, `modw`.`hosts` h, `modw_supremm`.`archive_paths` p
        WHERE h.hostname = nl.host_name AND p.filename = nl.arch_path
        ON DUPLICATE KEY UPDATE start_time_ts = VALUES(start_time_ts), end_time_ts = VALUES(end_time_ts)
        """
        cur.execute(nodelevel_query)

        cur.execute("DROP TEMPORARY TABLE `modw_supremm`.`archive_paths_load`;")
        cur.execute("DROP TEMPORARY TABLE `modw_supremm`.`joblevel_load`;")
        cur.execute("DROP TEMPORARY TABLE `modw_supremm`.`nodelevel_load`;")

        self.con.commit()


def test():
    """ simple test function """

    config = Config()
    xdm = XDMoDAcct(13, config)
    for job in xdm.get(1444151688, None):
        print(job)


if __name__ == "__main__":
    test()
