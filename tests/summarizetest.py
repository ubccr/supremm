import unittest
from mock import patch, Mock
import supremm
from supremm.proc_common import summarizejob
from supremm.summarize import Summarize
from supremm.outputter import MongoOutput
from supremm.account import DbAcct
from supremm.config import Config
from supremm.Job import Job
from supremm.errors import ProcessingError

import sys
import logging
import datetime

class TestSummarizeJob(unittest.TestCase):

    def setUp(self):
        confattrs = {'getsection.return_value': {}}
        self.mockconf = Mock(spec=Config, **confattrs)
        self.mockoutput = Mock(spec=MongoOutput)
        self.mocklog = Mock(spec=DbAcct)

        self.options = {
                'dodelete': True,
                'extractonly': False,
                'force_timeout': 172800,
                'job_output_dir': None,
                'libextract': False,
                'log': logging.INFO,
                'max_nodes': 0,
                'min_duration': None,
                'min_parallel_duration': None,
                'max_duration': 176400,
                'max_nodetime': None,
                'mode': 'all',
                'process_all': False,
                'process_bad': True,
                'process_big': False,
                'process_current': False,
                'process_error': 0,
                'process_notdone': True,
                'process_old': True,
                'resource': None,
                'tag': None,
                'dump_proclist': False,
                'threads': 1
        }

        self.mockjob = Mock(
                spec=Job,
                job_id = '1',
                nodecount = 1,
                walltime = 600,
            end_datetime = datetime.datetime(2016,1,1),
            jobdir = None)

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_job_too_short(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(walltime = 128)
        self.options['min_duration'] = 129

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.mocklog.markasdone.assert_called_once()
        summarizeerror = self.mocklog.markasdone.call_args[0][3]
        self.assertEquals(ProcessingError.TIME_TOO_SHORT, summarizeerror)

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_parallel_too_short(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(walltime = 599, nodecount = 10)
        self.options['min_parallel_duration'] = 600

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.mocklog.markasdone.assert_called_once()
        summarizeerror = self.mocklog.markasdone.call_args[0][3]
        self.assertEquals(ProcessingError.PARALLEL_TOO_SHORT, summarizeerror)

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_invlid_nodecount(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(nodecount = 0)

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.mocklog.markasdone.assert_called_once()
        summarizeerror = self.mocklog.markasdone.call_args[0][3]
        self.assertEquals(ProcessingError.INVALID_NODECOUNT, summarizeerror)

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_jobtoolong(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(walltime = 99999999)

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.mocklog.markasdone.assert_called_once()
        summarizeerror = self.mocklog.markasdone.call_args[0][3]
        self.assertEquals(ProcessingError.TIME_TOO_LONG, summarizeerror)

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_jobtoonodehours(self, summaryclass, extract):
        """ test the too many nodehours error """
        extract.return_value = 0

        self.mockjob.configure_mock(walltime=1000, nodecount=500)
        self.options['max_nodetime'] = 499999

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.mocklog.markasdone.assert_called_once()
        summarizeerror = self.mocklog.markasdone.call_args[0][3]
        self.assertEquals(ProcessingError.JOB_TOO_MANY_NODEHOURS, summarizeerror)

if __name__ == '__main__':
    unittest.main()
