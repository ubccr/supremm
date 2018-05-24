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
import tempfile

class TestSummarizeJob(unittest.TestCase):

    def setUp(self):
        confattrs = {'getsection.return_value': {}}
        self.mockconf = Mock(spec=Config, **confattrs)
        self.mockoutput = Mock(spec=MongoOutput)
        self.mocklog = Mock(spec=DbAcct)

        self.mockresconf = {
            'name': 'resource_name'
        }

        self.options = {
                'dry_run': False,
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

        confjob = {
            'job_id': '1',
            'nodecount': 1,
            'walltime': 600,
            'rawarchives.return_value': iter([('nodename', ['archive1', 'archive2'])]),
            'end_datetime': datetime.datetime(2016,1,1),
            'jobdir': None
        }
        self.mockjob = Mock(spec=Job, **confjob)

    def verify_errors(self, expectedProcessing, expectedMdata):

        self.mocklog.markasdone.assert_called_once()
        summarizeerror = self.mocklog.markasdone.call_args[0][3]
        self.assertEquals(expectedProcessing, summarizeerror)

        self.mockoutput.process.assert_called_once()
        mdata = self.mockoutput.process.call_args[0][1]
        self.assertIn(expectedMdata, mdata)
        self.assertEquals(True, mdata[expectedMdata])


    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_job_too_short(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(walltime = 128)
        self.options['min_duration'] = 129

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.verify_errors(ProcessingError.TIME_TOO_SHORT, 'skipped_too_short')

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_parallel_too_short(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(walltime = 599, nodecount = 10)
        self.options['min_parallel_duration'] = 600

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.verify_errors(ProcessingError.PARALLEL_TOO_SHORT, 'skipped_parallel_too_short')

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_invlid_nodecount(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(nodecount = 0)

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.verify_errors(ProcessingError.INVALID_NODECOUNT, 'skipped_invalid_nodecount')

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_jobtoolong(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(walltime = 99999999)

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.verify_errors(ProcessingError.TIME_TOO_LONG, 'skipped_too_long')

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_jobtoonodehours(self, summaryclass, extract):
        """ test the too many nodehours error """
        extract.return_value = 0

        self.mockjob.configure_mock(walltime=1000, nodecount=500)
        self.options['max_nodetime'] = 499999

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.verify_errors(ProcessingError.JOB_TOO_MANY_NODEHOURS, 'skipped_job_nodehours')

    @patch('supremm.pcparchive.adjust_job_start_end')
    @patch('supremm.pcparchive.pmlogextract')
    def test_pmlogextract(self, pmlogextracnfn, adjustjobfn):
        
        pmlogextracnfn.return_value = -10

        summarizejob(self.mockjob, self.mockconf, {}, [], [], self.mockoutput, self.mocklog, self.options)

        self.verify_errors(ProcessingError.PMLOGEXTRACT_ERROR, 'skipped_pmlogextract_error')

    @patch('supremm.pcparchive.adjust_job_start_end')
    @patch('supremm.pcparchive.getextractcmdline')
    @patch('subprocess.Popen')
    def test_pmlogextractfail0(self, popen, getextractcmdline, adjustjobfn):
        
        popensettings = {
                'communicate.return_value': ("","__pmLogPutResult2: write failed: returns 804876 expecting 954704: No space left on device"), 
                'returncode': 1
        }
        popen.return_value = Mock(**popensettings)

        configres = {'getsection.return_value': {'subdir_out_format': '%j', 'archive_out_dir': tempfile.mkdtemp()}}
        self.mockconf.configure_mock(**configres)

        summarizejob(self.mockjob, self.mockconf, self.mockresconf, [], [], self.mockoutput, self.mocklog, self.options)

        self.verify_errors(ProcessingError.PMLOGEXTRACT_ERROR, 'skipped_pmlogextract_error')

    @patch('supremm.pcparchive.adjust_job_start_end')
    @patch('supremm.pcparchive.getextractcmdline')
    @patch('subprocess.Popen')
    def test_pmlogextractfail1(self, popen, getextractcmdline, adjustjobfn):
        
        popensettings = {
                'communicate.return_value': ("","pmlogextract: Warning: no qualifying records found."),
                'returncode': 1
        }
        popen.return_value = Mock(**popensettings)

        configres = {'getsection.return_value': {'subdir_out_format': '%j', 'archive_out_dir': tempfile.mkdtemp()}}
        self.mockconf.configure_mock(**configres)

        summarizejob(self.mockjob, self.mockconf, self.mockresconf, [], [], self.mockoutput, self.mocklog, self.options)

        self.verify_errors(ProcessingError.PMLOGEXTRACT_ERROR, 'skipped_pmlogextract_error')

    @patch('supremm.pcparchive.adjust_job_start_end')
    @patch('supremm.pcparchive.getextractcmdline')
    @patch('subprocess.Popen')
    def test_pmlogextractfail2(self, popen, getextractcmdline, adjustjobfn):
        
        popensettings = {
                'communicate.return_value': ("","__pmLogPutResult2: write failed: returns 491416 expecting 1246712: Cannot allocate memory"),
                'returncode': 1
        }
        popen.return_value = Mock(**popensettings)

        configres = {'getsection.return_value': {'subdir_out_format': '%j', 'archive_out_dir': tempfile.mkdtemp()}}
        self.mockconf.configure_mock(**configres)

        summarizejob(self.mockjob, self.mockconf, self.mockresconf, [], [], self.mockoutput, self.mocklog, self.options)

        self.verify_errors(ProcessingError.PMLOGEXTRACT_ERROR, 'skipped_pmlogextract_error')


if __name__ == '__main__':
    unittest.main()
