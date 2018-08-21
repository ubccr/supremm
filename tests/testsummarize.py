import unittest
from mock import patch, Mock
from supremm.proc_common import summarizejob
from supremm.config import Config
from supremm.Job import Job
from supremm.errors import ProcessingError

import logging
import datetime
import tempfile

class TestSummarizeJob(unittest.TestCase):

    def setUp(self):
        confattrs = {'getsection.return_value': {}}
        self.mockconf = Mock(spec=Config, **confattrs)

        self.mockresconf = {
            'name': 'resource_name'
        }

        self.options = {
                'fail_fast': False,
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

    @staticmethod
    def verify_errors(expectedProcessing, expectedMdata, actual_error, actual_mdata):
        assert expectedProcessing == actual_error

        assert expectedMdata in actual_mdata
        assert actual_mdata[expectedMdata]

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_job_too_short(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(walltime = 128)
        self.options['min_duration'] = 129

        _, mdata, _, error = summarizejob(self.mockjob, self.mockconf, {}, [], [], self.options)

        self.verify_errors(ProcessingError.TIME_TOO_SHORT, 'skipped_too_short', error, mdata)

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_parallel_too_short(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(walltime = 599, nodecount = 10)
        self.options['min_parallel_duration'] = 600

        _, mdata, _, error = summarizejob(self.mockjob, self.mockconf, {}, [], [], self.options)

        self.verify_errors(ProcessingError.PARALLEL_TOO_SHORT, 'skipped_parallel_too_short', error, mdata)

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_invlid_nodecount(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(nodecount = 0)

        _, mdata, _, error = summarizejob(self.mockjob, self.mockconf, {}, [], [], self.options)

        self.verify_errors(ProcessingError.INVALID_NODECOUNT, 'skipped_invalid_nodecount', error, mdata)

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_jobtoolong(self, summaryclass, extract):
        extract.return_value = 0

        self.mockjob.configure_mock(walltime = 99999999)

        _, mdata, _, error = summarizejob(self.mockjob, self.mockconf, {}, [], [], self.options)

        self.verify_errors(ProcessingError.TIME_TOO_LONG, 'skipped_too_long', error, mdata)

    @patch('supremm.proc_common.extract_and_merge_logs')
    @patch('supremm.proc_common.Summarize')
    def test_jobtoonodehours(self, summaryclass, extract):
        """ test the too many nodehours error """
        extract.return_value = 0

        self.mockjob.configure_mock(walltime=1000, nodecount=500)
        self.options['max_nodetime'] = 499999

        _, mdata, _, error = summarizejob(self.mockjob, self.mockconf, {}, [], [], self.options)

        self.verify_errors(ProcessingError.JOB_TOO_MANY_NODEHOURS, 'skipped_job_nodehours', error, mdata)

    @patch('supremm.pcparchive.adjust_job_start_end')
    @patch('supremm.pcparchive.pmlogextract')
    def test_pmlogextract(self, pmlogextracnfn, adjustjobfn):
        
        pmlogextracnfn.return_value = -10

        _, mdata, _, error = summarizejob(self.mockjob, self.mockconf, {}, [], [], self.options)

        self.verify_errors(ProcessingError.PMLOGEXTRACT_ERROR, 'skipped_pmlogextract_error', error, mdata)

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

        _, mdata, _, error = summarizejob(self.mockjob, self.mockconf, self.mockresconf, [], [], self.options)

        self.verify_errors(ProcessingError.PMLOGEXTRACT_ERROR, 'skipped_pmlogextract_error', error, mdata)

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

        _, mdata, _, error = summarizejob(self.mockjob, self.mockconf, self.mockresconf, [], [], self.options)

        self.verify_errors(ProcessingError.PMLOGEXTRACT_ERROR, 'skipped_pmlogextract_error', error, mdata)

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

        _, mdata, _, error = summarizejob(self.mockjob, self.mockconf, self.mockresconf, [], [], self.options)

        self.verify_errors(ProcessingError.PMLOGEXTRACT_ERROR, 'skipped_pmlogextract_error', error, mdata)


if __name__ == '__main__':
    unittest.main()
