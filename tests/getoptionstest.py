import unittest
from mock import patch
from supremm.proc_common import getoptions
import sys
import logging
import datetime

class TestGetOptions(unittest.TestCase):

    def setUp(self):
        self.defaults = {
                'dodelete': True,
                'extractonly': False,
                'force_timeout': 172800,
                'job_output_dir': None,
                'libextract': False,
                'log': logging.INFO,
                'max_nodes': 0,
                'min_duration': None,
                'min_parallel_duration': None,
                'max_duration': 864000,
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

    def helper(self, args, expected):
        testargs = ['processname'] + args

        with patch.object(sys, 'argv', testargs):
            opts = getoptions(False)
            self.assertDictEqual(expected, opts)

    def test_defaults(self):
        self.helper([], self.defaults)

    def test_invalid_settings0(self):

        testargs = ['procname', '--process-all']

        with patch.object(sys, 'argv', testargs):
            with self.assertRaises(SystemExit):
                opt = getoptions(False)

    def test_invalid_settings2(self):

        testargs = ['procname', '--localjobid', '34']

        with patch.object(sys, 'argv', testargs):
            with self.assertRaises(SystemExit):
                opt = getoptions(False)

    def test_invalid_settings3(self):

        testargs = ['procname', '--start', '34']

        with patch.object(sys, 'argv', testargs):
            with self.assertRaises(ValueError):
                opt = getoptions(False)

    def test_invalid_settings4(self):

        testargs = ['procname', '--start', '2015-02-03']

        with patch.object(sys, 'argv', testargs):
            with self.assertRaises(SystemExit):
                opt = getoptions(False)

    def testspecify_job(self):
        testargs = ['--localjobid', '1', '--resource', '1']
        expected = self.defaults.copy()
        expected['resource'] = '1'
        expected['local_job_id'] = '1'
        expected['mode'] = 'single'

        self.helper(testargs, expected)

    def testspecify_timout(self):
        testargs = ['--min-duration', '600']
        expected = self.defaults.copy()
        expected['min_duration'] = 600

        self.helper(testargs, expected)

    def testspecify_paralleltimout(self):
        testargs = ['--min-parallel-duration', '600']
        expected = self.defaults.copy()
        expected['min_parallel_duration'] = 600

        self.helper(testargs, expected)

    def testspecify_timerange(self):
        testargs = ['-s', '2015-01-01', '--end', '2016-01-01']
        expected = self.defaults.copy()
        expected['start'] = datetime.datetime(2015, 1, 1, 0, 0)
        expected['end'] = datetime.datetime(2016, 1, 1, 0, 0)
        expected['mode'] = 'timerange'
        expected['process_all'] = True
        expected['process_bad'] = False
        expected['process_old'] = False
        expected['process_notdone'] = False

        self.helper(testargs, expected)

    def test_specifyresource(self):
        testargs = ['-r', 'hpc']
        expected = self.defaults.copy()
        expected['resource'] = 'hpc'
        expected['mode'] = 'resource'

        self.helper(testargs, expected)

    def test_specifyall(self):
        self.maxDiff = None
        testargs = ['-r', 'hpc', '-s', '2016-02-01', '-e', '2016-02-02', '-B', '-O', '-N', '-C']
        expected = self.defaults.copy()
        expected['start'] = datetime.datetime(2016, 2, 1, 0, 0)
        expected['end'] = datetime.datetime(2016, 2, 2, 0, 0)
        expected['mode'] = 'timerange'
        expected['resource'] = 'hpc'
        expected['process_all'] = True
        expected['process_bad'] = True
        expected['process_old'] = True
        expected['process_notdone'] = True
        expected['process_current'] = True

        self.helper(testargs, expected)

    def testsetloglevel(self):
        expected = self.defaults.copy()
        expected['log'] = logging.DEBUG
        self.helper(['-d'], expected)

        expected['log'] = logging.ERROR
        self.helper(['-q'], expected)

    def testsetmaxduration(self):
        expected = self.defaults.copy()
        expected['max_duration'] = 660

        self.helper(['--max-duration', '660'], expected)

    def testsettag(self):
        expected = self.defaults.copy()
        expected['tag'] = 'job tag'

        self.helper(['--tag', 'job tag'], expected)

    def testsetthreads(self):
        expected = self.defaults.copy()
        expected['threads'] = 4

        self.helper(['-t', '4'], expected)

    def testdumpprolist(self):
        expected = self.defaults.copy()
        expected['dump_proclist'] = True

        self.helper(['--dump-proclist'], expected)

    def testmaxnodetime(self):
        expected = self.defaults.copy()
        expected['max_nodetime'] = 3455

        self.helper(['--max-nodetime', "3455"], expected)

if __name__ == '__main__':
    unittest.main()
