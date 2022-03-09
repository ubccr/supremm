"""" tests for the pcp archive processor """
import unittest
from supremm.pcp_common.indexarchives import PcpArchiveProcessor

class TestPcpArchiveProcessor(unittest.TestCase):
    """ Tests for the pcp filename string parser code """

    def setUp(self):
        """ setUp """
        self.inst = PcpArchiveProcessor({'hostname_mode': 'hostname'})

    def test_archivestringmatching(self):
        """ test timestamp parsing """

        testCases = {
            'jo.log.ex.e-end-20180614.09.48.29.index': None,
            'job-2671016.index': None,
            'job-2679009[431].index': None,
            'job-123423-end-20181004.04.05.41.index': 1538625941.0,
            'job-123423-begin-20181004.04.05.41.index': 1538625941.0,
            'job-123423-postbegin-20181004.04.05.41.index': 1538625941.0,
            'job-123423[234]-end-20181004.04.05.41.index': 1538625941.0,
            'job-123423[]-end-20181004.04.05.41.index': 1538625941.0,
            'job-123423[234].server.net-end-20181004.04.05.41.index': 1538625941.0,
            'job-123423[234].server.net-postbegin-20181004.04.05.41.index': 1538625941.0,
            'job-123423[234].server.net-begin-20181004.04.05.41.index': 1538625941.0,
            'job-123423.server.net-end-20181004.04.05.41.index': 1538625941.0
        }

        for archiveName, expected in testCases.items():
            assert self.inst.get_archive_data_fast('/some/path/to/data/' + archiveName) == expected

    def test_jobidparser(self):
        """ test jobid parsing """

        testCases = {
            'jo.log.ex.e-end-20180614.09.48.29.index': None,
            '20180729.04.36.index': None,
            'job-2671016.index': (-1, -1, 2671016),
            'job-2673760.index': (-1, -1, 2673760),
            'job-2671022.login.example.edu-end-20180830.02.54.25.index': (-1, -1, 2671022),
            'job-2673760.login.example.edu-end-20180830.02.40.28.index': (-1, -1, 2673760),
            'job-2673760.login.example.edu-end-20180830.02.50.16.index': (-1, -1, 2673760),
            'job-1450543.login.example.edu-postbegin-20180830.00.00.00.index': (-1, -1, 1450543),
            'job-1450554.login.example.edu-postbegin-20180830.00.00.00.index': (-1, -1, 1450554),
            'job-2676199[18].index': (2676199, 18, -1),
            'job-2679009[431].index': (2679009, 431, -1),
            'job-1451551[326].hd-20180614.13.26.33.index': (1451551, 326, -1),
            'job-2676200[18].login.example.edu-end-20180830.02.45.38.index': (2676200, 18, -1),
            'job-2676200[18].login.example.edu-end-20180830.02.46.54.index': (2676200, 18, -1),
            'job-2679009[431].login.example.edu-end-20180904.18.38.02.index': (2679009, 431, -1),
            'job-2679136[520].login.example.edu-postbegin-20180614.00.00.00.index': (2679136, 520, -1),
            'job-2679136[523].login.example.edu-postbegin-20180614.00.00.00.index': (2679136, 523, -1),
            'job-1450512[4].login.example.edu-postbegin-20180614.00.00.00.index': (1450512, 4, -1),
            'job-123423-end-20181004.04.05.41.index': (-1, -1, 123423),
            'job-123423[234]-end-20181004.04.05.41.index': (123423, 234, -1),
            'job-123423[]-end-20181004.04.05.41.index': (-1, -1, 123423),
            'job-end-20181004.04.05.41.index': None,
            'job-123423[234].server.net-end-20181004.04.05.41.index': (123423, 234, -1),
            'job-123423.server.net-end-20181004.04.05.41.index': (-1, -1, 123423)
        }

        for archiveName, expected in testCases.items():
            assert self.inst.parsejobid(archiveName) == expected
