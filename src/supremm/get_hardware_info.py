#!/usr/bin/env python
""" Script that reads hardware info from the archives 
    and outputs the data into a json file

    @author Max Dudek <maxdudek@gmail.com>
"""

import argparse
from datetime import datetime, timedelta
import logging
import os
import re
from re import sub, search
import time
from copy import deepcopy

import json
from collections import defaultdict

from pcp import pmapi
import cpmapi as c_pmapi

from supremm.config import Config
from supremm.scripthelpers import parsetime, setuplogger
from supremm.indexarchives import PcpArchiveFinder
from supremm.account import DbArchiveCache
from supremm.xdmodaccount import XDMoDArchiveCache

# Testing only
import sys
import traceback

DAY_DELTA = 3

STAGING_COLUMNS = [
    'hostname',
    'manufacturer',
    'codename',
    'model_name',
    'clock_speed',
    'core_count',
    'board_manufacturer',
    'board_name',
    'board_version',
    'system_manufacturer',
    'system_name',
    'system_version',
    'physmem',
    'numa_node_count',
    'disk_count',
    'ethernet_count',
    'ib_device_count',
    'ib_device',
    'ib_ca_type',
    'ib_ports',
    'gpu_device_count',
    'gpu_device_manufacturer',
    'gpu_device_name',
    'record_time_ts'
]

class PcpArchiveHardwareProcessor(object):
    """ Parses a pcp archive and adds the archive information to the index """

    @staticmethod
    def getDataFromArchive(archive, host_from_path=None):
        """ Open the pcp archive and get hardware data
            @return a dictionary containing the data,
            or None if the processor encounters an error
        """
        context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, archive)
        mdata = context.pmGetArchiveLabel()
        hostname = mdata.hostname.split('.')[0]
        record_time_ts = float(mdata.start)

        pmfg = pmapi.fetchgroup(c_pmapi.PM_CONTEXT_ARCHIVE, archive)

        """
            'alias': {
                'name': 'the name of the metric in the pcp archive',
                'type': 'item' for single items, 'indom' for instance domains
                'extractor': a function which returns the desired data from the fetched object (optional)
                'default': the value returned if the metric is not found in the archive (optional - None by default)
            }
        """
        metrics = {
            'core_count': {
                'name': 'hinv.ncpu',
                'type': 'item',
            },
            'disk_count': {
                'name': 'hinv.ndisk',
                'type': 'item',
                'default': 0,
            },
            'physmem': {
                'name': 'hinv.physmem',
                'type': 'item',
            },
            'manufacturer': {
                'name': 'hinv.cpu.vendor',
                'type': 'indom',
                'extractor': PcpArchiveHardwareProcessor.extractFirstValue,
            },
            'model_name': {
                'name': 'hinv.cpu.model_name',
                'type': 'indom',
                'extractor': PcpArchiveHardwareProcessor.extractFirstValue,
            },
            'numa_node_count': {
                'name': 'hinv.map.cpu_node',
                'type': 'indom',
                'extractor': ( lambda x: max([n[2]() for n in x]) + 1 ),
            },
            'ib_type': {
                'name': 'infiniband.hca.type',
                'type': 'indom',
                'extractor': PcpArchiveHardwareProcessor.extractNamedData,
            },
            'ib_ca_type': {
                'name': 'infiniband.hca.ca_type',
                'type': 'indom',
                'extractor': PcpArchiveHardwareProcessor.extractFirstValue,
            },
            'ib_ports': {
                'name': 'infiniband.hca.numports',
                'type': 'indom',
                'extractor': PcpArchiveHardwareProcessor.extractFirstValue,
            },
            'ethernet_count': {
                'name': 'network.interface.in.bytes',
                'type': 'indom',
                'extractor': ( lambda x: len([device[1] for device in x if device[1] != 'lo']) ),
                'default': 0,
            },
            'gpu': {
                'name': 'nvidia.cardname',
                'type': 'indom',
                'extractor': PcpArchiveHardwareProcessor.extractNamedData,
            },
        }

        # ExtObjs maps metrics to PCP extend objects
        # Metrics map to None if the metric does not appear in the archive
        extObj = {}
        data = {}
        data['infiniband'] = defaultdict(list)  # TODO change this
        for metric in metrics:
            try:
                metricType = metrics[metric]['type']
                metricName = metrics[metric]['name']
                if metricType == 'item':
                    extObj[metric] = pmfg.extend_item(metricName)
                elif metricType == 'indom':
                    extObj[metric] = pmfg.extend_indom(metricName)
            except pmapi.pmErr as exc:
                # If the metric doesn't appear in the archive
                if exc.message().startswith('Unknown metric name'):
                    extObj[metric] = None
                    data[metric] = None
                    ##########
                    expected = [
                        'gpu',
                        'model_name',
                        'ib_type',
                        'ib_ca_type',
                        'ib_ports',
                    ]
                    if metric not in expected:
                        print("Metric '%s' with name '%s' not found in archive '%s'" % (metric, metricName, archive))
                    ##########
                else:
                    traceback.print_exception(exc)
                    print('ERROR: pmfg.extend_item or pmfd.extend_indom threw an unexpected exception')
                    sys.exit(1)

        # fetch data until all metrics have been retrieved, or the end of the archive is reached
        while not (all(metric in data for metric in metrics)):
            try:
                pmfg.fetch()
            except pmapi.pmErr as exc:
                if exc.message().startswith('End of PCP archive log'):
                    # End of archive - fill in missing data with default value
                    for metric in [m for m in metrics if m not in data]:
                        if 'default' in metrics[metric]:
                            data[metric] = metrics[metric]['default']
                        else:
                            data[metric] = None
                    break
                else:
                    traceback.print_exception(exc)
                    print('ERROR: pmfg.fetch() threw an unexpected exception')
                    sys.exit(1)

            for metric in metrics:
                metricType = metrics[metric]['type']
                if ((metricType == 'item' and metric not in data) or                                     # item case
                        (metricType == 'indom' and metric not in data and len(extObj[metric]()) > 0)):    # indom case (list)

                    # Extract the data needed
                    fetchedData = extObj[metric]()
                    if 'extractor' in metrics[metric]:
                        data[metric] = metrics[metric]['extractor'](fetchedData)
                    else:
                        data[metric] = fetchedData
                    
                    # Transform the infiniband data
                    # if metric == 'ina' or metric == 'inb' or metric == 'inc':
                    #     for _, iname, value in fetchedData:
                    #         data['infiniband'][iname].append(value())

        data['record_time_ts'] = record_time_ts
        data['hostname'] = hostname
        
        return data

    @staticmethod
    def isJobArchive(archive):
        fname = os.path.basename(archive)
        return fname.startswith('job-')
    
    @staticmethod
    def extractNamedData(fetchedData):
        """ Used to extract data from instance domains in which 
            the name is important (such as gpu or infiniband data)
            by returning a dictionary mapping names to values
        """
        result = {}
        for _, iname, value in fetchedData:
            result[iname] = value()
        return result
    
    @staticmethod
    def extractFirstValue(fetchedData):
        """ Used to extract only the first value from an instance domain
            which may contain multiple values
        """
        return fetchedData[0][2]()

class HardwareStagingTransformer(object):
    """ Transforms the raw data from the archive into a list
        representing rows in the hardware staging table
    """

    def __init__(self, archiveData, replacementPath=None, outputFilename='hardware_staging.json'):
        """ Run the transformation

        Parameters:
        list archiveData: the raw data from the archive
        replacement: the path to the replacement dictionary
        outputFilename: the name/path of the json output file
        """
        
        self.result = [ STAGING_COLUMNS ]

        # Transform the archive data
        for hw_info in archiveData:

            # Get the name of the first infiniband device
            if hw_info.get('ib_type'):
                for deviceName in hw_info['ib_type']:
                    hw_info['ib_device'] = deviceName
                    break

            if (hw_info.get('gpu')) and ('gpu0' in hw_info['gpu']):
                devices = list(hw_info['gpu'])
                hw_info['gpu_device_count'] = len(devices)
                hw_info['gpu_device_manufacturer'] = 'NA'
                hw_info['gpu_device_name'] = hw_info['gpu']['gpu0']
            elif 'gpu_device_count' not in hw_info:
                hw_info['gpu_device_count'] = 0

            self.result.append([                                # Column in staging table:
                hw_info['hostname'],                                # hostname
                self.get(hw_info.get('manufacturer')),              # manufacturer
                self.get(hw_info.get('codename')),                  # codename
                self.get(hw_info.get('model_name')),                # model_name
                'NA',                                               # clock_speed
                self.get(hw_info.get('core_count'), 'int'),         # core_count
                'NA',                                               # board_manufacturer 
                'NA',                                               # board_name
                'NA',                                               # board_version
                self.get(hw_info.get('system_manufacturer')),       # system_manufacturer
                self.get(hw_info.get('system_name')),               # system_name
                'NA',                                               # system_version
                self.get(hw_info.get('physmem'), 'int'),            # physmem
                self.get(hw_info.get('numa_node_count'), 'int'),    # numa_node_count
                self.get(hw_info.get('disk_count'), 'int'),         # disk_count
                self.get(hw_info.get('ethernet_count'), 'int'),     # ethernet_count
                1 if ('ib_device' in hw_info) else 0,               # ib_device_count
                self.get(hw_info.get('ib_device')),                 # ib_device
                self.get(hw_info.get('ib_ca_type')),                # ib_ca_type
                self.get(hw_info.get('ib_ports'), 'int'),           # ib_ports
                hw_info['gpu_device_count'],                        # gpu_device_count
                self.get(hw_info.get('gpu_device_manufacturer')),   # gpu_device_manufacturer
                self.get(hw_info.get('gpu_device_name')),           # gpu_device_name
                self.get(hw_info.get('record_time_ts')),            # record_time_ts
                self.get(hw_info.get('resource_name')),             # resource_name
            ])

        # Generate replacement rules from file
        if replacementPath == None:
            replacementPath = self.autoDetectReplacementPath()

        if (replacementPath is not None) and os.path.isdir(replacementPath):
            replacementFile = os.path.join(replacementPath, 'replacement_rules.json')
            try:
                with open(replacementFile, 'r') as inFile:
                    self.replacementRules = json.load(inFile)
                self.doReplacement()
            except IOError as e:
                pass
        ##########
        else:
            print('No replacement_rules.json file found. Replacement will not be performed')
        ##########

        with open(outputFilename, 'w') as outFile:
            outFile.write(json.dumps(self.result, indent=4, separators=(',', ': ')))
    
    @staticmethod
    def autoDetectReplacementPath():
        searchpaths = [
            os.path.dirname(os.path.abspath(__file__)) + '/../../../../etc/supremm',
            '/etc/supremm',
            pkg_resources.resource_filename(pkg_resources.Requirement.parse('supremm'), 'etc/supremm')
        ]

        for path in searchpaths:
            if os.path.exists(os.path.join(path, 'replacement_rules.json')):
                return os.path.abspath(path)

        return None

    @staticmethod
    def get(value, typehint='str'):
        if (value != None) and (value != ''):
            return value
        if typehint == 'str':
            return 'NA'
        else:
            return -1
    
    def doReplacement(self):
        print('doing replacement')

        # Build a dictionary mapping column names to index
        columnToIndex = {}
        for i in range(len(self.result[0])):
            columnToIndex[self.result[0][i]] = i

        for row in self.result[1:]:
            for rule in self.replacementRules:
                # Check if conditions are true
                conditionsMet = True
                if 'conditions' in rule:
                    for condition in rule['conditions']:
                        assert 'column' in condition, 'Conditions must contain a "column" entry'
                        value = row[columnToIndex[condition['column']]]
                        reverse = ('reverse' in condition) and (condition['reverse']) # If 'reverse' is true, then the condition must be FALSE to replace
                        # Case one: equality condition
                        if 'equals' in condition:
                            if (condition['equals'] != value) != reverse:
                                conditionsMet = False
                                break
                        # Case two: contains condition
                        else:
                            assert 'contains' in condition, 'Conditions must contain either an "equals" or a "contains" property'
                            if (search(condition['contains'], value) == None) != reverse:
                                conditionsMet = False
                                break
                # Process replacements
                if conditionsMet:
                    assert 'replacements' in rule, "Rules must contain a 'replacements' entry"
                    for replacement in rule['replacements']:
                        assert 'column' in replacement, "Replacements must contain a 'column' entry"
                        assert 'repl' in replacement, "Replacements must contain a 'repl' entry"
                        index = columnToIndex[replacement['column']]
                        # Case one: regex pattern replacement
                        if 'pattern' in replacement:
                            row[index] = sub(replacement['pattern'], replacement['repl'], row[index])
                        # Case two: replace whole value
                        else:
                            row[index] = replacement['repl']

def getOptions():
    """ process comandline options """
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', help='Specify the path to the configuration directory')

    parser.add_argument('-r', '--replace', help='Specify the path to the replacement_rules directory (if none, check config dir)')

    parser.add_argument('-o', '--output', default='hardware_staging.json', help='Specify the name and path of the output json file')

    parser.add_argument(
        '-m', '--mindate', metavar='DATE', type=parsetime, default=datetime.now() - timedelta(days=DAY_DELTA),
        help='Specify the minimum datestamp of archives to process (default {} days ago)'.format(DAY_DELTA)
    )

    parser.add_argument(
        '-M', '--maxdate', metavar='DATE', type=parsetime, default=datetime.now() - timedelta(minutes=10),
        help='Specify the maximum datestamp of archives to process (default now())'
    )

    parser.add_argument('-a', '--all', action='store_true', help='Process all archives regardless of age')

    grp = parser.add_mutually_exclusive_group()
    grp.add_argument('-d', '--debug', dest='log', action='store_const', const=logging.DEBUG, default=logging.INFO,
                     help='Set log level to debug')
    grp.add_argument('-q', '--quiet', dest='log', action='store_const', const=logging.ERROR,
                     help='Only log errors')

    parser.add_argument(
        '-D', '--debugfile',
        help="""
        Specify the path to a log file. If this option is present the process will log a DEBUG level to this file.
        This logging is independent of the console log.
        """
    )

    args = parser.parse_args()
    return vars(args)

def main():
    """Main entry point"""
    opts = getOptions()
    config = Config(opts['config'])
    outputFilename = opts['output']

    ##########
    if opts['config'] != None:
        print('CONFIG_DIR = ' + os.path.abspath(opts['config']))
        print('MIN_DATE = ' + str(opts['mindate']))
    ##########

    data = []

    # TODO: Multiple resources?
    for resourcename, resource in config.resourceconfigs():

        ##########
        print('\nresource name = ' + resourcename)
        print('LOG_DIR = ' + resource['pcp_log_dir'])
        ##########
        count = 0

        afind = PcpArchiveFinder(opts['mindate'], opts['maxdate'], opts['all'])
        try:
            for archive, fast_index, hostname in afind.find(resource['pcp_log_dir']):
                if not PcpArchiveHardwareProcessor.isJobArchive(archive):
                    hw_info = PcpArchiveHardwareProcessor.getDataFromArchive(archive)
                    if hw_info != None:
                        hw_info['resource_name'] = resourcename
                        data.append(hw_info)
                ##########
                    count += 1
                    if count % 100 == 0:
                        print('count = ' + str(count))
                else:
                    print('Job archive skipped')
                ##########
        except KeyboardInterrupt as i:
            print('\nKeyboardInterrupt detected, transforming data for %s archives and writing to output...' % (count))
            pass

    HardwareStagingTransformer(data, replacementPath=opts['replace'], outputFilename=outputFilename)

    # with open(outputFilename, "w") as outFile:
    #     outFile.write(json.dumps(data, indent=4, separators=(',', ': ')))

if __name__ == '__main__':
    main()
