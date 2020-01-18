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
import time
from copy import deepcopy
import sys
import traceback
from math import ceil

import json
from collections import defaultdict

from pcp import pmapi
import cpmapi as c_pmapi

from supremm.config import Config
from supremm.scripthelpers import parsetime, setuplogger
from supremm.indexarchives import PcpArchiveFinder

DAY_DELTA = 3

# Option flags
keepAll = False
doPatching = False
doReplacement = False

def getStagingColumns():
    result = [
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
        'record_time_ts',
        'resource_name',
    ]
    return result

STAGING_COLUMNS = getStagingColumns()

# Initialize counting variables
countArchivesFound = 0      # Total number of archives found in log dir
countArchivesRead = 0       # Number of archives read (non-job archives)
countJobArchives = 0        # Number of job archives skipped
countFinishedArchives = 0   # Number of archives which reached the end before data could be pulled out
countArchivesFailed = 0     # Number of archives which could not be read because of an error

errorCount = {}

class PcpArchiveHardwareProcessor(object):
    """ Parses a pcp archive and adds the archive information to the index """

    @staticmethod
    def getDataFromArchive(archive, host_from_path=None):
        """ Open the pcp archive and get hardware data
            @return a dictionary containing the data,
            or None if the processor encounters an error
        """
        global countFinishedArchives
        global keepAll
        
        """
            The METRICS dictionary defines the metrics to be collected, as well as how to extract them
            'alias': {
                'name': 'the name of the metric in the pcp archive',
                'type': 'item' for single items, 'indom' for instance domains
                'extractor': a function which returns the desired data from the fetched instance domain (optional)
                    - not needed for items (items only contain one value)
                    - default for instance domains is PcpArchiveHardwareProcessor.extractFirstValue
                'always_expected': (optional)
                    - if true: if an archive is missing this metric, send a debug message and omit it from the results
            }
        """
        DEFAULT_EXTRACTOR = PcpArchiveHardwareProcessor.extractFirstValue
        METRICS = {
            'core_count': {
                'name': 'hinv.ncpu',
                'type': 'item',
                'always_expected': True,
            },
            'disk_count': {
                'name': 'hinv.ndisk',
                'type': 'item',
                'always_expected': True,
            },
            'physmem': {
                'name': 'hinv.physmem',
                'type': 'item',
                'always_expected': True,
            },
            'manufacturer': {
                'name': 'hinv.cpu.vendor',
                'type': 'indom',
                'always_expected': True,
            },
            'board_manufacturer': {
                'name': 'hinv.dmi.board_vendor',
                'type': 'item',
            },
            'board_name': {
                'name': 'hinv.dmi.board_name',
                'type': 'item',
            },
            'board_version': {
                'name': 'hinv.dmi.board_version',
                'type': 'item',
            },
            'system_manufacturer': {
                'name': 'hinv.dmi.sys_vendor',
                'type': 'item',
            },
            'system_name': {
                'name': 'hinv.dmi.product_name',
                'type': 'item',
            },
            'system_version': {
                'name': 'hinv.dmi.product_version',
                'type': 'item',
            },
            'model_name': {
                'name': 'hinv.cpu.model_name',
                'type': 'indom',
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
            },
            'ib_ports': {
                'name': 'infiniband.hca.numports',
                'type': 'indom',
            },
            'ethernet_count': {
                'name': 'network.interface.in.bytes',
                'type': 'indom',
                'always_expected': True,
                'extractor': ( lambda x: len([device[1] for device in x if device[1] != 'lo']) ),
            },
            'gpu': {
                'name': 'nvidia.cardname',
                'type': 'indom',
                'extractor': PcpArchiveHardwareProcessor.extractNamedData,
            },
        }
        try:
            context = pmapi.pmContext(c_pmapi.PM_CONTEXT_ARCHIVE, archive)
        except pmapi.pmErr as exc:
            #pylint: disable=not-callable
            logging.debug('Context error\n\tarchive: %s\n\terror: "%s" (errno = %d)', archive, exc.message().split(' [')[0], exc.errno)
            countError(exc)
            return None

        mdata = context.pmGetArchiveLabel()
        hostname = mdata.hostname.split('.')[0]
        record_time_ts = float(mdata.start)

        pmfg = pmapi.fetchgroup(c_pmapi.PM_CONTEXT_ARCHIVE, archive)

        # ExtObjs maps metrics to PCP extend objects
        # Metrics map to None if the metric does not appear in the archive
        extObj = {}
        data = {}
        for metric in METRICS:
            try:
                metricType = METRICS[metric]['type']
                metricName = METRICS[metric]['name']
                if metricType == 'item':
                    extObj[metric] = pmfg.extend_item(metricName)
                elif metricType == 'indom':
                    extObj[metric] = pmfg.extend_indom(metricName)
            except pmapi.pmErr as exc:
                # If the metric doesn't appear in the archive
                if exc.errno == -12357:    # Unknown metric
                    extObj[metric] = None
                    data[metric] = None
                    # If the metric that is missing should ALWAYS be in the archive, then return None
                    # because this archive must be corrupted
                    if METRICS[metric].get('always_expected', False):
                        logging.debug("Metric '%s' with PCP name '%s' not found in archive '%s'", metric, metricName, archive)
                        countError(exc)
                        return None
                else:
                    handleUnexpectedException(exc, archive)
                    return None

        # fetch data until all METRICS have been retrieved, or the end of the archive is reached
        fetchCount = 0
        while not (all(metric in data for metric in METRICS)):

            # Check if the process is still waiting on gpu data
            if fetchCount > 0 and [m for m in METRICS if m not in data] == ['gpu']:
                # If the gpu data has not been found after one fetch
                if keepAll:
                    # If we want to still keep this archive even though data is missing
                    data['gpu'] = None
                    break
                else:
                    # If we want to ignore this archive because data is missing
                    logging.debug('Missing gpu data in archive %s', archive)
                    countFinishedArchives += 1
                    return None

            try:
                pmfg.fetch()
                fetchCount += 1
            except pmapi.pmErr as exc:
                if exc.errno == -12370:    # End of PCP archive log
                    # If we reach the end of the archive, it means relevant data is missing
                    # meaning the archive is corrupted, so return None
                    countFinishedArchives += 1
                    logging.debug('Processor reached the end of archive\n\tarchive: %s\n\tMissing metric(s): %s\n\tfetch count: %d', archive, str([m for m in METRICS if m not in data]), fetchCount)
                elif exc.errno == -12373:    # Corrupted record in a PCP archive log
                    logging.debug('Corrupted record in archive %s', archive)
                    countError(exc)
                else:
                    handleUnexpectedException(exc, archive)
                return None

            # Extract the data needed from metrics which have not yet been fetched
            for metric in [m for m in METRICS if m not in data]:
                try:
                    metricType = METRICS[metric]['type']
                    if ((metricType == 'item') or                                     # item case
                            (metricType == 'indom' and len(extObj[metric]()) > 0)):   # indom case (list)
                        fetchedData = extObj[metric]()
                        if metricType == 'indom':
                            if 'extractor' in METRICS[metric]:
                                data[metric] = METRICS[metric]['extractor'](fetchedData)
                            else:
                                data[metric] = DEFAULT_EXTRACTOR(fetchedData)
                        elif metricType == 'item':
                            data[metric] = fetchedData
                except pmapi.pmErr as exc:
                    #pylint: disable=not-callable
                    logging.debug('Extraction error\n\tarchive: %s\n\ttimestamp: %f\n\tmetric: %s\n\terror: "%s" (errno = %d)', archive, record_time_ts, metric, exc.message(), exc.errno)
                    countError(exc)
                    return None

        data['record_time_ts'] = record_time_ts
        data['hostname'] = hostname
        
        return data

    @staticmethod
    def isJobArchive(archive):
        fname = os.path.basename(archive)
        return fname.startswith('job-')

    # Extractor methods
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
        from patch_and_replace import StagingPatcher, replaceData
        # Option flags
        global doPatching
        global doReplacement
        
        self.result = []

        # Transform the archive data
        for hw_info in archiveData:

            # Get the name of the first infiniband device
            if hw_info.get('ib_type'):
                for deviceName in hw_info['ib_type']:
                    hw_info['ib_device'] = deviceName
                    break

            # Get GPU data
            if (hw_info.get('gpu')) and ('gpu0' in hw_info['gpu']):
                devices = list(hw_info['gpu'])
                hw_info['gpu_device_count'] = len(devices)
                hw_info['gpu_device_manufacturer'] = 'NA'
                hw_info['gpu_device_name'] = hw_info['gpu']['gpu0']
            elif 'gpu_device_count' not in hw_info:
                hw_info['gpu_device_count'] = 0

            # Get model_name and clock_speed
            clock_speed = None
            model_name = None
            if hw_info.get('model_name'):
                processor_info = hw_info['model_name'].split(' @ ')
                model_name = processor_info[0]
                if (len(processor_info) > 1):
                    clock_speed = processor_info[1]

            # Convert MB to GB, round up to nearest even GB
            if hw_info.get('physmem'):
                hw_info['physmem'] = int(ceil(hw_info['physmem'] / 1024.0 / 2.0) * 2)

            self.result.append([                                # Column in staging table:
                hw_info['hostname'],                                # hostname
                self.get(hw_info.get('manufacturer')),              # manufacturer
                self.get(hw_info.get('codename')),                  # codename
                self.get(model_name),                               # model_name
                self.get(clock_speed, 'int'),                       # clock_speed
                self.get(hw_info.get('core_count'), 'int'),         # core_count
                self.get(hw_info.get('board_manufacturer')),        # board_manufacturer 
                self.get(hw_info.get('board_name')),                # board_name
                self.get(hw_info.get('board_version')),             # board_version
                self.get(hw_info.get('system_manufacturer')),       # system_manufacturer
                self.get(hw_info.get('system_name')),               # system_name
                self.get(hw_info.get('system_version')),            # system_version
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

        # Patch gpu data and ib data into archives which are missing it
        if doPatching:
            self.result = StagingPatcher(self.result, mode='gpu').stagingData
            self.result = StagingPatcher(self.result, mode='ib').stagingData

        # Do replacement
        if doReplacement:
            self.result = replaceData(self.result, replacementPath)

        logging.debug('Writing staging table columns to %s', os.path.abspath(outputFilename))

        self.result.insert(0, STAGING_COLUMNS)   # Add header row to result

        # Output staging rows to file
        with open(outputFilename, 'w') as outFile:
            outFile.write(json.dumps(self.result, indent=4, separators=(',', ': ')))

    @staticmethod
    def get(value, typehint='str'):
        if (value != None) and (value != ''):
            return value
        if typehint == 'str':
            return 'NA'
        else:
            return -1

def handleUnexpectedException(exc, archive, metric=None):
    """ Print an error message for an unexpected exception
        and record it
    """
    if metric:
        metricString = '\n\tmetric: ' + metric
    else:
        metricString = ''
    #pylint: disable=not-callable
    logging.warning('Unexpected exception: %s\n\tarchive: %s%s\n\terror: "%s" (errno = %d)\n%s', 
            str(exc), archive, metricString, exc.message(), exc.errno, traceback.format_exc())
    countError(exc)

def countError(exc):
    """ Record an error in the errorCount dictionary
    """

    # Clean up the error message to use it as a key
    #pylint: disable=not-callable
    message = exc.message()
    message = message.split(' [')[0]
    message = message.split(' <')[0]

    key = '%s (errno = %d)' % (message, exc.errno)
    if key in errorCount:
        errorCount[key] += 1
    else:
        errorCount[key] = 1

def getOptions():
    """ process comandline options """
    parser = argparse.ArgumentParser()

    parser.add_argument('-c', '--config', help='Specify the path to the configuration directory')

    parser.add_argument(
        "-r", "--resource", metavar="RES",
        help="Process only archive files for the specified resource, if absent then all resources are processed"
    )

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

    parser.add_argument('-k', '--keep', action='store_true', help="Keep archives which are missing gpu data (fill in data with 'NA')")

    parser.add_argument("-P", "--patch", action="store_true", help="Patch the data using patch_and_replace.py before outputting")

    parser.add_argument("-R", "--replace", action="store_true", help="Run replacement on the data using a repalcement_rules.json file in the config directory")

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
    # Variables used to count the archives read
    global countArchivesFound
    global countArchivesRead
    global countFinishedArchives
    global countJobArchives
    global countArchivesFailed
    # Option flags
    global keepAll
    global doPatching
    global doReplacement

    opts = getOptions()
    
    setuplogger(opts['log'], opts['debugfile'], filelevel=logging.DEBUG)
    logging.debug('Command: %s', ' '.join(sys.argv))

    config = Config(opts['config'])
    keepAll = opts['keep']
    doPatching = opts['patch']
    doReplacement = opts['replace']

    numberOfResources = len(config._config['resources'])
    resourceNum = 1
    data = []

    for resourcename, resource in config.resourceconfigs():

        if opts['resource'] in (None, resourcename, str(resource['resource_id'])):

            logging.info('Processing resource %d of %d', resourceNum, numberOfResources)
            resourceNum += 1
            logging.info('Resource name = %s', resourcename)
            log_dir = resource['pcp_log_dir']
            if log_dir == '':
                logging.info('No log diretcory specified for resource %s. Skipping...', resourcename)
                continue
            else:
                logging.info('Log directory = %s', log_dir)
            

            afind = PcpArchiveFinder(opts['mindate'], opts['maxdate'], opts['all'])

            # Search for and process archives in this resource
            startTime = time.time()
            try:
                for archive, fast_index, hostname in afind.find(log_dir):
                    if not PcpArchiveHardwareProcessor.isJobArchive(archive):
                        # Try to extract information from the archive
                        try:
                            hw_info = PcpArchiveHardwareProcessor.getDataFromArchive(archive)
                        except pmapi.pmErr as exc:
                            handleUnexpectedException(exc, archive)
                            hw_info = None
                            countArchivesFailed += 1
                        
                        # Add the extracted information to the data list
                        if hw_info != None:
                            hw_info['resource_name'] = resourcename
                            data.append(hw_info)
                        else:
                            countArchivesFailed += 1
                        
                        countArchivesRead += 1
                        if countArchivesRead % 100 == 0:
                            logging.debug('%d archives read (cumulative rate = %f archives/second)', countArchivesRead, countArchivesRead / (time.time() - startTime))
                    else:
                        countJobArchives += 1
                    countArchivesFound += 1
            except KeyboardInterrupt as i:
                logging.info('KeyboardInterrupt detected, skipping this resource after reading %s archives...', countArchivesRead)
            except Exception as exc:
                # Ignore and record any unexpected python exceptions
                logging.error('UNEXPECTED PYTHON ERROR (%s)\n%s', str(exc), traceback.format_exc())
                countArchivesFailed += 1

            processTime = time.time() - startTime
            # Log job info
            logging.info('Processing complete for resource %s', resourcename)
            if (countArchivesFound != 0):
                logging.info('Number of archives found: %d', countArchivesFound)
                logging.info('Number of job archives skipped: %d/%d (%.1f%%)', countJobArchives, countArchivesFound, (float(countJobArchives)/countArchivesFound)*100)
                logging.info('Number of archives read: %d/%d (%.1f%%)', countArchivesRead, countArchivesFound, (float(countArchivesRead)/countArchivesFound)*100)
                logging.info('Number of archives which reached the end: %d/%d (%.1f%%)', countFinishedArchives, countArchivesRead, (float(countFinishedArchives)/countArchivesRead)*100)
                logging.info('Number of archives which failed to be read because of an error: %d/%d (%.1f%%)', countArchivesFailed, countArchivesRead, (float(countArchivesFailed)/countArchivesRead)*100)
                logging.info('Total process time: %.2f minutes (%.4f seconds/archive, %.4f archives/second)', processTime / 60, processTime / countArchivesRead, countArchivesRead / processTime)
                if errorCount != {}:
                    logging.info('Error count = \n%s', json.dumps(errorCount, indent=4))
            else:
                logging.info('No archives found for resource %s in specified date range', resourcename)
            
            # Reset count variables
            countArchivesFound = 0
            countArchivesRead = 0
            countJobArchives = 0
            countFinishedArchives = 0
            countArchivesFailed = 0
            errorCount.clear()
    
    # Transform data to staging columns
    startTime = time.time()
    HardwareStagingTransformer(data, replacementPath=config.getconfpath(), outputFilename=opts['output'])
    transformTime = time.time() - startTime
    logging.info('Total transform time: %.2f seconds', transformTime)

if __name__ == '__main__':
    main()
