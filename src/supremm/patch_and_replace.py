#!/usr/bin/env python
""" Script that patches missing data in a staging table
    and runs replacement on the data

    @author Max Dudek <maxdudek@gmail.com>
"""

import json
import os
import sys
import logging
import argparse
from re import sub, search
from supremm.scripthelpers import setuplogger

# TODO: Remove
from math import ceil

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
    'record_time_ts',
    'resource_name',
]

# Build a dictionary mapping column names to index
columnToIndex = {}
for i in range(len(STAGING_COLUMNS)):
    columnToIndex[STAGING_COLUMNS[i]] = i

def loadJson(filename):
    with open(filename, 'r') as inFile:
        return json.load(inFile)

class StagingPatcher(object):

    def __init__(self, stagingData, maxdays=10, mode='gpu'):

        modes = {
            'gpu': {
                'indicator_column': 'gpu_device_count',                         # The column used to test if data is missing
                'indicator_value': 0,                                           # The value the indicator column takes if data is missing
                'columns_to_patch': ['gpu_device_count', 'gpu_device_name'],    # The list of columns which need to be patched
            },
            'ib': {
                'indicator_column': 'ib_device_count',
                'indicator_value': 0,
                'columns_to_patch': ['ib_device_count', 'ib_device', 'ib_ca_type', 'ib_ports'],
            },
        }

        if mode not in modes:
            logging.error('The patching mode "%s" is not a valid mode', mode)
            return
        
        logging.info('Patching data using mode "%s"', mode)

        settings = modes[mode]
        self.indicatorColumn = settings['indicator_column']
        self.indicatorValue = settings['indicator_value']
        self.columnsToPatch = settings['columns_to_patch']

        self.indexsToPatch = [columnToIndex[c] for c in self.columnsToPatch]
        self.indicatorIndex = columnToIndex[self.indicatorColumn]

        self.stagingData = stagingData
        self.maxdays = maxdays
    
        self.patch()

    def patch(self):

        # Sort the data by hostname, then by timestamp
        self.stagingData.sort(key=lambda x: (x[columnToIndex['hostname']], x[columnToIndex['record_time_ts']]))

        # Initialize state variables
        self.lastIndex = None       # The last index of the current hostname to have complete data (None if not yet seen)
        self.currentHostname = None # The current hostname in the iteration

        self.hostnameIndex = columnToIndex['hostname']
        self.timestampIndex = columnToIndex['record_time_ts']

        for i in range(len(self.stagingData)):
            self.currentIndex = i

            if self.patchingShouldOccur():
                self.fillInMissingData()
            self.resetState()

    def patchingShouldOccur(self):
        """ Returns True if patching needs to occur at the current index """
        index = self.currentIndex
        row = self.stagingData[index]

        # Check hostname (make sure it is the same as the current hostname)
        if self.hostnameChanged():
            return False

        # Check if currently tracking
        if self.lastIndex == None:
            return False
        
        # Check if data is missing (can't patch if it still is)
        if self.isMissingData():
            return False
        
        # Check if it's been too long since the data was last there
        if self.maxTimeExceeded():
            return False
        
        # Check if there's actually a gap of 2 or greater
        if (index - self.lastIndex <= 1):
            return False
        
        return True
        
    def hostnameChanged(self):
        """ Returns True if the hostname is different than the last row """
        row = self.stagingData[self.currentIndex]
        return row[self.hostnameIndex] != self.currentHostname

    def maxTimeExceeded(self):
        """ Returns true if the time difference between the current row and the row specified by lastIndex
        (i.e., the last row to contain data) is greater that the max gap in data allowed (specified by self.maxdays)"""

        row = self.stagingData[self.currentIndex]

        SECONDS_PER_DAY = 86400
        maxSeconds = self.maxdays * SECONDS_PER_DAY
        timeGap = row[self.timestampIndex] - self.stagingData[self.lastIndex][self.timestampIndex]
        return timeGap > maxSeconds

    def isMissingData(self):
        """ Returns true if the row is missing data, false if data is intact """
        row = self.stagingData[self.currentIndex]
        return row[self.indicatorIndex] == self.indicatorValue

    def fillInMissingData(self):
        index = self.currentIndex
        correctRow = self.stagingData[self.lastIndex] # The last row to have full data

        # Missing data rows
        for j in range(self.lastIndex+1, index):
            row = self.stagingData[j]
            # Missing data columns
            for c in self.indexsToPatch:
                row[c] = correctRow[c]

        # Debug message
        numberOfRows = index - self.lastIndex - 1
        if numberOfRows > 1:
            logging.debug('Patched %d rows', numberOfRows)

    def resetState(self):
        """ Resets the iteration state based on data at index """
        row = self.stagingData[self.currentIndex]
        
        if self.isMissingData():
            if self.hostnameChanged():
                # Reset if the hostname changed, otherwise stay the same
                self.lastIndex = None
        else:
            self.lastIndex = self.currentIndex

        self.currentHostname = row[self.hostnameIndex]

def patchByColumn(stagingData, columnsToPatch, indicatorColumn):
    """ Patch the data for a list of columns
        stagingData: the list of staging rows
        columnsToPatch: the list of columns to patch missing data for
        indicatorColumn: the column which equals 0 when data is missing

        Assumes that the stagingData list has at least 3 rows
    """

    # Store indexes of columns for faster retrieval of data
    indexsToPatch = [columnToIndex[c] for c in columnsToPatch]
    indicatorIndex = columnToIndex[indicatorColumn]
    hostnameIndex = columnToIndex['hostname']

    # #TODO Patch first row 
    firstRow = stagingData[0]
    secondRow = stagingData[1]

    if (firstRow[indicatorIndex] == 0 and secondRow[indicatorIndex] != 0 and firstRow[hostnameIndex] == secondRow[hostnameIndex]):
        for index in indexsToPatch:
            firstRow[index] = secondRow[index]

    # Patch last row
    lastRow = stagingData[-1]
    penultimateRow = stagingData[-2]

    if (lastRow[indicatorIndex] == 0 and penultimateRow[indicatorIndex] != 0 and lastRow[hostnameIndex] == penultimateRow[hostnameIndex]):
        for index in indexsToPatch:
            lastRow[index] = penultimateRow[index]

    # Patch middle data
    for i in range(1, len(stagingData)-1):
        previousRow = stagingData[i-1]
        currentRow = stagingData[i]
        nextRow = stagingData[i+1]

        # Patch data
        if (previousRow[indicatorIndex] != 0 and currentRow[indicatorIndex] == 0 and rowsAreEqual(previousRow, nextRow)):
            # Patch missing data into current row using data from previous row
            for index in indexsToPatch:
                currentRow[index] = previousRow[index]
    
    return stagingData

def rowsAreEqual(row1, row2):
    """ Returns true if the first row is equal to the second row, EXCEPT the record_time_ts column
        (helper function for patch())
    """
    columnsToCheck = [x for x in STAGING_COLUMNS if x != 'record_time_ts']

    for c in columnsToCheck:
        index = columnToIndex[c]
        if row1[index] != row2[index]:
            return False
    
    return True

def replaceData(stagingData, replacementPath):
    # Look for replacement file
    if (replacementPath is not None) and os.path.isfile(os.path.join(replacementPath, 'replacement_rules.json')):
        replacementFile = os.path.join(replacementPath, 'replacement_rules.json')
        replacementRules = loadJson(replacementFile)
    else:
        logging.info('No replacement_rules.json file found. Replacement will not be applied to staging columns.')
        return stagingData
    
    logging.info('Applying replacement rules to staging columns using file %s...', replacementFile)

    for row in stagingData:
        for rule in replacementRules:
            # Check if conditions are true
            conditionsMet = True
            if 'conditions' in rule:
                for condition in rule['conditions']:
                    assert 'column' in condition, 'Conditions must contain a "column" entry'
                    value = row[columnToIndex[condition['column']]]
                    reverse = condition.get('reverse', False) # If 'reverse' is true, then the condition must be FALSE to replace
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

    return stagingData

def getOptions():
    """ process comandline options """
    parser = argparse.ArgumentParser()

    parser.add_argument("-i", "--input", help="The path to the input json file", required=True)

    parser.add_argument('-o', '--output', default='hardware_staging.json', help='Specify the name and path of the output json file')

    parser.add_argument("-p", "--patch", action="store_true", help="Patch the data")

    parser.add_argument("-r", "--replace", help="Specify the path to the repalcement rules directory (if replacement should occur)")

    grp = parser.add_mutually_exclusive_group()
    grp.add_argument('-d', '--debug', dest='log', action='store_const', const=logging.DEBUG, default=logging.INFO,
                    help='Set log level to debug')
    grp.add_argument('-q', '--quiet', dest='log', action='store_const', const=logging.ERROR,
                    help='Only log errors')

    args = parser.parse_args()

    if not (args.replace or args.patch):
        parser.error('No action requested, add --replace or --patch')

    return vars(args)

def main():
    """ Main entry point """
    opts = getOptions()

    setuplogger(opts['log'])

    inputFile = os.path.abspath(opts['input'])

    if os.path.isfile(inputFile):
        stagingData = loadJson(inputFile)[1:]   # Remove header from input
    else:
        logging.error("Can't find input file %s", inputFile)
        sys.exit(1)

    if opts['patch']:
        # stagingData = patchData(stagingData)

        # TODO: Get rid of this (adjust the memory)
        for i in range(len(stagingData)):
            row = stagingData[i]
            mem = columnToIndex['physmem']
            if row[mem] > 4000:
                row[mem] = int(ceil(row[mem] / 1024.0 / 2.0) * 2)
            if row[mem] % 2 != 0:
                row[mem] = int(ceil(row[mem] / 2.0) * 2)

        stagingData = StagingPatcher(stagingData, mode='gpu').stagingData
        stagingData = StagingPatcher(stagingData, mode='ib').stagingData
    
    if opts['replace'] != None:
        stagingData = replaceData(stagingData, opts['replace'])

    stagingData.insert(0, STAGING_COLUMNS)   # Add header row back to result

    logging.info('Writing result to %s', opts['output'])    

    # Output staging rows to file
    with open(opts['output'], 'w') as outFile:
        outFile.write(json.dumps(stagingData, indent=4, separators=(',', ': ')))

if __name__ == '__main__':
    main()
