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

def patchData(stagingData):
    """ If data (such as gpu data) is missing for one archive,
        patch this data using the next/previous archive for that host
    """
    logging.info('Patching missing gpu and ib data...')

    # Sort the data by hostname, then by timestamp
    stagingData.sort(key=lambda x: (x[columnToIndex['hostname']], x[columnToIndex['record_time_ts']]))

    gpuColumnsToPatch = ['gpu_device_count', 'gpu_device_name']
    # gpuIndexsToPatch = [columnToIndex[c] for c in gpuColumnsToPatch]
    # gpuIndex = columnToIndex['gpu_device_count']

    ibColumnsToPatch = ['ib_device_count', 'ib_device', 'ib_ca_type', 'ib_ports']
    # ibIndexsToPatch = [columnToIndex[c] for c in ibColumnsToPatch]
    # ibIndex = columnToIndex['ib_device_count']

    # TODO: Get rid of this (adjust the memory)
    for i in range(len(stagingData)):
        row = stagingData[i]
        mem = columnToIndex['physmem']
        if row[mem] > 4000:
            row[mem] = int(ceil(row[mem] / 1024.0))

    patchByColumn(stagingData, gpuColumnsToPatch, 'gpu_device_count')
    patchByColumn(stagingData, ibColumnsToPatch, 'ib_device_count')

    # for i in range(1, len(stagingData)-1):
    #     previousRow = stagingData[i-1]
    #     currentRow = stagingData[i]
    #     nextRow = stagingData[i+1]

    #     # Patch gpu data
    #     if (previousRow[gpuIndex] != 0 and currentRow[gpuIndex] == 0 and rowsAreEqual(previousRow, nextRow)):
    #         # Patch missing data into current row using data from previous row
    #         for index in gpuIndexsToPatch:
    #             stagingData[i][index] = previousRow[index]
        
    #     # Patch ib data
    #     if (previousRow[ibIndex] != 0 and currentRow[ibIndex] == 0 and rowsAreEqual(previousRow, nextRow)):
    #         # Patch missing data into current row using data from previous row
    #         for index in ibIndexsToPatch:
    #             stagingData[i][index] = previousRow[index]

    return stagingData

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

    # Patch first row
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

    args = parser.parse_args()

    if not (args.replace or args.patch):
        parser.error('No action requested, add --replace or --patch')

    return vars(args)

def main():
    """ Main entry point """
    opts = getOptions()

    setuplogger(logging.INFO)

    inputFile = os.path.abspath(opts['input'])

    if os.path.isfile(inputFile):
        stagingData = loadJson(inputFile)[1:]   # Remove header from input
    else:
        logging.error("Can't find input file %s", inputFile)
        sys.exit(1)

    if opts['patch']:
        stagingData = patchData(stagingData)
    
    if opts['replace'] != None:
        stagingData = replaceData(stagingData, opts['replace'])

    stagingData.insert(0, STAGING_COLUMNS)   # Add header row back to result

    logging.info('Writing result to %s', opts['output'])    

    # Output staging rows to file
    with open(opts['output'], 'w') as outFile:
        outFile.write(json.dumps(stagingData, indent=4, separators=(',', ': ')))

if __name__ == '__main__':
    main()
