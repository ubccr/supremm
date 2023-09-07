#!/bin/bash
set -euxo pipefail
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

outputfile=`mktemp`
python3 $DIR/../../src/supremm/supremm_testharness.py -c $DIR/../../config $DIR/data/perfevent > $outputfile

# Check that there are data in the output for all the following
jq -e .cpuperf.cpiref.avg < $outputfile 
jq -e .cpuperf.flops.avg < $outputfile 
jq -e .cpuperf.cpldref.avg < $outputfile 
jq -e .uncperf.membw.avg < $outputfile
jq -e .timeseries.membw < $outputfile
jq -e .timeseries.simdins < $outputfile
jq -e .summarization.datasource < $outputfile

rm -f $outputfile
