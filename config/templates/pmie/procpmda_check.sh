#!/bin/sh

mytime=`/bin/date`
myhost=`/bin/hostname`

echo "Proc PMDA died at $mytime on $myhost" >> /var/log/pcp/pmie/procpmda.log 2>&1
sudo /etc/pcp/pmie/pcp-restart.sh >> /var/log/pcp/pmie/pcpsudo.out 2>&1
