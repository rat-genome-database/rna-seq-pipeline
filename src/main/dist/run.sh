#!/usr/bin/env bash
. /etc/profile

SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
APPNAME='rna-seq-pipeline'
APPDIR=/home/rgddata/pipelines/$APPNAME
EMAILLIST=mtutaj@mcw.edu
DEVELOPER=mtutaj@mcw.edu

if [ "$SERVER" == "REED" ]; then
  EMAILLIST=mtutaj@mcw.edu
fi

cd $APPDIR

# Dragon Toolkit's EnvVariable class runs 'env' and parses each line as KEY=VALUE.
# Multi-line exported bash functions produce continuation lines without '=',
# which throw StringIndexOutOfBoundsException. Unset all exported functions.
echo "Unsetting exported bash functions for Dragon Toolkit compatibility:"
while read -r _ _ fn; do
    if [ -n "$fn" ]; then
        echo "  unset -f $fn"
        unset -f "$fn"
    fi
done < <(declare -Fx 2>/dev/null)

java -Dspring.config=../properties/default_db2.xml \
    -Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml \
    -jar lib/${APPNAME}.jar  "$@" 2>&1 > run.log

mailx -s "[$SERVER] RNASeq Pipeline Summary" $EMAILLIST < $APPDIR/logs/summary.log

