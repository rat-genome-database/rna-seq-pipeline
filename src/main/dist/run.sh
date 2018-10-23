#!/usr/bin/env bash
. /etc/profile

SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
APPNAME=RNASeqPipeline
APPDIR=/home/rgddata/pipelines/$APPNAME
EMAILLIST=cdursun@mcw.edu
DEVELOPER=cdursun@mcw.edu

if [ "$SERVER" == "KYLE" ]; then
  EMAILLIST=cdursun@mcw.edu,jrsmith@mcw.edu
fi

cd $APPDIR

DB_OPTS="-Dspring.config=$APPDIR/../properties/default_db.xml"
LOG4J_OPTS="-Dlog4j.configuration=file://$APPDIR/properties/log4j.properties"
export RNA_SEQ_PIPELINE_OPTS="$DB_OPTS $LOG4J_OPTS"
bin/$APPNAME "$@" 2>&1

mailx -s "[$SERVER] RNASeq Pipeline Summary" $EMAILLIST < $APPDIR/logs/summary.log

