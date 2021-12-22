#!/usr/bin/env bash
. /etc/profile

SERVER=`hostname -s | tr '[a-z]' '[A-Z]'`
APPNAME=RNASeqPipeline
APPDIR=/home/rgddata/pipelines/$APPNAME
EMAILLIST=mtutaj@mcw.edu
DEVELOPER=mtutaj@mcw.edu

if [ "$SERVER" == "REED" ]; then
  EMAILLIST=mtutaj@mcw.edu,jrsmith@mcw.edu
fi

cd $APPDIR

DB_OPTS="-Dspring.config=$APPDIR/../properties/default_db.xml"
LOG4J_OPTS="-Dlog4j.configurationFile=file://$APPDIR/properties/log4j2.xml"
export RNA_SEQ_PIPELINE_OPTS="$DB_OPTS $LOG4J_OPTS"
bin/$APPNAME "$@" 2>&1

mailx -s "[$SERVER] RNASeq Pipeline Summary" $EMAILLIST < $APPDIR/logs/summary.log

