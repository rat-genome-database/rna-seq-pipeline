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
LOG4J_OPTS="-Dlog4j.configuration=file://$APPDIR/properties/log4j.properties"
export RNA_SEQ_PIPELINE_OPTS="$DB_OPTS $LOG4J_OPTS"
bin/$APPNAME "$@" 2>&1

java -Dspring.config=$APPDIR/../properties/default_db.xml \
    -Dlog4j.configuration=file://$APPDIR/properties/log4j.properties \
    -jar lib/${APPNAME}.jar  "$@" 2>&1 > run.log

mailx -s "[$SERVER] RNASeq Pipeline Summary" $EMAILLIST < $APPDIR/logs/summary.log

