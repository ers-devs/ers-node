#!/bin/bash
LOGFILE="$1"
SCRIPT_DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
COUCHDB_PID=`cat "$SCRIPT_DIR/../couch/couchdata/couch.pid"`
DELAY=2
echo "# Started `date`, delay $DELAY sec." > $LOGFILE
echo "# VSZ TIME CPU" >> $LOGFILE
while true; do 
     ps -o vsz= -o time= -o cpu= -p "$COUCHDB_PID" >> $LOGFILE
     sleep "$DELAY"
done
