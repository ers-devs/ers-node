#!/bin/bash

if [ ! $# == 2 ]; then
  echo "Start continuous replication between a local database and https://ers.iriscouch.com/ers_shared"
  echo "Usage: $0 local_db_name remote_db_password"
  exit
fi

LOCAL_DB=$1
PASS=$2

REMOTE_DB="https://shared_replicator:$PASS@ers.iriscouch.com/ers_shared"
DOC='{
   "source": "'$LOCAL_DB'",
   "target": "'$REMOTE_DB'",
   "continuous": true,
   "create_target": false
}'
curl -X PUT 'http://admin:admin@localhost:5984/_replicator/'$LOCAL_DB'2remote' -d "$DOC"

DOC='{
   "target": "'$LOCAL_DB'",
   "source": "'$REMOTE_DB'",
   "continuous": true,
   "create_target": false
}'
curl -X PUT 'http://admin:admin@localhost:5984/_replicator/remote2'$LOCAL_DB -d "$DOC"
