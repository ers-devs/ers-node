#!/bin/bash
NRUNS=1
SCRIPT_DIR="$( cd -P "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
BASE_DIR="$SCRIPT_DIR/.."
LOG_DIR="$BASE_DIR"/test_logs/`date "+%m%d%H%M"`
LOGFILE="$LOG_DIR/log.txt"
mkdir -p "$LOG_DIR"
export PYTHONPATH="$BASE_DIR/site-packages:$PYTHONPATH"

trap 'kill $(jobs -p)' EXIT
"$BASE_DIR/scripts_run_q/trackmem.sh" "$LOG_DIR/memory.log"&

cd "$BASE_DIR"
echo "Loading Tests" > $LOGFILE
for i in `seq $NRUNS`; do
	echo "Run $i out of $NRUNS." | tee -a "$LOGFILE"
	python 2_optimized_ld-in-couch.py -i test_dataset/repo10k.nt -g repo10k_test -d "load-test-run-$i" -u admin -p admin 2>> "$LOGFILE"
done

for i in `seq $NRUNS`; do
	curl -X DELETE "http://admin:admin@localhost:5984/load-test-run-$i"
done
