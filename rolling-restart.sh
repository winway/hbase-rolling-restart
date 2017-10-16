#!/usr/bin/env bash
#
#/**
# * Copyright 2011 The Apache Software Foundation
# *
# * Licensed to the Apache Software Foundation (ASF) under one
# * or more contributor license agreements.  See the NOTICE file
# * distributed with this work for additional information
# * regarding copyright ownership.  The ASF licenses this file
# * to you under the Apache License, Version 2.0 (the
# * "License"); you may not use this file except in compliance
# * with the License.  You may obtain a copy of the License at
# *
# *     http://www.apache.org/licenses/LICENSE-2.0
# *
# * Unless required by applicable law or agreed to in writing, software
# * distributed under the License is distributed on an "AS IS" BASIS,
# * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# * See the License for the specific language governing permissions and
# * limitations under the License.
# */

# Move regions off a server then stop it.  Optionally restart and reload.
# Turn off the balancer before running this script.
function usage {
  echo "Usage: $0 -f <filename> -n <parallelism>"
  echo " f      File contain regionservers, one regionserver per line"
  echo " n      Restart n regionserver(s) parallelly each round"
  exit 1
}

if [ $# -lt 1 ]; then
  usage
fi

bin=`dirname "$0"`
bin=`cd "$bin">/dev/null; pwd`
# This will set HBASE_HOME, etc.
. "$bin"/hbase-config.sh
# Get arguments
rsfile=
parallelism=1
while [ $# -gt 0 ]
do
  case "$1" in
    -f)  shift; rsfile="$1"; shift;;
    -n)  shift; parallelism="$1"; shift;;
    --) shift; break;;
    -*) usage ;;
    *)  break;;	# terminate while loop
  esac
done

# Emit a log line w/ iso8901 date prefixed
log() {
  echo `date +%Y-%m-%dT%H:%M:%S` $1
}

if [[ ! -s $rsfile ]]
then
    log "No such file: $rsfile"
    exit 1
fi

rs_array=($(cat $rsfile))
n_rs=${#rs_array[*]}

if [[ $parallelism -gt 0 ]] && [[ $parallelism -le $(($n_rs/2)) ]]
then
    :
else
    log "Illegal parallelism or too large parallelism: $parallelism"
    exit 1
fi

LOGDIR=/opt/logs/rollingrestart/$$
mkdir -p $LOGDIR

n_round=$(($n_rs/$parallelism))
if [[ $(($n_round * $parallelism)) -lt $n_rs ]]
then
    n_round=$(($n_round+1))
fi

log "rsfile: $rsfile, n_rs: $n_rs, parallelism: $parallelism, round: $n_round"

log "Disabling load balancer"
HBASE_BALANCER_STATE=`echo 'balance_switch false' | "$bin"/hbase --config ${HBASE_CONF_DIR} shell | tail -3 | head -1`
log "Previous balancer state was $HBASE_BALANCER_STATE"

excludefile=/tmp/$(basename $0)_excludefile.$$
for ((i=0; i<$n_round; i++))
do
    log "round $(($i+1))/$n_round"
    : >$excludefile
    for ((j=0; j<parallelism && i*parallelism+j<n_rs; j++));
    do
        echo ${rs_array[$(($i*$parallelism + $j))]} | tee -a $excludefile
    done

    while read rs
    do
        sh "$bin"/graceful_stop.sh --restart --reload --debug -x $excludefile --maxthreads 20 $rs &>$LOGDIR/$rs.$$.log &
    done <$excludefile

    wait

    if [[ $i -lt $(($n_round-1)) ]]
    then
        log "sleep 20s before next round"
        sleep 20
    fi
done

# Restore balancer state
if [ $HBASE_BALANCER_STATE != "false" ]; then
  log "Restoring balancer state to $HBASE_BALANCER_STATE"
  echo "balance_switch $HBASE_BALANCER_STATE" | "$bin"/hbase --config ${HBASE_CONF_DIR} shell &> /dev/null
fi

