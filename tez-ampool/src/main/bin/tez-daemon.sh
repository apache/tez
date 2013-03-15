#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


# Runs a Hadoop command as a daemon.
#
# Environment Variables
#
#   TEZ_CONF_DIR  Alternate conf dir. Default is ${TEZ_HOME}/conf.
#   TEZ_LOG_DIR   Where log files are stored.  PWD by default.
#   TEZ_PID_DIR   The pid files are stored. /tmp by default.
#   TEZ_IDENT_STRING   A string representing this instance of tez. $USER by default
#   TEZ_NICENESS The scheduling priority for daemons. Defaults to 0.
##

usage="Usage: tez-daemon.sh [--config <conf-dir>] (start|stop) <tez-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "${BASH_SOURCE-$0}"`
bin=`cd "$bin"; pwd`

DEFAULT_LIBEXEC_DIR="$bin"/../libexec
TEZ_LIBEXEC_DIR=${TEZ_LIBEXEC_DIR:-$DEFAULT_LIBEXEC_DIR}
. $TEZ_LIBEXEC_DIR/tez-config.sh

# get arguments

startStop=$1
shift
command=$1
shift

tez_rotate_log ()
{
  log=$1;
  num=5;
  if [ -n "$2" ]; then
  	num=$2
  fi
  if [ -f "$log" ]; then # rotate logs
	  while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	  done
	  mv "$log" "$log.$num";
  fi
}

if [ -f "${TEZ_CONF_DIR}/tez-env.sh" ]; then
  . "${TEZ_CONF_DIR}/tez-env.sh"
fi

if [ "$TEZ_IDENT_STRING" = "" ]; then
  export TEZ_IDENT_STRING="$USER"
fi

# get log directory
if [ "$TEZ_LOG_DIR" = "" ]; then
  export TEZ_LOG_DIR="$TEZ_HOME/logs"
fi

if [ ! -w "$TEZ_LOG_DIR" ] ; then
  mkdir -p "$TEZ_LOG_DIR"
  chown $TEZ_IDENT_STRING $TEZ_LOG_DIR
fi

if [ "$TEZ_PID_DIR" = "" ]; then
  TEZ_PID_DIR=/tmp
fi

# some variables
export TEZ_LOGFILE=tez-$TEZ_IDENT_STRING-$command-$HOSTNAME.log
export TEZ_ROOT_LOGGER=${TEZ_ROOT_LOGGER:-"INFO,RFA"}
log=$TEZ_LOG_DIR/tez-$TEZ_IDENT_STRING-$command-$HOSTNAME.out
pid=$TEZ_PID_DIR/tez-$TEZ_IDENT_STRING-$command.pid
TEZ_STOP_TIMEOUT=${TEZ_STOP_TIMEOUT:-5}

# Set default scheduling priority
if [ "$TEZ_NICENESS" = "" ]; then
    export TEZ_NICENESS=0
fi

TEZ_OPTS="$TEZ_OPTS -Dtez.log.dir=$TEZ_LOG_DIR"
TEZ_OPTS="$TEZ_OPTS -Dtez.log.file=$TEZ_LOGFILE"
TEZ_OPTS="$TEZ_OPTS -Dtez.home.dir=$TEZ_HOME"
TEZ_OPTS="$TEZ_OPTS -Dtez.id.str=$TEZ_IDENT_STRING"
TEZ_OPTS="$TEZ_OPTS -Dtez.root.logger=${TEZ_ROOT_LOGGER:-INFO,console}"

case $startStop in

  (start)

    [ -w "$TEZ_PID_DIR" ] ||  mkdir -p "$TEZ_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    tez_rotate_log $log
    echo starting $command, logging to $log
    cd "$TEZ_HOME"
    case $command in
      ampoolservice)
        export CLASSPATH=$CLASSPATH
        nohup nice -n $TEZ_NICENESS "$JAVA" $JAVA_HEAP_MAX $TEZ_OPTS \
          org.apache.tez.ampool.AMPoolService --cli \
          > "$log" 2>&1 < /dev/null &

      ;;
      (*)
         echo "Unknown command $command"
         echo "Only command 'ampoolservice' is supported"
         exit 1
      ;;
    esac
    echo $! > $pid
    sleep 1
    echo "ulimit -a for user $USER" >> $log
    ulimit -a >> $log 2>&1
    head -30 "$log"
    sleep 3;
    if ! ps -p $! > /dev/null ; then
      exit 1
    fi
    ;;

  (stop)

    if [ -f $pid ]; then
      TARGET_PID=`cat $pid`
      if kill -0 $TARGET_PID > /dev/null 2>&1; then
        echo stopping $command
        kill $TARGET_PID
        sleep $TEZ_STOP_TIMEOUT
        if kill -0 $TARGET_PID > /dev/null 2>&1; then
          echo "$command did not stop gracefully after $TEZ_STOP_TIMEOUT seconds: killing with kill -9"
          kill -9 $TARGET_PID
        fi
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac


