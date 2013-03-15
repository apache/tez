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

# included in all the tez scripts with source command
# should not be executable directly
# also should not be passed any arguments, since we need original $*

# Resolve links ($0 may be a softlink) and convert a relative path
# to an absolute path.  NB: The -P option requires bash built-ins
# or POSIX:2001 compliant cd and pwd.

#   TEZ_CLASSPATH Extra Java CLASSPATH entries.
#

this="${BASH_SOURCE-$0}"
tez_bin=$(cd -P -- "$(dirname -- "$this")" && pwd -P)
script="$(basename -- "$this")"
this="$tez_bin/$script"

TEZ_DIR=${TEZ_DIR:-""}
TEZ_LIB_DIR=${TEZ_LIB_DIR:-"lib"}

TEZ_DEFAULT_HOME=$(cd -P -- "$tez_bin"/.. && pwd -P)
TEZ_HOME=${TEZ_HOME:-$TEZ_DEFAULT_HOME}
export TEZ_HOME

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
  if [ "--config" = "$1" ]
  then
    shift
    confdir=$1
    shift
    TEZ_CONF_DIR=$confdir
  fi
fi

export TEZ_CONF_DIR="${TEZ_CONF_DIR:-$TEZ_HOME/conf}"

if [ -f "${TEZ_CONF_DIR}/tez-env.sh" ]; then
  . "${TEZ_CONF_DIR}/tez-env.sh"
fi

# Newer versions of glibc use an arena memory allocator that causes virtual
# memory usage to explode.
export MALLOC_ARENA_MAX=${MALLOC_ARENA_MAX:-4}

# Attempt to set JAVA_HOME if it is not set
if [[ -z $JAVA_HOME ]]; then
  # On OSX use java_home (or /Library for older versions)
  if [ "Darwin" == "$(uname -s)" ]; then
    if [ -x /usr/libexec/java_home ]; then
      export JAVA_HOME=($(/usr/libexec/java_home))
    else
      export JAVA_HOME=(/Library/Java/Home)
    fi
  fi

  # Bail if we did not detect it
  if [[ -z $JAVA_HOME ]]; then
    echo "Error: JAVA_HOME is not set and could not be found." 1>&2
    exit 1
  fi
fi

JAVA=$JAVA_HOME/bin/java
# some Java parameters
JAVA_HEAP_MAX=-Xmx1024m

# check envvars which might override default args
if [[ "$TEZ_HEAPSIZE" != "" ]]; then
  #echo "run with heapsize $TEZ_HEAPSIZE"
  JAVA_HEAP_MAX="-Xmx""$TEZ_HEAPSIZE""m"
  #echo $JAVA_HEAP_MAX
fi

# CLASSPATH initially contains $TEZ_CONF_DIR
CLASSPATH="${TEZ_CONF_DIR}"

if [ "$TEZ_USER_CLASSPATH_FIRST" != "" ] && [ "$TEZ_CLASSPATH" != "" ] ; then
  CLASSPATH=${CLASSPATH}:${TEZ_CLASSPATH}
fi

CLASSPATH=${CLASSPATH}:${TEZ_HOME}/${TEZ_DIR}'/*':${TEZ_HOME}/${TEZ_LIB_DIR}'/*'

# so that filenames w/ spaces are handled correctly in loops below
IFS=

if [[ -z $HADOOP_HOME ]]; then
  HADOOP_PATH=`which hadoop`
  result=$?
  if [ "$result" != "0" ]; then
    echo 'Failed to find hadoop in $PATH.'
    echo 'Please ensure that HADOOP_HOME is defined or hadoop is in your $PATH.'
    exit 1
  fi
else
  HADOOP_PATH="$HADOOP_HOME/bin/hadoop"
fi

HADOOP_CLASSPATH=`$HADOOP_PATH classpath`
result=$?
if [ "$result" != "0" ]; then
   echo "Failed to run $HADOOP_PATH classpath. Result=$result."
   echo 'Please ensure that HADOOP_HOME is defined or hadoop is in your $PATH.'
   exit 1
fi
CLASSPATH=${CLASSPATH}:$HADOOP_CLASSPATH

# add user-specified CLASSPATH last
if [ "$TEZ_USER_CLASSPATH_FIRST" = "" ] && [ "$TEZ_CLASSPATH" != "" ]; then
  CLASSPATH=${CLASSPATH}:${TEZ_CLASSPATH}
fi

# default log directory & file
if [ "$TEZ_LOG_DIR" = "" ]; then
  TEZ_LOG_DIR="$TEZ_HOME/logs"
fi

# restore ordinary behaviour
unset IFS

echo "$CLASSPATH"
