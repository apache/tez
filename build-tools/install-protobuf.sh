#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script attempts to install an arbitrary version of protobuf if needed.
# The desired version should be the first parameter: $1.
# Typical usage: the script is automatically called from tez-api (by maven) during the build process.

# This script runs from build-tools folder. The user can remove
# the dynamically installed protobuf anytime like:
# rm -rf ./build-tools/protobuf/ #from root folder

set -x
PROTOBUF_VERSION=${1:-2.5.0}
PROTOBUF_MAJOR_VERSION=$(echo "$PROTOBUF_VERSION" | cut -d. -f1)
if [ -n "$ZSH_VERSION" ]; then
   SCRIPT_DIR="${0:a:h}"
else
   SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
fi

function install_protobuf {
    # before protobuf 3, there is no pre-compiled executables are host on github, let's try to build and make it
    if (( PROTOBUF_MAJOR_VERSION < 3 )); then
        wget "https://github.com/google/protobuf/releases/download/v$PROTOBUF_VERSION/protobuf-$PROTOBUF_VERSION.tar.gz"
        tar -xzvf "protobuf-$PROTOBUF_VERSION.tar.gz"
        rm "protobuf-$PROTOBUF_VERSION.tar.gz"
        cd "protobuf-$PROTOBUF_VERSION" && ./configure --prefix=/usr && make && sudo make install
    # since protobuf 3, there are precompiled protoc executables on github, let's quickly download and use it
    else
        ARCH=`uname -m`
        case "$(uname -s)" in
            Darwin)
                FILE_NAME="protoc-$PROTOBUF_VERSION-osx-$ARCH"
                ;;
            Linux)
                if test $ARCH = "aarch64"; then
                    ARCH="aarch_64"
                fi
                FILE_NAME="protoc-$PROTOBUF_VERSION-linux-$ARCH"
                ;;
            *)
                echo "Unsupported OS returned by uname -s, you'll have to install protobuf 3.x manually"
                exit 1
                ;;
        esac
        rm -f "$FILE_NAME.zip" #cleanup unfinished file if any
        wget "https://github.com/google/protobuf/releases/download/v$PROTOBUF_VERSION/$FILE_NAME.zip"
        mkdir "$SCRIPT_DIR/protobuf"
        unzip -o "$FILE_NAME.zip" -d "$SCRIPT_DIR/protobuf"
        rm "$FILE_NAME.zip"
    fi
}

if test -f "$SCRIPT_DIR/protobuf/bin/protoc"; then
    PROTOBUF_INSTALLED_VERSION=$("$SCRIPT_DIR/protobuf/bin/protoc" --version)
else
    PROTOBUF_INSTALLED_VERSION=$(protoc --version)
fi

PROTOC_EXIT_CODE=$?

if [ $PROTOC_EXIT_CODE -eq 0 ]; then
    PROTOBUF_INSTALLED_VERSION=$(echo "$PROTOBUF_INSTALLED_VERSION" | tr -s ' ' | cut -d ' ' -f 2)
    if [ "$PROTOBUF_INSTALLED_VERSION" == "$PROTOBUF_VERSION" ]; then
        echo "Current protobuf version is equal to the requested ($PROTOBUF_INSTALLED_VERSION), exiting..."
    else
        echo "Current protobuf version ($PROTOBUF_INSTALLED_VERSION) is not equal to the requested ($PROTOBUF_VERSION), installing $PROTOBUF_VERSION"
        install_protobuf
    fi
else
    echo "protoc --version command had non-zero return value, need to install probuf"
    install_protobuf
fi
