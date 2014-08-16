#!/bin/bash

set -e

APPID=$1

YARN=$(which yarn);
TMP=$(mktemp)

echo "Fetching yarn logs for $APPID"
$YARN logs -applicationId $APPID | grep HISTORY > $TMP 
python swimlane.py -o $APPID.svg $TMP
