It can be time consuming to download logs via "yarn logs -applicationId <appId> | grep something". Also mining large volumes of logs can be time consuming on single node.
This is a simple Pig loader to parse TFiles and provide line by line format (tuple of (machine, key, line)) for distributed processing of logs.

Build/Install:
==============
1. "mvn clean package" should create "tfile-parser-x.y.z-SNAPSHOT.jar" would be created in ./target directory

Running pig with tez:
====================
1. Install pig
2. $PIG_HOME/bin/pig -x tez (to open grunt shell)

Sample pig script:
==================
set pig.splitCombination false;
set tez.grouping.min-size 52428800;
set tez.grouping.max-size 52428800;

register 'tfile-parser-1.0-SNAPSHOT.jar';
raw = load '/app-logs/root/logs/application_1411511669099_0769/*' using org.apache.tez.tools.TFileLoader() as (machine:chararray, key:chararray, line:chararray);
filterByLine = FILTER raw BY (key MATCHES '.*container_1411511669099_0769_01_000001.*')
                   AND (line MATCHES '.*Shuffle.*');
dump filterByLine;

