<!--
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

Counter-Diff
============

This is a post-hoc analysis tool for Apache Tez which shows the diffences in counter
values for two different DAGs. This tool uses dag_id.zip as input which can be
downloaded from Tez UI.

Pre-requisite:
--------------
This tool requires texttable python package for printing output in tabular format.
Before using the tool, make sure to install texttable using `pip install texttable`

To use the tool, run

`python counter-diff.py dag_1.zip dag_2.zip [--detail]`

This will print counter output difference between the specified DAGs like this

Example: `python counter-diff.py dag_1499978510619_0002_143.zip dag_1499978510619_0002_144.zip`

```
+-------------------+-------------------------------------+----------------------------+----------------------------+-------------+
| Counter Group     | Counter Name                        | dag_1499978510619_0002_143 | dag_1499978510619_0002_144 | delta       |
+-------------------+-------------------------------------+----------------------------+----------------------------+-------------+
|                   | TOTAL_LAUNCHED_TASKS                | 12585                      | 12585                      | 0           |
|                   | AM_GC_TIME_MILLIS                   | 1755                       | 1977                       | +222        |
| DAGCounter        | AM_CPU_MILLISECONDS                 | 258490                     | 66780                      | -191710     |
|                   | NUM_SUCCEEDED_TASKS                 | 12585                      | 12585                      | 0           |
|                   | DATA_LOCAL_TASKS                    | 10862                      | 10862                      | 0           |
+-------------------+-------------------------------------+----------------------------+----------------------------+-------------+
|                   | FILE_BYTES_WRITTEN                  | 218900517                  | 218900517                  | 0           |
|                   | HDFS_READ_OPS                       | 2                          | 2                          | 0           |
| FileSystemCounter | FILE_BYTES_READ                     | 5144112                    | 4779466                    | -364646     |
|                   | WASB_BYTES_READ                     | 0                          | 16154935                   | +16154935   |
|                   | HDFS_WRITE_OPS                      | 2                          | 2                          | 0           |
|                   | HDFS_BYTES_WRITTEN                  | 104                        | 104                        | 0           |
+-------------------+-------------------------------------+----------------------------+----------------------------+-------------+
|                   | SPILLED_RECORDS                     | 105948                     | 105948                     | 0           |
|                   | LAST_EVENT_RECEIVED                 | 575819                     | 226519                     | -349300     |
|                   | ADDITIONAL_SPILLS_BYTES_WRITTEN     | 687529                     | 687413                     | -116        |
|                   | SHUFFLE_PHASE_TIME                  | 862942                     | 402967                     | -459975     |
|                   | SHUFFLE_BYTES_DECOMPRESSED          | 8539266320                 | 8539876954                 | +610634     |
|                   | MERGED_MAP_OUTPUTS                  | 52594                      | 52594                      | 0           |
|                   | OUTPUT_BYTES                        | 7079513                    | 7079513                    | 0           |
|                   | OUTPUT_RECORDS                      | 85883                      | 85883                      | 0           |
|                   | FIRST_EVENT_RECEIVED                | 256781                     | 38903                      | -217878     |
|                   | REDUCE_INPUT_RECORDS                | 52974                      | 52974                      | 0           |
|                   | INPUT_RECORDS_PROCESSED             | 4587575                    | 4563880                    | -23695      |
| TaskCounter       | OUTPUT_BYTES_PHYSICAL               | 105752877                  | 105752877                  | 0           |
|                   | ADDITIONAL_SPILLS_BYTES_READ        | 715818                     | 713333                     | -2485       |
|                   | SHUFFLE_BYTES_TO_MEM                | 537430239                  | 538217106                  | +786867     |
|                   | MERGE_PHASE_TIME                    | 280112                     | 294778                     | +14666      |
|                   | SHUFFLE_BYTES_DISK_DIRECT           | 5629283                    | 5268494                    | -360789     |
|                   | REDUCE_INPUT_GROUPS                 | 29225                      | 29225                      | 0           |
|                   | SHUFFLE_CHUNK_COUNT                 | 11280                      | 11280                      | 0           |
|                   | INPUT_SPLIT_LENGTH_BYTES            | 5128710330361              | 5128710330361              | 0           |
|                   | OUTPUT_BYTES_WITH_OVERHEAD          | 35610110                   | 35610110                   | 0           |
|                   | NUM_SKIPPED_INPUTS                  | 4657694                    | 4657694                    | 0           |
|                   | SHUFFLE_BYTES                       | 543059522                  | 543485600                  | +426078     |
|                   | NUM_SHUFFLED_INPUTS                 | 109451                     | 109871                     | +420        |
+-------------------+-------------------------------------+----------------------------+----------------------------+-------------+
|                   | FAILED_TASK_ATTEMPTS                | 0                          | 0                          | 0           |
|                   | FAILED_TASKS                        | 0                          | 0                          | 0           |
|                   | COMPLETED_TASKS                     | 12585                      | 12585                      | 0           |
| otherinfo         | KILLED_TASK_ATTEMPTS                | 0                          | 0                          | 0           |
|                   | SUCCEEDED_TASKS                     | 12585                      | 12585                      | 0           |
|                   | KILLED_TASKS                        | 0                          | 0                          | 0           |
|                   | TIME_TAKEN                          | 250198                     | 68981                      | -181217     |
+-------------------+-------------------------------------+----------------------------+----------------------------+-------------+
```