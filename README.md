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

Apache Tez
==========

Apache Tez is a generic data-processing pipeline engine envisioned as a low-level engine for higher abstractions
such as Apache Hadoop Map-Reduce, Apache Pig, Apache Hive etc.

At its heart, tez is very simple and has just two components:

*   The data-processing pipeline engine where-in one can plug-in input, processing and output implementations to 
    perform arbitrary data-processing. Every 'task' in tez has the following:
   -   Input to consume key/value pairs from.
   -   Processor to process them.
   -   Output to collect the processed key/value pairs.


*  A master for the data-processing application, where-by one can put together arbitrary data-processing 'tasks' 
   described above into a task-DAG to process data as desired. 
   The generic master is implemented as a Apache Hadoop YARN ApplicationMaster.
