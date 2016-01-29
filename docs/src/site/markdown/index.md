<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->

<head><title>Welcome to Apache Tez&trade;</title></head>

Introduction
------------

The Apache Tez&trade; project is aimed at building an application framework
which allows for a complex directed-acyclic-graph of tasks for processing
data. It is currently built atop
[Apache Hadoop YARN](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/YARN.html).

The 2 main design themes for Tez are:

-   **Empowering end users by:**
    -   Expressive dataflow definition APIs
    -   Flexible Input-Processor-Output runtime model
    -   Data type agnostic
    -   Simplifying deployment
-   **Execution Performance**
    -   Performance gains over Map Reduce
    -   Optimal resource management
    -   Plan reconfiguration at runtime
    -   Dynamic physical data flow decisions

By allowing projects like Apache Hive and Apache Pig to run a complex
DAG of tasks, Tez can be used to process data, that earlier took
multiple MR jobs, now in a single Tez job as shown below.

![Flow for a Hive or Pig Query on MapReduce](./images/PigHiveQueryOnMR.png)
![Flow for a Hive or Pig Query on Tez](./images/PigHiveQueryOnTez.png)

To download the Apache Tez software, go to the [Releases](./releases/index.html) page.
