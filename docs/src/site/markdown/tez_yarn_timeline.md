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
<head><title>Using YARN Timeline with Tez for History</title></head>

## YARN Timeline Background

Initial support for [YARN Timeline](http://hadoop.apache.org/docs/r2.4.0/hadoop-yarn/hadoop-yarn-site/TimelineServer.html) was introduced in Apache Hadoop 2.4.0. Support for ACLs in Timeline was introduced in Apache Hadoop 2.6.0. Support for Timeline was introduced in Tez in 0.5.x ( with some experimental support in 0.4.x ). However, Tez ACLs integration with Timeline is only available from Tez 0.6.0 onwards.

## How Tez Uses YARN Timeline

Tez uses YARN Timeline as its application history store. Tez stores most of its lifecycle information into this history store such as:
  - DAG information such as:
    - DAG Plan
    - DAG Submission, Start and End times
    - DAG Counters
    - Final status of the DAG and additional diagnostics
  - Vertex, Task and Task Attempt Information
    - Start and End times
    - Counters
    - Diagnostics

Using the above information, a user can analyze a Tez DAG while it is running and after it has completed.

## YARN Timeline and Hadoop Versions

Given that the support for YARN Timeline with full security was only realized in Apache Hadoop 2.6.0, some features may or may not be supported depending on which version of Apache Hadoop is used.


|  | Hadoop 2.2.x, 2.3.x | Hadoop 2.4.x, 2.5.x | Hadoop 2.6.x and higher |
| ------- | ----- | ----- | ----- |
| Timeline Support | No | Yes | Yes |
| Timeline with ACLs Support | No | No | Yes |

## Configuring Tez to use YARN Timeline

By default, Tez writes its history data into a file on HDFS. To use Timeline, add the following property into your tez-site.xml:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.history.logging.service.class&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

For Tez 0.4.x, the above property is not respected. For 0.4.x, please set the following property:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.yarn.ats.enabled&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;true&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

When using Tez with Apache Hadoop 2.4.x or 2.5.x, given that these versions are not fully secure, the following property also needs to be enabled:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.allow.disabled.timeline-domains&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;true&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>
