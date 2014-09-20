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

<head><title>Tez UI</title></head>

Tez UI Overview
---------------

The Tez UI relies on the _Application Timeline Server_ whose role is as a
backing store for the application data generated during the lifetime of a
YARN application.

Tez provides its own UI that interfaces with the _Application Timeline Server_
and displays both a live and historical view of the Tez application inside
a Tez UI web application.

Application Timeline Server Setup
---------------------------------

_Requires_: **Hadoop 2.6.0-SNAPSHOT**

A minimal _yarn-site.xml_ configuration snippet is provided below. Replace
localhost with the actual hostname if running in a distributed setup.

```
yarn-site.xml
-------------
...
<property>
  <description>Indicate to clients whether Timeline service is enabled or not.
  If enabled, the TimelineClient library used by end-users will post entities
  and events to the Timeline server.</description>
  <name>yarn.timeline-service.enabled</name>
  <value>true</value>
</property>

<property>
  <description>The hostname of the Timeline service web application.</description>
  <name>yarn.timeline-service.hostname</name>
  <value>localhost</value>
</property>

<property>
  <description>Enables cross-origin support (CORS) for web services where
  cross-origin web response headers are needed. For example, javascript making
  a web services request to the timeline server.</description>
  <name>yarn.timeline-service.http-cross-origin.enabled</name>
  <value>true</value>
</property>
...
```

[Detailed Application Timeline Server Configuration](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/TimelineServer.html)

```
# Starting the Application Timeline Server
$ $HADOOP_PREFIX/sbin/yarn-daemon.sh start timelineserver
```

Tez Setup
---------

_Requires_: **Tez 0.6.0-SNAPSHOT**

A minimal _tez-site.xml_ configuration snippet is provided below

```
tez-site.xml
-------------
...
<property>
  <description>Log history using the Application Timeline Server</description>
  <name>tez.history.logging.service.class</name>
  <value>org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService</value>
</property>
...
```

Tez UI Install
--------------

Tez UI is currently under development and is targeted for the 0.6.0 release. A
proof-of-concept Tez UI is available in the Tez UI feature branch TEZ-8.

```
$ git clone git://git.apache.org/tez.git
$ cd tez
$ git checkout TEZ-8

# Navigate you web browser to file:///<tez git directory root>/tez-ui/original/login.html
# Enter hostname to the Application Timeline Server hostname specified in yarn-site.xml
# Enter port to the Application Timeline Server as 8188
```
