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

## Tez UI Overview

The Tez UI relies on the _Application Timeline Server_ whose role is as a backing store for the
application data generated during the lifetime of a YARN application.

Tez provides its own UI that interfaces with the _Application Timeline Server_ and displays both a
live and historical view of the Tez application inside a Tez UI web application.

## Application Timeline Server Setup

_Requires_: **Hadoop 2.6.0 or above**

A minimal _yarn-site.xml_ configuration snippet is provided below. Replace localhost with the actual
hostname if running in a distributed setup.

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

## Tez Setup
---------

_Requires_: **Tez 0.6.0 or above**

A minimal _tez-site.xml_ configuration snippet is provided below

```
tez-site.xml
-------------
...
<property>
  <description>Log history using the Timeline Server</description>
  <name>tez.history.logging.service.class</name>
  <value>org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService</value>
</property>
<property>
  <description>Publish configuration information to Timeline server.</description>
  <name>tez.runtime.convert.user-payload.to.history-text</name>
  <value>true</value>
</property>
...

```

[Detailed Tez - Timeline Server Configuration](tez_yarn_timeline.html)

[Embedding Application Specific Data into Tez UI](tez_ui_user_data.html)

## Tez UI Install

### Building Tez UI from sources
For instructions on building Tez UI from sources or setting up a development environment see the
README.txt in tez-ui subfolder. The build generates a tarball as well as a war file of the distribution
in the tez-ui/target. currently the Tez-UI can be hosted using one of the following options.

### hosting using the war file and tomcat.
1. Remove any old deployments in $TOMCAT_HOME/webapps
2. Copy the war to $TOMCAT_HOME/webapps
3. Restart tomcat and the war will get deployed. The content of the war would be available in
$TOMCAT_HOME/webapps/tez-ui-x.x.x.

### hosting using tarball and a standalone webserver
1. untar the tarball
2. copy the resulting directory to the document root of the web server.
3. reload/restart the webserver

### configuring the Timeline Server URL and Resource manager web url.
By default, the Tez UI attempts to connect to Timeline Server using the same host as the Tez UI.
For example, if the UI is hosted on localhost, the Timeline Server URL is assumed to be
http(s)://localhost:8188 and the Resource manager web url is assumed to be http(s)://localhost:8088.

If the Timeline Server and/or Resource manager is hosted on a different host, the Tez UI needs
corresponding changes to be configured in scripts/configs.js. Uncomment the following lines and
set the hostname and port appropriately.

```
    // timelineBaseUrl: 'http://localhost:8188',
    // RMWebUrl: 'http://localhost:8088',

```
