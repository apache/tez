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

__For information on how application-specific data ends up being displayed in the Tez UI, please refer to [this User Guide](tez_ui_user_data.html)__

## Setting up Tez for the Tez UI
---------

_Requires_: **Tez 0.6.0 or above**

For Tez to have a functional UI and for the History links to work correctly from the YARN ResourceManager UI, the following settings need to be enabled in tez-site.xml:

```
tez-site.xml
-------------
...
<property>
  <description>Enable Tez to use the Timeline Server for History Logging</description>
  <name>tez.history.logging.service.class</name>
  <value>org.apache.tez.dag.history.logging.ats.ATSHistoryLoggingService</value>
</property>

<property>
  <description>URL for where the Tez UI is hosted</description>
  <name>tez.tez-ui.history-url.base</name>
  <value>http://<webserver-host:9999/tez-ui/</value>
</property>
...

```

## Deploying the Tez UI

The tez-ui.war can be obtained in 3 ways:

  - Obtain the tez-ui.war from the binary artifacts [for a given release](./releases/). Binary artifacts are not available for all releases and in such cases, please use any of the other options listed below.
  - Obtain the tez-ui.war from the [Maven repo](https://repository.apache.org/content/repositories/releases/org/apache/tez/tez-ui/) for a given release.
  - Build the tez-ui.war from the source itself. Refer to the README.txt in the tez-ui module for more details.

Once the tez-ui.war is available, you can extract the contents of the war and host them on any webserver. The Tez UI consists only of html, js and css files
and does not require any complex server-side hosting logic.

### Configuring the Timeline Server URL and Resource Manager UI URL.
By default, the Tez UI attempts to connect to Timeline Server using the same host as the Tez UI.  For example, if the UI is hosted on localhost, the Timeline Server URL is assumed to be http(s)://localhost:8188 and the Resource manager web url is assumed to be http(s)://localhost:8088.

If the Timeline Server and/or Resource manager is hosted on a different host, the Tez UI needs corresponding changes to be configured in scripts/configs.js ( within the extracted tez-ui.war). Uncomment the following lines and set the hostname and port appropriately. "timelineBaseUrl" maps to YARN Timeline Server and "RMWebUrl" maps to YARN ResourceManager.

```
    // timelineBaseUrl: 'http://localhost:8188',
    // RMWebUrl: 'http://localhost:8088',

```

### Hosting the UI in Tomcat.

1. Remove any old deployments in $TOMCAT_HOME/webapps
2. Extract the war into $TOMCAT_HOME/webapps/tez-ui/
3. Modify scripts/config.js as needed.
4. Restart tomcat and the UI should be available under the tez-ui/ path.

### Hosting the UI using any standalone webserver
1. Untar the war file
2. Modify scripts/config.js as needed.
3. Copy the resulting directory to the document root of the web server.
4. Restart the webserver

## Additional Setup steps for the Application Timeline Server

_Requires_: **Apache Hadoop 2.6.0 or above**

The following configurations need to be setup in yarn-site.xml for correct functional behavior of the Timeline Server with respect to the Tez UI. Replace localhost with the actual hostname if running in a distributed setup.

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

<property>
  <description>Publish YARN information to Timeline Server</description>
  <name> yarn.resourcemanager.system-metrics-publisher.enabled</name>
  <value>true</value>
</property>
...
```

__For more detailed information (setup, configuration, deployment), please refer to the [Apache Hadoop Documentation on the Application Timeline Server](http://hadoop.apache.org/docs/current/hadoop-yarn/hadoop-yarn-site/TimelineServer.html)__

__For general information on the compatibility matrix of the Tez UI with YARN TimelineServer, please refer to the [Tez - Timeline Server Guide](tez_yarn_timeline.html)__


