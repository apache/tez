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

<head><title>Shuffle Handler</title></head>

## Tez Shuffle Handler Overview

A Tez specific shuffle handler allows Tez DAGs to shuffle data in a way that takes advantage of the new features in Tez.
In particular, the Tez shuffle handler allows DAGs to shuffle data more efficiently for Tez’s new data movements types
and runtime optimizations, such as auto-reduce parallelism. Long running Tez sessions will be able to clean up
intermediate data for completed queries and Tez applications can decide to clean up completed intermediate data for
running applications.

## Setup for the Tez Shuffle Handler
---------

_Requires_: **Apache Tez 0.9.0 or above**

Configuration in the client specify the Tez shuffle handler

```
tez-site.xml
-------------
...
<property>
  <name>tez.am.shuffle.auxiliary-service.id</name>
  <value>tez_shuffle</value>
</property>
...

```

## Deploying the Tez Shuffle Handler

The Tez Shuffle Handler jar artifact org.apache.org:tez-aux-services needs to be placed into the Node Manager classpath and restarted

## Setup for Node Manager

_Requires_: **Apache Hadoop 2.6.0 or above**

The following configuration needs to be setup in the Node Manager yarn-site.xml to enable the Tez Shuffle Handler

```
yarn-site.xml
-------------
...
<property>
  <name>yarn.nodemanager.aux-services</name>
  <value>tez_shuffle</value>
</property>

<property>
  <name>yarn.nodemanager.aux-services.tez_shuffle.class</name>
  <value>org.apache.tez.auxservices.ShuffleHandler</value>
</property>
...

```
