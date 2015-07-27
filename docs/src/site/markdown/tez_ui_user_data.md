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

<head><title>Embedding Application Specific Data into Tez UI</title></head>

# Embedding Application Specific Data into Tez UI

The Tez UI is built mainly off data stored in [YARN Timeline]. The Tez API, currently, provides some minimal support for an application to inject application-specific data into the same storage layer. Using a general standard guideline by following a well-defined format, this data can also be displayed in the Tez UI. 

## Setting DAG-level Information

To set DAG level information, the API to use is DAG::setDAGInfo.  ( Please refer to the Javadocs for more detailed and up-to-date information )

The DAG::setDAGInfo() API expects a to be String passed to it. This string is recommended to be a json-encoded value with the following keys recognized keys by the UI:

  - "context": The application context in which this DAG is being used. For example, this could be set to "Hive" or "Pig" if this is being run as part of a Hive or Pig script.
  - "description": General description on what this DAG is going to do. In the case of Hive, this could be the SQL query text.

## Setting Information for each Input/Output/Processor

Each Input/Output/Processor specified in the DAG Plan is specified via a TezEntityDescriptor. Applications specify a user payload that is used to initialize/configure the instance as needed. From a Tez UI point of view, users are usually keen to understand what "work" the particular Input/Output/Processor is doing in addition to any additional configuration information on how the object was initialized/configured. Keeping that in mind, each TezEntityDescriptor supports an api for application developers to specify this information when creating the DAG plan. The API to use for this is setHistoryText(). 

The setHistoryText() API expects a String to be passed to it. This string is recommended to be a json-encoded value with the following keys recognized keys by the UI:

  - "desc" : A simple string describing for the object in question. For example, for a particular Hive Processor, this could be a description of what that particular processor is doing.
  - "config" : A map of key-value pairs representing the configuration/payload used to initialize the object in question.

By default, the Inputs/Outputs/Processors that are part of the tez-runtime-library do not publish their configuration information via the setHistoryText() API. To enable this, the following property needs to be enabled:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.runtime.convert.user-payload.to.history-text&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;true&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

Please note that this is a data-heavy operation and adds a lot of load on the TimelineServer (due to the additional amount of configuration data generated) and therefore should be disabled for most jobs unless there is a real debugging need.

## Use of DAG Info and History Text in the Tez UI

If the data setup in the DAG Info and History Text conforms to the format expected by the UI, it will be displayed in the Tez UI in an easy to consume manner. In cases where this is not possible, the UI may fall back to either not displaying the data at all or displaying the string as is in a safe manner. 

[YARN Timeline]:./tez_yarn_timeline.html
