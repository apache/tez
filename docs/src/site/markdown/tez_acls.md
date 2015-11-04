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

<head><title>Access Control in Tez</title></head>

## Background

Access control in Tez can be categorized as follows:

  - Modify permissions on the Tez AM ( or Session ). Users with this permision can:
    - Submit a DAG to a Tez Session
    - Kill any DAG within the given AM/Session
    - Kill the Session
  - View permissions on the Tez AM ( or Session ). Users with this permision can:
    - Monitor/View the status of the Session
    - Monitor/View the progress/status of any DAG within the given AM/Session
  - Modify permissions on a particular Tez DAG. Users with this permision can:
    - Kill the DAG
  - View permissions on a particular Tez DAG. Users with this permision can:
    - Monitor/View the progress/status of the DAG

From the above, you can see that All users/groups that have access to do operations on the AM also have access to similar operations on all DAGs within that AM/session. Also, by default, the owner of the Tez AM,  i.e. the user who started the Tez AM, is considered a super-user and has access to all operations on the AM as well as all DAGs within the AM/Session.

Support for ACLs was introduced in Tez 0.5.0. Integration of these ACLs with YARN Timeline is only available from Tez 0.6.0 onwards.

## ACLs and the Tez UI

For [versions of YARN Timeline Server](./tez_yarn_timeline.html) that support ACLs, the UI will respect the ACLs for such secure clusters. For example, when a particular
authenticated user is viewing the "All DAGs" page on the UI, the user will only be shown DAGs that the user has access to view.

## How to setup the ACLs

By default, ACLs are always enabled in Tez. To disable ACLs, set the following configuration property:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.am.acls.enabled&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;false&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

### YARN Administration ACLs

YARN Administration ACLs are driven by configuration at the cluster level. YARN administrators are granted AM level view and modify permissions. One current limitation is that a modification to the cluster wide yarn.admin.acl configuration while an AM is running is not reflected in the AM view and modify ACLs. To setup the ACLs, the following properties need to be defined:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;yarn.admin.acl&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

The format of the value is a comma-separated list of users and groups with the users and groups separated by a single whitespace. e.g. "user1,user2 group1,group2". To allow all users to do a given operation, the value "*" can be specified.

### AM/Session Level ACLs

AM/Session level ACLs are driven by configuration. To setup the ACLs, the following properties need to be defined:

> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.am.view-acls&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>
> &lt;property&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;name&gt;tez.am.modify-acls&lt;/name&gt;<br/>
> &nbsp;&nbsp;&nbsp;&lt;value&gt;&lt;/value&gt;<br/>
> &lt;/property&gt;<br/>

The format of the value is a comma-separated list of users and groups with the users and groups separated by a single whitespace. e.g. "user1,user2 group1,group2". To allow all users to do a given operation, the value "*" can be specified.

### DAG ACLs

In certain scenarios, applications may need DAGs running within a given Session to have different access permissions. In such cases, the ACLs for each DAG can be specified programmatically via the DAG API. Look for DAG::setAccessControls in the API docs for the Tez release that you are using.
In this scenario, it is important to note that the Session ACLs should be defined with only super-users specified to ensure that other users do not inadvertently gain access to information for all DAGs within the given Session.
