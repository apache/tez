/**
* Licensed to the Apache Software Foundation (ASF) under one
* or more contributor license agreements.  See the NOTICE file
* distributed with this work for additional information
* regarding copyright ownership.  The ASF licenses this file
* to you under the Apache License, Version 2.0 (the
* "License"); you may not use this file except in compliance
* with the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package org.apache.tez.dag.app;

import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.node.AMNodeTracker;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.records.TezDAGID;


/**
 * Context interface for sharing information across components in Tez DAG
 */
@InterfaceAudience.Private
public interface AppContext {

  DAGAppMaster getAppMaster();

  Configuration getAMConf();

  ApplicationId getApplicationID();

  TezDAGID getCurrentDAGID();

  ApplicationAttemptId getApplicationAttemptId();

  String getApplicationName();

  Map<ApplicationAccessType, String> getApplicationACLs();

  long getStartTime();

  String getUser();

  DAG getCurrentDAG();

  void setDAG(DAG dag);

  Set<String> getAllDAGIDs();

  @SuppressWarnings("rawtypes")
  EventHandler getEventHandler();

  Clock getClock();

  ClusterInfo getClusterInfo();

  AMContainerMap getAllContainers();

  AMNodeTracker getNodeTracker();

  TaskSchedulerEventHandler getTaskScheduler();

  boolean isSession();

  DAGAppMasterState getAMState();

  HistoryEventHandler getHistoryHandler();

  Path getCurrentRecoveryDir();

  boolean isRecoveryEnabled();

  ACLManager getAMACLManager();

  String getAMUser();
}
