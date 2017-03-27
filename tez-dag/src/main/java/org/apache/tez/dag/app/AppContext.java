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
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.ApplicationAccessType;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.app.RecoveryParser.DAGRecoveryData;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.rm.TaskSchedulerManager;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.app.rm.node.AMNodeTracker;
import org.apache.tez.common.security.ACLManager;
import org.apache.tez.dag.history.HistoryEventHandler;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.hadoop.shim.HadoopShim;

import com.google.common.util.concurrent.ListeningExecutorService;


/**
 * Context interface for sharing information across components in Tez DAG
 */
@InterfaceAudience.Private
public interface AppContext {

  DAGAppMaster getAppMaster();

  Configuration getAMConf();

  ApplicationId getApplicationID();

  TezDAGID getCurrentDAGID();
  
  long getCumulativeCPUTime();
  
  long getCumulativeGCTime();

  ApplicationAttemptId getApplicationAttemptId();

  String getApplicationName();

  Map<ApplicationAccessType, String> getApplicationACLs();

  long getStartTime();

  String getUser();

  DAG getCurrentDAG();
  
  ListeningExecutorService getExecService();

  void setDAG(DAG dag);

  void setDAGRecoveryData(DAGRecoveryData dagRecoveryData);

  Set<String> getAllDAGIDs();

  @SuppressWarnings("rawtypes")
  EventHandler getEventHandler();

  Clock getClock();

  ClusterInfo getClusterInfo();

  AMContainerMap getAllContainers();

  AMNodeTracker getNodeTracker();

  TaskSchedulerManager getTaskScheduler();

  TaskCommunicatorManagerInterface getTaskCommunicatorManager();

  boolean isSession();

  boolean isLocal();

  DAGAppMasterState getAMState();

  HistoryEventHandler getHistoryHandler();

  Path getCurrentRecoveryDir();

  boolean isRecoveryEnabled();

  ACLManager getAMACLManager();

  String[] getLogDirs();

  String[] getLocalDirs();

  String getAMUser();

  String getQueueName();

  void setQueueName(String queueName);

  /** Whether the AM is in the process of shutting down/completing */
  boolean isAMInCompletionState();

  Credentials getAppCredentials();

  public Integer getTaskCommunicatorIdentifier(String name);
  public Integer getTaskScheduerIdentifier(String name);
  public Integer getContainerLauncherIdentifier(String name);

  public String getTaskCommunicatorName(int taskCommId);
  public String getTaskSchedulerName(int schedulerId);
  public String getContainerLauncherName(int launcherId);

  public String getTaskCommunicatorClassName(int taskCommId);
  public String getTaskSchedulerClassName(int schedulerId);
  public String getContainerLauncherClassName(int launcherId);

  public HadoopShim getHadoopShim();

  public DAGRecoveryData getDAGRecoveryData();
}
