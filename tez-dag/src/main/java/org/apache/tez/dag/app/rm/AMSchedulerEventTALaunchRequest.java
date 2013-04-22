/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.dag.app.rm;

import java.util.Map;

import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezTaskContext;
import org.apache.tez.dag.app.dag.TaskAttempt;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.apache.tez.engine.records.TezTaskAttemptID;

public class AMSchedulerEventTALaunchRequest extends AMSchedulerEvent {

  // TODO Get rid of remoteTask from here. Can be forgottent after it has been assigned.
  //.... Maybe have the Container talk to the TaskAttempt to pull in the remote task.
  
  private final TezTaskAttemptID attemptId;
  private final Resource capability;
  private final Map<String, LocalResource> localResources;
  private final TezTaskContext remoteTaskContext;
  private final TaskAttempt taskAttempt;
  private final Credentials credentials;
  private Token<JobTokenIdentifier> jobToken;
  private final String[] hosts;
  private final String[] racks;
  private final Priority priority;
  private final Map<String, String> environment;
  
  
  public AMSchedulerEventTALaunchRequest(TezTaskAttemptID attemptId,
      Resource capability,
      Map<String, LocalResource> localResources,
      TezTaskContext remoteTaskContext, TaskAttempt ta,
      Credentials credentials, Token<JobTokenIdentifier> jobToken,
      String[] hosts, String[] racks, Priority priority,
      Map<String, String> environment) {
    super(AMSchedulerEventType.S_TA_LAUNCH_REQUEST);
    this.attemptId = attemptId;
    this.capability = capability;
    this.localResources = localResources;
    this.remoteTaskContext = remoteTaskContext;
    this.taskAttempt = ta;
    this.credentials = credentials;
    this.jobToken = jobToken;
    this.hosts = hosts;
    this.racks = racks;
    this.priority = priority;
    this.environment = environment;
  }

  public TezTaskAttemptID getAttemptID() {
    return this.attemptId;
  }

  public Resource getCapability() {
    return capability;
  }

  public String[] getHosts() {
    return hosts;
  }
  
  public String[] getRacks() {
    return racks;
  }
  
  public Priority getPriority() {
    return priority;
  }
  
  public TezTaskContext getRemoteTaskContext() {
    return remoteTaskContext;
  }
  
  public TaskAttempt getTaskAttempt() {
    return this.taskAttempt;
  }
  
  public Credentials getCredentials() {
    return this.credentials;
  }
  
  public Token<JobTokenIdentifier> getJobToken() {
    return this.jobToken;
  }

  public Map<String, LocalResource> getLocalResources() {
    return this.localResources;
  }
  
  public Map<String, String> getEnvironment() {
    return this.environment;
  }

  // Parameter replacement: @taskid@ will not be usable
  // ProfileTaskRange not available along with ContainerReUse

  /*Requirements to determine a container request.
   * + Data-local + Rack-local hosts.
   * + Resource capability
   * + Env - mapreduce.map.env / mapreduce.reduce.env can change. M/R log level. 
   * - JobConf and JobJar file - same location.
   * - Distributed Cache - identical for map / reduce tasks at the moment.
   * - Credentials, tokens etc are identical.
   * + Command - dependent on map / reduce java.opts
   */
}