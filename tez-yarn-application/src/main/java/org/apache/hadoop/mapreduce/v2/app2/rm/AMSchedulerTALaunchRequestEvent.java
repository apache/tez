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

package org.apache.hadoop.mapreduce.v2.app2.rm;

import org.apache.hadoop.mapreduce.security.token.JobTokenIdentifier;
import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app2.job.TaskAttempt;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.mapreduce.task.impl.MRTaskContext;

public class AMSchedulerTALaunchRequestEvent extends AMSchedulerEvent {

  // TODO Get rid of remoteTask from here. Can be forgottent after it has been assigned.
  //.... Maybe have the Container talk to the TaskAttempt to pull in the remote task.
  
  private final TaskAttemptId attemptId;
  private final boolean rescheduled;
  private final Resource capability;
  private final MRTaskContext remoteTaskContext;
  private final TaskAttempt taskAttempt;
  private final Credentials credentials;
  private Token<JobTokenIdentifier> jobToken;
  private final String[] hosts;
  private final String[] racks;
  
  
  public AMSchedulerTALaunchRequestEvent(TaskAttemptId attemptId,
      boolean rescheduled, Resource capability,
      MRTaskContext remoteTaskContext, TaskAttempt ta,
      Credentials credentials, Token<JobTokenIdentifier> jobToken,
      String[] hosts, String[] racks) {
    super(AMSchedulerEventType.S_TA_LAUNCH_REQUEST);
    this.attemptId = attemptId;
    this.rescheduled = rescheduled;
    this.capability = capability;
    this.remoteTaskContext = remoteTaskContext;
    this.taskAttempt = ta;
    this.credentials = credentials;
    this.jobToken = jobToken;
    this.hosts = hosts;
    this.racks = racks;
  }

  public TaskAttemptId getAttemptID() {
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
  
  public boolean isRescheduled() {
    return rescheduled;
  }
  
  public MRTaskContext getRemoteTaskContext() {
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
