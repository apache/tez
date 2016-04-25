/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.dag.impl;


public class ServicePluginInfo {

  private String containerLauncherName;
  private String taskSchedulerName;
  private String taskCommunicatorName;
  private String containerLauncherClassName;
  private String taskSchedulerClassName;
  private String taskCommunicatorClassName;

  public ServicePluginInfo() {
  }

  public String getContainerLauncherName() {
    return containerLauncherName;
  }

  public ServicePluginInfo setContainerLauncherName(String containerLauncherName) {
    this.containerLauncherName = containerLauncherName;
    return this;
  }

  public String getTaskSchedulerName() {
    return taskSchedulerName;
  }

  public ServicePluginInfo setTaskSchedulerName(String taskSchedulerName) {
    this.taskSchedulerName = taskSchedulerName;
    return this;
  }

  public String getTaskCommunicatorName() {
    return taskCommunicatorName;
  }

  public ServicePluginInfo setTaskCommunicatorName(String taskCommunicatorName) {
    this.taskCommunicatorName = taskCommunicatorName;
    return this;
  }

  public String getContainerLauncherClassName() {
    return containerLauncherClassName;
  }

  public ServicePluginInfo setContainerLauncherClassName(String containerLauncherClassName) {
    this.containerLauncherClassName = containerLauncherClassName;
    return this;
  }

  public String getTaskSchedulerClassName() {
    return taskSchedulerClassName;
  }

  public ServicePluginInfo setTaskSchedulerClassName(String taskSchedulerClassName) {
    this.taskSchedulerClassName = taskSchedulerClassName;
    return this;
  }

  public String getTaskCommunicatorClassName() {
    return taskCommunicatorClassName;
  }

  public ServicePluginInfo setTaskCommunicatorClassName(String taskCommunicatorClassName) {
    this.taskCommunicatorClassName = taskCommunicatorClassName;
    return this;
  }

  @Override
  public String toString() {
    return "ServicePluginInfo {" +
        "containerLauncherName=" + containerLauncherName +
        ", taskSchedulerName=" + taskSchedulerName +
        ", taskCommunicatorName=" + taskCommunicatorName +
        ", containerLauncherClassName=" + containerLauncherClassName +
        ", taskSchedulerClassName=" + taskSchedulerClassName +
        ", taskCommunicatorClassName=" + taskCommunicatorClassName +
        " }";
  }
}
