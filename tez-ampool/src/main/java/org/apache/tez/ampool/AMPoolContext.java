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

package org.apache.tez.ampool;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationRequest;
import org.apache.hadoop.yarn.api.protocolrecords.SubmitApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.AMRMClient;
import org.apache.hadoop.yarn.client.YarnClient;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.ampool.manager.AMPoolManager;

public class AMPoolContext {

  private final Dispatcher dispatcher;

  private final YarnClient rmYarnClient;

  private final AMRMClient amRMClient;

  private final AMPoolManager amPoolManager;

  private Resource minResourceCapability;
  private Resource maxResourceCapability;

  private final String nmHost;

  public AMPoolContext(Configuration conf, String nmHost,
      Dispatcher dispatcher, YarnClient yarnClient, AMRMClient amRMClient,
      AMPoolManager amPoolManager) {
    // TODO
    this.dispatcher = dispatcher;
    this.rmYarnClient = yarnClient;
    this.amRMClient = amRMClient;
    this.amPoolManager = amPoolManager;
    this.nmHost = nmHost;
  }

  public ApplicationId getNewApplicationId() {
    return amPoolManager.getAM();
  }

  public YarnClient getRMYarnClient() {
    return rmYarnClient;
  }

  public AMContext getAMContext(ApplicationId applicationId) {
    return amPoolManager.getAMContext(applicationId);
  }

  public SubmitApplicationResponse submitApplication(
      SubmitApplicationRequest request) {
    amPoolManager.submitApplication(request);
    SubmitApplicationResponse response =
        Records.newRecord(SubmitApplicationResponse.class);
    return response;
  }

  public AMRMClient getAMRMClient() {
    return amRMClient;
  }

  public synchronized Resource getMaxResourceCapability() {
    return maxResourceCapability;
  }

  public synchronized Resource getMinResourceCapability() {
    return minResourceCapability;
  }

  public synchronized void setMaxResourceCapability(Resource resource) {
    maxResourceCapability = resource;
  }

  public synchronized void setMinResourceCapability(Resource resource) {
    minResourceCapability = resource;
  }

  public Dispatcher getDispatcher() {
    return dispatcher;
  }

  public boolean isManagedApp(ApplicationId applicationId) {
    return amPoolManager.isManagedApp(applicationId);
  }

  public String getNMHost() {
    return nmHost;
  }

}
