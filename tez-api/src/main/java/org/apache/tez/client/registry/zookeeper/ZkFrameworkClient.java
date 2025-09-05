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

package org.apache.tez.client.registry.zookeeper;

import java.io.IOException;

import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Records;
import org.apache.tez.client.FrameworkClient;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;

public class ZkFrameworkClient extends FrameworkClient {

  private AMRecord amRecord;
  private TezConfiguration tezConf;
  private ZkAMRegistryClient amRegistryClient = null;
  private boolean isRunning = false;
  private String amHost;
  private int amPort;

  @Override
  public synchronized void init(TezConfiguration tezConf) {
    this.tezConf = tezConf;
    if (this.amRegistryClient == null) {
      try {
        this.amRegistryClient = ZkAMRegistryClient.getClient(tezConf);
        this.isRunning = true;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override public void start() {
    try {
      this.amRegistryClient.start();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    isRunning = true;
  }

  @Override public void stop() {
    isRunning = false;
    amRegistryClient.close();
  }

  @Override public void close() throws IOException {
    amRegistryClient.close();
  }

  @Override public YarnClientApplication createApplication() throws YarnException, IOException {
    ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
    ApplicationId appId = amRecord.getApplicationId();
    context.setApplicationId(appId);
    GetNewApplicationResponse response = Records.newRecord(GetNewApplicationResponse.class);
    response.setApplicationId(appId);
    return new YarnClientApplication(response, context);
  }

  @Override public ApplicationId submitApplication(ApplicationSubmissionContext appSubmissionContext)
      throws YarnException, IOException, TezException {
    //Unused
    return null;
  }

  @Override public void killApplication(ApplicationId appId) throws YarnException, IOException {
    amRegistryClient.close();
  }

  @Override public ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setApplicationId(appId);
    report.setTrackingUrl("");
    amRecord = amRegistryClient.getRecord(appId.toString());
    // this could happen if the AM died, the AM record store under path will not exist
    if (amRecord == null) {
      report.setYarnApplicationState(YarnApplicationState.FINISHED);
      report.setFinalApplicationStatus(FinalApplicationStatus.FAILED);
      report.setDiagnostics("AM record not found (likely died) in zookeeper for application id: " + appId);
    } else {
      report.setHost(amRecord.getHost());
      amHost = amRecord.getHost();
      amPort = amRecord.getPort();
      report.setRpcPort(amRecord.getPort());
      report.setYarnApplicationState(YarnApplicationState.RUNNING);
    }
    return report;
  }

  @Override public boolean isRunning() throws IOException {
    return isRunning;
  }

  @Override
  public String getAmHost() {
    return amHost;
  }

  @Override
  public int getAmPort() {
    return amPort;
  }
}
