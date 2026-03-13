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

package org.apache.tez.client.registry.zookeeper;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;

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

import com.google.common.annotations.VisibleForTesting;

public class ZkFrameworkClient extends FrameworkClient {

  private volatile AMRecord amRecord;
  private ZkAMRegistryClient amRegistryClient = null;
  private volatile boolean isRunning = false;

  @Override
  public synchronized void init(TezConfiguration tezConf) {
    if (amRegistryClient == null) {
      try {
        amRegistryClient = ZkAMRegistryClient.getClient(tezConf);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void start() {
    if (isRunning) {
      return;
    }
    try {
      amRegistryClient.start();
      isRunning = true;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    isRunning = false;
    close();
  }

  @Override
  public void close() {
    if (amRegistryClient != null) {
      amRegistryClient.close();
    }
  }

  /**
   * Creates a dummy {@link YarnClientApplication} using a pre-existing {@link ApplicationId}
   * rather than requesting a new one from the ResourceManager.
   *
   * <p><strong>Note:</strong> This is a <em>dummy, backward-compatibility implementation</em>.
   * Instead of allocating a fresh application ID from the ResourceManager, this method
   * reuses the {@code applicationId} already obtained via {@code getApplicationReport()}.
   * This allows legacy code paths to continue operating without requiring actual
   * creation of a new application.</p>
   *
   * <p>Hidden assumption here: this method assumes that
   * {@code getApplicationReport()} has already been called before
   * {@code createApplication()}, ensuring that {@code amRecord.getApplicationId()}
   * is always available. This assumption holds in all supported usage patterns:
   * the only code path where {@code createApplication()} might be called first is
   * {@code TezClient.submitDAGApplication()}, but that path is never exercised in
   * Zookeeper standalone mode because that mode assumes applications are already
   * running. Therefore, the ordering guarantee is valid in practice.</p>
   *
   * <p>
   * The method constructs a minimal {@link ApplicationSubmissionContext} and a
   * synthetic {@link GetNewApplicationResponse}, both populated with the already
   * known application ID. These objects are then wrapped into a
   * {@link YarnClientApplication} instance and returned.
   * </p>
   *
   * @return a {@link YarnClientApplication} backed by a submission context and
   *         a mocked {@link GetNewApplicationResponse}, both tied to the pre-existing
   *         application ID.
   */
  @Override
  public YarnClientApplication createApplication() {
    if (amRecord == null) {
      long startTime = System.currentTimeMillis();
      while (!isZkInitialized() && (System.currentTimeMillis() - startTime) < 5000) {
        try {
          TimeUnit.MILLISECONDS.sleep(1000);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new RuntimeException("Interrupted while waiting for ZK registry sync", e);
        }
      }

      List<AMRecord> records = amRegistryClient.getAllRecords();
      if (records != null && !records.isEmpty()) {
        amRecord = records.getFirst();
        LOG.info(
            "No AppId provided, discovered AM from Zookeeper: {}", amRecord.getApplicationId());
      } else {
        throw new RuntimeException(
            "No AM record found in Zookeeper. Ensure the AM is running and registered.");
      }
    }

    ApplicationSubmissionContext context = Records.newRecord(ApplicationSubmissionContext.class);
    ApplicationId appId = amRecord.getApplicationId();
    context.setApplicationId(appId);
    GetNewApplicationResponse response = Records.newRecord(GetNewApplicationResponse.class);
    response.setApplicationId(appId);
    return new YarnClientApplication(response, context);
  }

  @Override
  public ApplicationId submitApplication(ApplicationSubmissionContext appSubmissionContext) {
    return null;
  }

  @Override
  public void killApplication(ApplicationId appId) throws YarnException, IOException {
    close();
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException {
    ApplicationReport report = Records.newRecord(ApplicationReport.class);
    report.setApplicationId(appId);
    report.setTrackingUrl("");
    amRecord = amRegistryClient.getRecord(appId);
    // this could happen if the AM died, the AM record store under path will not exist
    if (amRecord == null) {
      report.setYarnApplicationState(YarnApplicationState.FINISHED);
      report.setFinalApplicationStatus(FinalApplicationStatus.FAILED);
      report.setDiagnostics("AM record not found (likely died) in zookeeper for application id: " + appId);
    } else {
      report.setHost(amRecord.getHostName());
      report.setRpcPort(amRecord.getPort());
      report.setYarnApplicationState(YarnApplicationState.RUNNING);
    }
    return report;
  }

  @Override
  public boolean isRunning() {
    return isRunning;
  }

  @Override
  public String getAmHost() {
    return amRecord == null ? null : amRecord.getHostName();
  }

  @Override
  public int getAmPort() {
    return amRecord == null ? 0 : amRecord.getPort();
  }

  @VisibleForTesting
  boolean isZkInitialized() {
    return amRegistryClient.isInitialized();
  }
}
