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

package org.apache.tez.client;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;

@Private
public class TezYarnClient extends FrameworkClient {

  private final YarnClient yarnClient;

  protected TezYarnClient(YarnClient yarnClient) {
    this.yarnClient = yarnClient;
  }

  @Override
  public void init(TezConfiguration tezConf, YarnConfiguration yarnConf) {
    yarnClient.init(yarnConf);
  }

  @Override
  public void start() {
    yarnClient.start();
  }

  @Override
  public void stop() {
    yarnClient.stop();
  }

  @Override
  public final void close() throws IOException {
    yarnClient.close();
  }

  @Override
  public YarnClientApplication createApplication() throws YarnException, IOException {
    return yarnClient.createApplication();
  }

  @Override
  public ApplicationId submitApplication(ApplicationSubmissionContext appSubmissionContext)
      throws YarnException, IOException, TezException {
	ApplicationId appId= yarnClient.submitApplication(appSubmissionContext);
    ApplicationReport appReport = getApplicationReport(appId);
    if (appReport.getYarnApplicationState() == YarnApplicationState.FAILED){
      throw new TezException("Failed to submit application to YARN"
          + ", applicationId=" + appId
          + ", diagnostics=" + appReport.getDiagnostics());
    }
    return appId;
  }

  @Override
  public void killApplication(ApplicationId appId) throws YarnException, IOException {
    yarnClient.killApplication(appId);
  }

  @Override
  public ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException {
    return yarnClient.getApplicationReport(appId);
  }
}
