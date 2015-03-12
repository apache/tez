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
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;

@Private
public abstract class FrameworkClient {

  public static FrameworkClient createFrameworkClient(TezConfiguration tezConf) {

    boolean isLocal = tezConf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE, TezConfiguration.TEZ_LOCAL_MODE_DEFAULT);
    if (isLocal) {
      return ReflectionUtils.createClazzInstance("org.apache.tez.client.LocalClient");
    }
    return new TezYarnClient(YarnClient.createYarnClient());
  }

  /**
   * Initialize the framework client. </p>
   * <p/>
   * The actual implementation of FramworkClient may modify the configuration instances that are
   * passed in to configure required functionality
   *
   * @param tezConf  the {@link org.apache.tez.dag.api.TezConfiguration} instance being used by the
   *                 cluster
   * @param yarnConf the {@link org.apache.hadoop.yarn.conf.YarnConfiguration} instance being used
   *                 by the cluster
   */
  public abstract void init(TezConfiguration tezConf, YarnConfiguration yarnConf);

  public abstract void start();

  public abstract void stop();

  public abstract void close() throws IOException;

  public abstract YarnClientApplication createApplication() throws YarnException, IOException;

  public abstract ApplicationId submitApplication(ApplicationSubmissionContext appSubmissionContext)
      throws YarnException, IOException, TezException;

  public abstract void killApplication(ApplicationId appId) throws YarnException, IOException;

  public abstract ApplicationReport getApplicationReport(ApplicationId appId) throws YarnException, IOException;

}
