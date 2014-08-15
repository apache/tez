/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.tez.client;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.MRDAGClient;

@InterfaceAudience.Private
public class MRTezClient extends TezClient {
  public MRTezClient(String name, TezConfiguration tezConf, boolean isSession,
                     @Nullable Map<String, LocalResource> localResources,
                     @Nullable Credentials credentials) {
    super(name, tezConf, isSession, localResources, credentials);
  }

  // To be used only by YarnRunner
  public DAGClient submitDAGApplication(ApplicationId appId, org.apache.tez.dag.api.DAG dag)
      throws TezException, IOException {
    return super.submitDAGApplication(appId, dag);
  }

  public static MRDAGClient getDAGClient(ApplicationId appId, TezConfiguration tezConf, FrameworkClient frameworkClient)
      throws IOException, TezException {
    return new MRDAGClient(TezClient.getDAGClient(appId, tezConf, frameworkClient));
  }
}
