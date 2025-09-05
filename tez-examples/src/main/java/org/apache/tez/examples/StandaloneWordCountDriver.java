/*
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

package org.apache.tez.examples;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.client.CallerContext;
import org.apache.tez.client.TezClient;
import org.apache.tez.client.registry.AMRecord;
import org.apache.tez.client.registry.zookeeper.ZkAMRegistryClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.client.StatusGetOpts;

import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
public abstract class StandaloneWordCountDriver {

  private static final Logger LOG = LoggerFactory.getLogger(StandaloneWordCountDriver.class);

  private static TezClient tezClientInternal;

  public static void main(String[] args) throws Exception {
    ExampleBase clazz = new StandaloneWordCount();
    _execute(clazz, args, null, null);
  }

  private static int _execute(ExampleBase clazz, String[] otherArgs, TezConfiguration tezConf, TezClient tezClient) throws
          Exception {
    tezConf = new TezConfiguration();
    tezConf.set(TezConfiguration.TEZ_AM_ZOOKEEPER_QUORUM, "localhost:2181");
    ZkAMRegistryClient registryClientZk = ZkAMRegistryClient.getClient(tezConf);
    registryClientZk.start();
    List<AMRecord> sessions = registryClientZk.getAllRecords();
    Collections.shuffle(sessions);
    AMRecord am = sessions.get(0);
    tezConf.set(TezConfiguration.TEZ_FRAMEWORK_MODE, "STANDALONE_ZOOKEEPER");
    tezConf.setBoolean(TezConfiguration.TEZ_IGNORE_LIB_URIS, true);
    tezClientInternal = createTezClient(am.getApplicationId().toString(), tezConf);
    StandaloneWordCount standaloneWordCount = new StandaloneWordCount();
    return standaloneWordCount.runJob(otherArgs, tezConf, tezClientInternal);
  }

  public static int runDag(DAG dag, ApplicationId appId, Logger logger) throws Exception {
    //tezClientInternal.waitTillReady();

    CallerContext callerContext = CallerContext.create("TezExamples",
        "Tez Example DAG: " + dag.getName());

    if (appId != null) {
      callerContext.setCallerIdAndType(appId.toString(), "TezExampleApplication");
    }
    dag.setCallerContext(callerContext);

    DAGClient dagClient = tezClientInternal.submitDAG(dag);
    Set<StatusGetOpts> getOpts = Sets.newHashSet();
    getOpts.add(StatusGetOpts.GET_COUNTERS);

    DAGStatus dagStatus;
    dagStatus = dagClient.waitForCompletionWithStatusUpdates(getOpts);

    if (dagStatus.getState() != DAGStatus.State.SUCCEEDED) {
      logger.info("DAG diagnostics: " + dagStatus.getDiagnostics());
      return -1;
    }
    return 0;
  }

  private static TezClient createTezClient(String appId, TezConfiguration tezConf) throws IOException, TezException {
    Credentials credentials = new Credentials();
    Token<TokenIdentifier> token = new Token<TokenIdentifier>();
    credentials.addToken(new Text("root"), token);
    TezClient tezClient = TezClient.create("TezExampleApplication", tezConf, true, null, credentials);
    return tezClient.getClient(appId);
  }

}
