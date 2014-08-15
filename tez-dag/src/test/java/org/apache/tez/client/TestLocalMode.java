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

package org.apache.tez.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import junit.framework.Assert;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.app.dag.DAGState;
import org.apache.tez.runtime.library.processor.SleepProcessor;
import org.junit.Test;

public class TestLocalMode {

  @Test
  public void testMultipleClientsWithSession() throws TezException, InterruptedException,
      IOException {
    TezConfiguration tezConf1 = new TezConfiguration();
    tezConf1.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf1.set("fs.defaultFS", "file:///");
    TezClient tezClient1 = new TezClient("commonName", tezConf1, true);
    tezClient1.start();

    DAG dag1 = createSimpleSleepDAG("dag1");

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient1.getDAGStatus(null).getState());

    dagClient1.close();
    tezClient1.stop();


    TezConfiguration tezConf2 = new TezConfiguration();
    tezConf2.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf2.set("fs.defaultFS", "file:///");
    DAG dag2 = createSimpleSleepDAG("dag2");
    TezClient tezClient2 = new TezClient("commonName", tezConf2, true);
    tezClient2.start();
    DAGClient dagClient2 = tezClient2.submitDAG(dag2);
    dagClient2.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient2.getDAGStatus(null).getState());
    assertFalse(dagClient1.getExecutionContext().equals(dagClient2.getExecutionContext()));
    dagClient2.close();
    tezClient2.stop();
  }

  @Test
  public void testMultipleClientsWithoutSession() throws TezException, InterruptedException,
      IOException {
    TezConfiguration tezConf1 = new TezConfiguration();
    tezConf1.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf1.set("fs.defaultFS", "file:///");
    TezClient tezClient1 = new TezClient("commonName", tezConf1, false);
    tezClient1.start();

    DAG dag1 = createSimpleSleepDAG("dag1");

    DAGClient dagClient1 = tezClient1.submitDAG(dag1);
    dagClient1.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient1.getDAGStatus(null).getState());

    dagClient1.close();
    tezClient1.stop();


    TezConfiguration tezConf2 = new TezConfiguration();
    tezConf2.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf2.set("fs.defaultFS", "file:///");
    DAG dag2 = createSimpleSleepDAG("dag2");
    TezClient tezClient2 = new TezClient("commonName", tezConf2, false);
    tezClient2.start();
    DAGClient dagClient2 = tezClient2.submitDAG(dag2);
    dagClient2.waitForCompletion();
    assertEquals(DAGStatus.State.SUCCEEDED, dagClient2.getDAGStatus(null).getState());
    assertFalse(dagClient1.getExecutionContext().equals(dagClient2.getExecutionContext()));
    dagClient2.close();
    tezClient2.stop();
  }

  private DAG createSimpleSleepDAG(String dagName) {
    DAG dag = new DAG(dagName).addVertex(new Vertex("Sleep", new ProcessorDescriptor(
        SleepProcessor.class.getName()).setUserPayload(
        new UserPayload(new SleepProcessor.SleepProcessorConfig(1).toUserPayload())), 1));
    return dag;

  }
}
