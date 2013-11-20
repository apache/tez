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

package org.apache.tez.dag.app.rm.node;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.app.AppContext;
import org.junit.Test;

public class TestAMNodeMap {

  @Test
  @SuppressWarnings({ "resource", "rawtypes" })
  public void testHealthUpdateKnownNode() {

    DrainDispatcher dispatcher = new DrainDispatcher();
    dispatcher.init(new Configuration());
    dispatcher.start();
    EventHandler eventHandler = dispatcher.getEventHandler();

    AppContext appContext = mock(AppContext.class);

    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);

    NodeId nodeId = NodeId.newInstance("host1", 2342);
    amNodeMap.nodeSeen(nodeId);

    NodeReport nodeReport = generateNodeReport(nodeId, NodeState.UNHEALTHY);
    amNodeMap.handle(new AMNodeEventStateChanged(nodeReport));
    dispatcher.await();
    assertEquals(AMNodeState.UNHEALTHY, amNodeMap.get(nodeId).getState());
    dispatcher.stop();
  }

  @Test
  @SuppressWarnings({ "resource", "rawtypes" })
  public void testHealthUpdateUnknownNode() {
    DrainDispatcher dispatcher = new DrainDispatcher();
    EventHandler eventHandler = dispatcher.getEventHandler();

    AppContext appContext = mock(AppContext.class);

    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);

    NodeId nodeId = NodeId.newInstance("unknownhost", 2342);

    NodeReport nodeReport = generateNodeReport(nodeId, NodeState.UNHEALTHY);
    amNodeMap.handle(new AMNodeEventStateChanged(nodeReport));

    // No exceptions - the status update was ignored. Not bothering to capture
    // the log message for verification.
    dispatcher.stop();
  }

  private static NodeReport generateNodeReport(NodeId nodeId, NodeState nodeState) {
    NodeReport nodeReport = NodeReport.newInstance(nodeId, nodeState, nodeId.getHost() + ":3433",
        "/default-rack", Resource.newInstance(0, 0), Resource.newInstance(10240, 12), 10,
        nodeState.toString(), System.currentTimeMillis());
    return nodeReport;
  }
}
