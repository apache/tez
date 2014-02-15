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

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.DrainDispatcher;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.rm.AMSchedulerEventNodeBlacklisted;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerEvent;
import org.apache.tez.dag.app.rm.container.AMContainerEventNodeFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

@SuppressWarnings({ "resource", "rawtypes" })
public class TestAMNodeMap {

  DrainDispatcher dispatcher;
  EventHandler eventHandler;
  
  @Before
  public void setup() {
    dispatcher = new DrainDispatcher();
    dispatcher.init(new Configuration());
    dispatcher.start();
    eventHandler = dispatcher.getEventHandler();
  }
  
  class TestEventHandler implements EventHandler{
    List<Event> events = Lists.newLinkedList();
    @SuppressWarnings("unchecked")
    @Override
    public void handle(Event event) {
      events.add(event);
      eventHandler.handle(event);
    }
  }
  
  @After
  public void teardown() {
    dispatcher.stop();
  }
  
  @Test(timeout=5000)
  public void testHealthUpdateKnownNode() {
    AppContext appContext = mock(AppContext.class);

    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    amNodeMap.init(new Configuration(false));
    amNodeMap.start();

    NodeId nodeId = NodeId.newInstance("host1", 2342);
    amNodeMap.nodeSeen(nodeId);

    NodeReport nodeReport = generateNodeReport(nodeId, NodeState.UNHEALTHY);
    amNodeMap.handle(new AMNodeEventStateChanged(nodeReport));
    dispatcher.await();
    assertEquals(AMNodeState.UNHEALTHY, amNodeMap.get(nodeId).getState());
    amNodeMap.stop();
  }

  @Test(timeout=5000)
  public void testHealthUpdateUnknownNode() {
    AppContext appContext = mock(AppContext.class);

    AMNodeMap amNodeMap = new AMNodeMap(eventHandler, appContext);
    amNodeMap.init(new Configuration(false));
    amNodeMap.start();

    NodeId nodeId = NodeId.newInstance("unknownhost", 2342);

    NodeReport nodeReport = generateNodeReport(nodeId, NodeState.UNHEALTHY);
    amNodeMap.handle(new AMNodeEventStateChanged(nodeReport));
    dispatcher.await();

    amNodeMap.stop();
    // No exceptions - the status update was ignored. Not bothering to capture
    // the log message for verification.
  }
  
  @Test(timeout=5000)
  public void testNodeSelfBlacklist() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    TestEventHandler handler = new TestEventHandler();
    AMNodeMap amNodeMap = new AMNodeMap(handler, appContext);
    amNodeMap.init(conf);
    amNodeMap.start();

    NodeId nodeId = NodeId.newInstance("host1", 1234);
    amNodeMap.nodeSeen(nodeId);
    AMNodeImpl node = (AMNodeImpl) amNodeMap.get(nodeId);
    
    ContainerId cId1 = mock(ContainerId.class);
    ContainerId cId2 = mock(ContainerId.class);
    ContainerId cId3 = mock(ContainerId.class);
    
    amNodeMap.handle(new AMNodeEventContainerAllocated(nodeId, cId1));
    amNodeMap.handle(new AMNodeEventContainerAllocated(nodeId, cId2));
    amNodeMap.handle(new AMNodeEventContainerAllocated(nodeId, cId3));
    assertEquals(3, node.containers.size());
    
    TezTaskAttemptID ta1 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta2 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta3 = mock(TezTaskAttemptID.class);
    
    amNodeMap.handle(new AMNodeEventTaskAttemptSucceeded(nodeId, cId1, ta1));
    assertEquals(1, node.numSuccessfulTAs);
    
    amNodeMap.handle(new AMNodeEventTaskAttemptEnded(nodeId, cId2, ta2, true));
    assertEquals(1, node.numSuccessfulTAs);
    assertEquals(1, node.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node.getState());
    // duplicate should not affect anything
    amNodeMap.handle(new AMNodeEventTaskAttemptEnded(nodeId, cId2, ta2, true));
    assertEquals(1, node.numSuccessfulTAs);
    assertEquals(1, node.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node.getState());
    
    amNodeMap.handle(new AMNodeEventTaskAttemptEnded(nodeId, cId2, ta3, true));
    assertEquals(1, node.numSuccessfulTAs);
    assertEquals(2, node.numFailedTAs);
    assertEquals(AMNodeState.BLACKLISTED, node.getState());
    
    assertEquals(5, handler.events.size());
    assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(0).getType());
    assertEquals(cId1, ((AMContainerEventNodeFailed)handler.events.get(0)).getContainerId());
    assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(1).getType());
    assertEquals(cId2, ((AMContainerEventNodeFailed)handler.events.get(1)).getContainerId());
    assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(2).getType());
    assertEquals(cId3, ((AMContainerEventNodeFailed)handler.events.get(2)).getContainerId());
    assertEquals(AMNodeEventType.N_NODE_WAS_BLACKLISTED, handler.events.get(3).getType());
    assertEquals(node.getNodeId(), ((AMNodeEvent)handler.events.get(3)).getNodeId());
    assertEquals(AMSchedulerEventType.S_NODE_BLACKLISTED, handler.events.get(4).getType());
    assertEquals(node.getNodeId(), ((AMSchedulerEventNodeBlacklisted)handler.events.get(4)).getNodeId());
    amNodeMap.stop();
  }

  private static NodeReport generateNodeReport(NodeId nodeId, NodeState nodeState) {
    NodeReport nodeReport = NodeReport.newInstance(nodeId, nodeState, nodeId.getHost() + ":3433",
        "/default-rack", Resource.newInstance(0, 0), Resource.newInstance(10240, 12), 10,
        nodeState.toString(), System.currentTimeMillis());
    return nodeReport;
  }
}
