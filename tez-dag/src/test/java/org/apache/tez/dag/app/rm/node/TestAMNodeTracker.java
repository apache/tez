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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.apache.tez.dag.app.rm.AMSchedulerEventNodeBlacklistUpdate;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.dag.app.rm.TaskSchedulerEventHandler;
import org.apache.tez.dag.app.rm.container.AMContainerEventNodeFailed;
import org.apache.tez.dag.app.rm.container.AMContainerEventType;
import org.apache.tez.dag.app.rm.container.AMContainerMap;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

@SuppressWarnings({ "resource", "rawtypes" })
public class TestAMNodeTracker {

  private static final Log LOG = LogFactory.getLog(TestAMNodeTracker.class);

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

    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    amNodeTracker.init(new Configuration(false));
    amNodeTracker.start();

    NodeId nodeId = NodeId.newInstance("host1", 2342);
    amNodeTracker.nodeSeen(nodeId);

    NodeReport nodeReport = generateNodeReport(nodeId, NodeState.UNHEALTHY);
    amNodeTracker.handle(new AMNodeEventStateChanged(nodeReport));
    dispatcher.await();
    assertEquals(AMNodeState.UNHEALTHY, amNodeTracker.get(nodeId).getState());
    amNodeTracker.stop();
  }

  @Test(timeout=5000)
  public void testHealthUpdateUnknownNode() {
    AppContext appContext = mock(AppContext.class);

    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    amNodeTracker.init(new Configuration(false));
    amNodeTracker.start();

    NodeId nodeId = NodeId.newInstance("unknownhost", 2342);

    NodeReport nodeReport = generateNodeReport(nodeId, NodeState.UNHEALTHY);
    amNodeTracker.handle(new AMNodeEventStateChanged(nodeReport));
    dispatcher.await();

    amNodeTracker.stop();
    // No exceptions - the status update was ignored. Not bothering to capture
    // the log message for verification.
  }

  @Test (timeout = 5000)
  public void testSingleNodeNotBlacklisted() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    conf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_IGNORE_THRESHOLD, 33);

    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerEventHandler taskSchedulerEventHandler =
        mock(TaskSchedulerEventHandler.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerEventHandler);
    amNodeTracker.init(conf);
    amNodeTracker.start();

    amNodeTracker.handle(new AMNodeEventNodeCountUpdated(1));
    NodeId nodeId = NodeId.newInstance("host1", 1234);
    amNodeTracker.nodeSeen(nodeId);

    AMNodeImpl node = (AMNodeImpl) amNodeTracker.get(nodeId);

    ContainerId cId1 = mock(ContainerId.class);
    ContainerId cId2 = mock(ContainerId.class);

    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, cId1));
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, cId2));

    TezTaskAttemptID ta1 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta2 = mock(TezTaskAttemptID.class);

    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, cId1, ta1, true));
    dispatcher.await();
    assertEquals(1, node.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node.getState());

    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, cId2, ta2, true));
    dispatcher.await();
    assertEquals(2, node.numFailedTAs);
    assertEquals(1, handler.events.size());
    assertEquals(AMNodeEventType.N_IGNORE_BLACKLISTING_ENABLED, handler.events.get(0).getType());
    assertEquals(AMNodeState.FORCED_ACTIVE, node.getState());
  }

  @Test(timeout=10000)
  public void testNodeSelfBlacklist() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerEventHandler taskSchedulerEventHandler =
        mock(TaskSchedulerEventHandler.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerEventHandler);
    amNodeTracker.init(conf);
    amNodeTracker.start();

    amNodeTracker.handle(new AMNodeEventNodeCountUpdated(4));
    NodeId nodeId = NodeId.newInstance("host1", 1234);
    NodeId nodeId2 = NodeId.newInstance("host2", 1234);
    NodeId nodeId3 = NodeId.newInstance("host3", 1234);
    NodeId nodeId4 = NodeId.newInstance("host4", 1234);
    amNodeTracker.nodeSeen(nodeId);
    amNodeTracker.nodeSeen(nodeId2);
    amNodeTracker.nodeSeen(nodeId3);
    amNodeTracker.nodeSeen(nodeId4);
    AMNodeImpl node = (AMNodeImpl) amNodeTracker.get(nodeId);
    
    ContainerId cId1 = mock(ContainerId.class);
    ContainerId cId2 = mock(ContainerId.class);
    ContainerId cId3 = mock(ContainerId.class);
    
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, cId1));
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, cId2));
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, cId3));
    assertEquals(3, node.containers.size());
    
    TezTaskAttemptID ta1 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta2 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta3 = mock(TezTaskAttemptID.class);
    
    amNodeTracker.handle(new AMNodeEventTaskAttemptSucceeded(nodeId, cId1, ta1));
    assertEquals(1, node.numSuccessfulTAs);
    
    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, cId2, ta2, true));
    assertEquals(1, node.numSuccessfulTAs);
    assertEquals(1, node.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node.getState());
    // duplicate should not affect anything
    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, cId2, ta2, true));
    assertEquals(1, node.numSuccessfulTAs);
    assertEquals(1, node.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node.getState());
    
    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, cId3, ta3, true));
    dispatcher.await();
    assertEquals(1, node.numSuccessfulTAs);
    assertEquals(2, node.numFailedTAs);
    assertEquals(AMNodeState.BLACKLISTED, node.getState());
    
    assertEquals(4, handler.events.size());
    assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(0).getType());
    assertEquals(cId1, ((AMContainerEventNodeFailed)handler.events.get(0)).getContainerId());
    assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(1).getType());
    assertEquals(cId2, ((AMContainerEventNodeFailed)handler.events.get(1)).getContainerId());
    assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(2).getType());
    assertEquals(cId3, ((AMContainerEventNodeFailed)handler.events.get(2)).getContainerId());
    assertEquals(AMSchedulerEventType.S_NODE_BLACKLISTED, handler.events.get(3).getType());
    assertEquals(node.getNodeId(), ((AMSchedulerEventNodeBlacklistUpdate)handler.events.get(3)).getNodeId());


    // Trigger one more node failure, which should cause BLACKLISTING to be disabled
    ContainerId cId4 = mock(ContainerId.class);
    ContainerId cId5 = mock(ContainerId.class);
    TezTaskAttemptID ta4 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta5 = mock(TezTaskAttemptID.class);
    AMNodeImpl node2 = (AMNodeImpl) amNodeTracker.get(nodeId2);
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId2, cId4));
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId2, cId5));
    
    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId2, cId4, ta4, true));
    assertEquals(1, node2.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node2.getState());
    
    handler.events.clear();
    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId2, cId5, ta5, true));
    dispatcher.await();
    assertEquals(2, node2.numFailedTAs);
    assertEquals(AMNodeState.FORCED_ACTIVE, node2.getState());
    AMNodeImpl node3 = (AMNodeImpl) amNodeTracker.get(nodeId3);
    assertEquals(AMNodeState.FORCED_ACTIVE, node3.getState());
    assertEquals(5, handler.events.size());

    // Blacklisting Disabled, the node causing this will not be blacklisted. The single node that
    // was blacklisted will be unblacklisted.
    int numIgnoreBlacklistingEnabledEvents = 0;
    int numUnblacklistedEvents = 0;
    for (Event event : handler.events) {
      if (event.getType() == AMNodeEventType.N_IGNORE_BLACKLISTING_ENABLED) {
        numIgnoreBlacklistingEnabledEvents++;
      } else if (event.getType() == AMSchedulerEventType.S_NODE_UNBLACKLISTED) {
        numUnblacklistedEvents++;
      } else {
        fail("Unexpected event of type: " + event.getType());
      }
    }
    assertEquals(4, numIgnoreBlacklistingEnabledEvents);
    assertEquals(1, numUnblacklistedEvents);

    // drain all previous events
    dispatcher.await();


    // Increase the number of nodes. BLACKLISTING should be re-enabled.
    // Node 1 and Node 2 should go into BLACKLISTED state
    handler.events.clear();
    amNodeTracker.handle(new AMNodeEventNodeCountUpdated(8));
    dispatcher.await();
    LOG.info(("Completed waiting for dispatcher to process all pending events"));
    assertEquals(AMNodeState.BLACKLISTED, node.getState());
    assertEquals(AMNodeState.BLACKLISTED, node2.getState());
    assertEquals(AMNodeState.ACTIVE, node3.getState());
    assertEquals(8, handler.events.size());

    int index = 0;
    int numIgnoreBlacklistingDisabledEvents = 0;
    int numBlacklistedEvents = 0;
    int numNodeFailedEvents = 0;
    for (Event event : handler.events) {
      LOG.info("Logging event: index:" + index++
          + " type: " + event.getType());
      if (event.getType() == AMNodeEventType.N_IGNORE_BLACKLISTING_DISABLED) {
        numIgnoreBlacklistingDisabledEvents++;
      } else if (event.getType() == AMSchedulerEventType.S_NODE_BLACKLISTED) {
        numBlacklistedEvents++;
      } else if (event.getType() == AMContainerEventType.C_NODE_FAILED) {
        numNodeFailedEvents++;
        // Node2 is now blacklisted so the container's will be informed
        assertTrue(((AMContainerEventNodeFailed) event).getContainerId() == cId4 ||
            ((AMContainerEventNodeFailed) event).getContainerId() == cId5);
      } else {
        fail("Unexpected event of type: " + event.getType());
      }
    }
    assertEquals(4, numIgnoreBlacklistingDisabledEvents);
    assertEquals(2, numBlacklistedEvents);
    assertEquals(2, numNodeFailedEvents);
    
    amNodeTracker.stop();
  }

  private static NodeReport generateNodeReport(NodeId nodeId, NodeState nodeState) {
    NodeReport nodeReport = mock(NodeReport.class);
    doReturn(nodeId).when(nodeReport).getNodeId();
    doReturn(nodeState).when(nodeReport).getNodeState();
    String httpAddress = nodeId.getHost() + ":3433";
    doReturn(httpAddress).when(nodeReport).getHttpAddress();
    doReturn("/default-rack").when(nodeReport).getRackName();
    doReturn(Resource.newInstance(0, 0)).when(nodeReport).getUsed();
    doReturn(Resource.newInstance(10240, 12)).when(nodeReport).getCapability();
    doReturn(10).when(nodeReport).getNumContainers();
    doReturn(nodeState.toString()).when(nodeReport).getHealthReport();
    long healthReportTime = System.currentTimeMillis();
    doReturn(healthReportTime).when(nodeReport).getLastHealthReportTime();
    return nodeReport;
  }
}
