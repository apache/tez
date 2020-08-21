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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.apache.tez.dag.app.dag.DAG;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.DrainDispatcher;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.rm.AMSchedulerEventNodeBlocklistUpdate;
import org.apache.tez.dag.app.rm.AMSchedulerEventType;
import org.apache.tez.dag.app.rm.TaskSchedulerManager;
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

  private static final Logger LOG = LoggerFactory.getLogger(TestAMNodeTracker.class);

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
    amNodeTracker.nodeSeen(nodeId, 0);

    NodeReport nodeReport = generateNodeReport(nodeId, NodeState.UNHEALTHY);
    amNodeTracker.handle(new AMNodeEventStateChanged(nodeReport, 0));
    dispatcher.await();
    assertEquals(AMNodeState.UNHEALTHY, amNodeTracker.get(nodeId, 0).getState());
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
    amNodeTracker.handle(new AMNodeEventStateChanged(nodeReport, 0));
    dispatcher.await();

    amNodeTracker.stop();
    // No exceptions - the status update was ignored. Not bothering to capture
    // the log message for verification.
  }

  @Test (timeout = 5000)
  public void testMultipleSourcesNodeRegistration() {
    AppContext appContext = mock(AppContext.class);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();

    amNodeTracker.init(new Configuration(false));
    amNodeTracker.start();

    NodeId nodeId1 = NodeId.newInstance("source01", 3333);
    NodeId nodeId2 = NodeId.newInstance("source02", 3333);

    amNodeTracker.nodeSeen(nodeId1, 0);
    amNodeTracker.nodeSeen(nodeId2, 1);

    assertEquals(1, amNodeTracker.getNumNodes(0));
    assertEquals(1, amNodeTracker.getNumNodes(1));
    assertNotNull(amNodeTracker.get(nodeId1, 0));
    assertNull(amNodeTracker.get(nodeId2, 0));
    assertNull(amNodeTracker.get(nodeId1, 1));
    assertNotNull(amNodeTracker.get(nodeId2, 1));
  }

  @Test (timeout = 5000)
  public void testMultipleSourcesNodeCountUpdated() {
    AppContext appContext = mock(AppContext.class);
    AMNodeTracker amNodeTracker = new AMNodeTracker(eventHandler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();

    amNodeTracker.init(new Configuration(false));
    amNodeTracker.start();

    NodeId nodeId1 = NodeId.newInstance("source01", 3333);
    NodeId nodeId2 = NodeId.newInstance("source02", 3333);

    amNodeTracker.nodeSeen(nodeId1, 0);
    amNodeTracker.nodeSeen(nodeId2, 1);
    amNodeTracker.handle(new AMNodeEventNodeCountUpdated(10, 0));
    amNodeTracker.handle(new AMNodeEventNodeCountUpdated(20, 1));

    // NodeCountUpdate does not reflect in getNumNodes.
    assertEquals(1, amNodeTracker.getNumNodes(0));
    assertEquals(1, amNodeTracker.getNumNodes(1));
    assertNotNull(amNodeTracker.get(nodeId1, 0));
    assertNull(amNodeTracker.get(nodeId2, 0));
    assertNull(amNodeTracker.get(nodeId1, 1));
    assertNotNull(amNodeTracker.get(nodeId2, 1));
  }

  @Test (timeout = 5000)
  public void testSingleNodeNotBlocklisted() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    conf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_IGNORE_THRESHOLD, 33);

    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerManager taskSchedulerManager =
        mock(TaskSchedulerManager.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerManager);
    amNodeTracker.init(conf);
    amNodeTracker.start();

    _testSingleNodeNotBlocklisted(amNodeTracker, handler, 0);
  }

  @Test (timeout = 5000)
  public void testSingleNodeNotBlocklistedAlternateScheduler() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    conf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_IGNORE_THRESHOLD, 33);

    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerManager taskSchedulerManager =
        mock(TaskSchedulerManager.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerManager);
    amNodeTracker.init(conf);
    amNodeTracker.start();

    _testSingleNodeNotBlocklisted(amNodeTracker, handler, 1);
  }

  @Test (timeout = 5000)
  public void testSingleNodeNotBlocklistedAlternateScheduler2() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    conf.setBoolean(TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_ENABLED, true);
    conf.setInt(TezConfiguration.TEZ_AM_NODE_BLOCKLISTING_IGNORE_THRESHOLD, 33);

    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerManager taskSchedulerManager =
        mock(TaskSchedulerManager.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerManager);
    amNodeTracker.init(conf);
    amNodeTracker.start();

    // Register multiple nodes from a scheduler which isn't being tested.
    // This should not affect the blocklisting behaviour
    for (int i = 0 ; i < 10 ; i++) {
      amNodeTracker.nodeSeen(NodeId.newInstance("fakenode" + i, 3333), 0);
    }

    _testSingleNodeNotBlocklisted(amNodeTracker, handler, 1);
    // No impact on blocklisting for the alternate source
    assertFalse(amNodeTracker.isBlocklistingIgnored(0));
  }

  @Test(timeout=10000)
  public void testNodeSelfBlocklist() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerManager taskSchedulerManager =
        mock(TaskSchedulerManager.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerManager);
    amNodeTracker.init(conf);
    amNodeTracker.start();
    try {
      _testNodeSelfBlocklist(amNodeTracker, handler, 0);
    } finally {
      amNodeTracker.stop();
    }
  }

  @Test(timeout=10000)
  public void testNodeSelfBlocklistAlternateScheduler1() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerManager taskSchedulerManager =
        mock(TaskSchedulerManager.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerManager);
    amNodeTracker.init(conf);
    amNodeTracker.start();
    try {
      _testNodeSelfBlocklist(amNodeTracker, handler, 1);
    } finally {
      amNodeTracker.stop();
    }
  }

  @Test(timeout=10000)
  public void testNodeSelfBlocklistAlternateScheduler2() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerManager taskSchedulerManager =
        mock(TaskSchedulerManager.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerManager);
    amNodeTracker.init(conf);
    amNodeTracker.start();
    try {
      // Register multiple nodes from a scheduler which isn't being tested.
      // This should not affect the blocklisting behaviour
      for (int i = 0 ; i < 100 ; i++) {
        amNodeTracker.nodeSeen(NodeId.newInstance("fakenode" + i, 3333), 0);
      }
      _testNodeSelfBlocklist(amNodeTracker, handler, 1);
      assertFalse(amNodeTracker.isBlocklistingIgnored(0));
    } finally {
      amNodeTracker.stop();
    }
  }

  @Test(timeout=10000)
  public void testMultipleAMNodeIDs() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerManager taskSchedulerManager =
      mock(TaskSchedulerManager.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerManager);
    amNodeTracker.init(conf);
    amNodeTracker.start();
    try {
      amNodeTracker.nodeSeen(new ExtendedNodeId(NodeId.newInstance("host", 2222), "uuid1"), 0);
      amNodeTracker.nodeSeen(new ExtendedNodeId(NodeId.newInstance("host", 2222), "uuid1"), 0);
      amNodeTracker.nodeSeen(new ExtendedNodeId(NodeId.newInstance("host", 2222), "uuid2"), 0);
      amNodeTracker.nodeSeen(new ExtendedNodeId(NodeId.newInstance("host", 2222), "uuid2"), 0);
      assertEquals(2, amNodeTracker.getNumNodes(0));
    } finally {
      amNodeTracker.stop();
    }
  }

  @Test(timeout = 10000L)
  public void testNodeCompletedAndCleanup() {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setInt(TezConfiguration.TEZ_AM_MAX_TASK_FAILURES_PER_NODE, 2);
    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    AMContainerMap amContainerMap = mock(AMContainerMap.class);
    TaskSchedulerManager taskSchedulerManager =
        mock(TaskSchedulerManager.class);
    dispatcher.register(AMNodeEventType.class, amNodeTracker);
    dispatcher.register(AMContainerEventType.class, amContainerMap);
    dispatcher.register(AMSchedulerEventType.class, taskSchedulerManager);
    amNodeTracker.init(conf);
    amNodeTracker.start();

    try {

      NodeId nodeId = NodeId.newInstance("fakenode", 3333);
      amNodeTracker.nodeSeen(nodeId, 0);

      AMNode amNode = amNodeTracker.get(nodeId, 0);
      ContainerId[] containerIds = new ContainerId[7];

      // Start 5 containers.
      for (int i = 0; i < 5; i++) {
        containerIds[i] = mock(ContainerId.class);
        amNodeTracker
            .handle(new AMNodeEventContainerAllocated(nodeId, 0, containerIds[i]));
      }
      assertEquals(5, amNode.getContainers().size());

      // Finnish 1st dag
      amNodeTracker.dagComplete(mock(DAG.class));
      assertEquals(5, amNode.getContainers().size());


      // Mark 2 as complete. Finish 2nd dag.
      for (int i = 0; i < 2; i++) {
        amNodeTracker.handle(
            new AMNodeEventContainerCompleted(nodeId, 0, containerIds[i]));
      }
      amNodeTracker.dagComplete(mock(DAG.class));
      assertEquals(3, amNode.getContainers().size());

      // Add 2 more containers. Mark all as complete. Finish 3rd dag.
      for (int i = 5; i < 7; i++) {
        containerIds[i] = mock(ContainerId.class);
        amNodeTracker
            .handle(new AMNodeEventContainerAllocated(nodeId, 0, containerIds[i]));
      }
      assertEquals(5, amNode.getContainers().size());
      amNodeTracker.dagComplete(mock(DAG.class));
      assertEquals(5, amNode.getContainers().size());
      amNodeTracker.dagComplete(mock(DAG.class));
      assertEquals(5, amNode.getContainers().size());

      for (int i = 2; i < 7; i++) {
        amNodeTracker.handle(
            new AMNodeEventContainerCompleted(nodeId, 0, containerIds[i]));
      }
      assertEquals(5, amNode.getContainers().size());
      amNodeTracker.dagComplete(mock(DAG.class));
      assertEquals(0, amNode.getContainers().size());

    } finally {
      amNodeTracker.stop();
    }

  }

  @Test(timeout=10000)
  public void testNodeUnhealthyRescheduleTasksEnabled() throws Exception {
    _testNodeUnhealthyRescheduleTasks(true, false);
  }

  @Test(timeout=10000)
  public void testNodeUnhealthyRescheduleTasksDisabled() throws Exception {
    _testNodeUnhealthyRescheduleTasks(false, false);
  }


  @Test(timeout=10000)
  public void testNodeUnhealthyRescheduleTasksEnabledAMNode() throws Exception {
    _testNodeUnhealthyRescheduleTasks(true, true);
  }

  @Test(timeout=10000)
  public void testNodeUnhealthyRescheduleTasksDisabledAMNode() throws Exception {
    _testNodeUnhealthyRescheduleTasks(false, true);
  }

  private void _testNodeUnhealthyRescheduleTasks(boolean rescheduleTasks, boolean useExtendedNodeId) {
    AppContext appContext = mock(AppContext.class);
    Configuration conf = new Configuration(false);
    conf.setBoolean(TezConfiguration.TEZ_AM_NODE_UNHEALTHY_RESCHEDULE_TASKS,
        rescheduleTasks);
    TestEventHandler handler = new TestEventHandler();
    AMNodeTracker amNodeTracker = new AMNodeTracker(handler, appContext);
    doReturn(amNodeTracker).when(appContext).getNodeTracker();
    amNodeTracker.init(conf);
    amNodeTracker.start();

    // add a node
    amNodeTracker.handle(new AMNodeEventNodeCountUpdated(1, 0));
    NodeId nodeId;
    if (useExtendedNodeId) {
      nodeId = new ExtendedNodeId(NodeId.newInstance("host1", 1234), "uuid2");
      amNodeTracker.nodeSeen(nodeId, 0);
    } else {
      nodeId = NodeId.newInstance("host1", 1234);
      amNodeTracker.nodeSeen(nodeId, 0);
    }
    AMNodeImpl node = (AMNodeImpl) amNodeTracker.get(nodeId, 0);

    // simulate task starting on node
    ContainerId cid = mock(ContainerId.class);
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, 0, cid));

    // mark node unhealthy
    NodeReport nodeReport = generateNodeReport(nodeId, NodeState.UNHEALTHY);
    amNodeTracker.handle(new AMNodeEventStateChanged(nodeReport, 0));
    assertEquals(AMNodeState.UNHEALTHY, node.getState());

    // check for task rescheduling events
    if (rescheduleTasks) {
      assertEquals(1, handler.events.size());
      assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(0).getType());
    } else {
      assertEquals(0, handler.events.size());
    }

    amNodeTracker.stop();
  }

  private void _testSingleNodeNotBlocklisted(AMNodeTracker amNodeTracker,
                                             TestEventHandler handler, int schedulerId) {
    amNodeTracker.handle(new AMNodeEventNodeCountUpdated(1, schedulerId));
    NodeId nodeId = NodeId.newInstance("host1", 1234);
    amNodeTracker.nodeSeen(nodeId, schedulerId);

    AMNodeImpl node = (AMNodeImpl) amNodeTracker.get(nodeId, schedulerId);

    ContainerId cId1 = mock(ContainerId.class);
    ContainerId cId2 = mock(ContainerId.class);

    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, schedulerId, cId1));
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, schedulerId, cId2));

    TezTaskAttemptID ta1 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta2 = mock(TezTaskAttemptID.class);

    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, schedulerId, cId1, ta1, true));
    dispatcher.await();
    assertEquals(1, node.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node.getState());

    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, schedulerId, cId2, ta2, true));
    dispatcher.await();
    assertEquals(2, node.numFailedTAs);
    assertEquals(1, handler.events.size());
    assertEquals(AMNodeEventType.N_IGNORE_BLOCKLISTING_ENABLED, handler.events.get(0).getType());
    assertEquals(AMNodeState.FORCED_ACTIVE, node.getState());
    // Blocklisting should be ignored since the node should have been blocklisted, but has not been
    // as a result of being a single node for the source
    assertTrue(amNodeTracker.isBlocklistingIgnored(schedulerId));
  }

  private void _testNodeSelfBlocklist(AMNodeTracker amNodeTracker, TestEventHandler handler, int schedulerId) {
    amNodeTracker.handle(new AMNodeEventNodeCountUpdated(4, schedulerId));
    NodeId nodeId = NodeId.newInstance("host1", 1234);
    NodeId nodeId2 = NodeId.newInstance("host2", 1234);
    NodeId nodeId3 = NodeId.newInstance("host3", 1234);
    NodeId nodeId4 = NodeId.newInstance("host4", 1234);
    amNodeTracker.nodeSeen(nodeId, schedulerId);
    amNodeTracker.nodeSeen(nodeId2, schedulerId);
    amNodeTracker.nodeSeen(nodeId3, schedulerId);
    amNodeTracker.nodeSeen(nodeId4, schedulerId);
    AMNodeImpl node = (AMNodeImpl) amNodeTracker.get(nodeId, schedulerId);

    ContainerId cId1 = mock(ContainerId.class);
    ContainerId cId2 = mock(ContainerId.class);
    ContainerId cId3 = mock(ContainerId.class);

    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, schedulerId, cId1));
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, schedulerId, cId2));
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId, schedulerId, cId3));
    assertEquals(3, node.containers.size());

    TezTaskAttemptID ta1 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta2 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta3 = mock(TezTaskAttemptID.class);

    amNodeTracker.handle(new AMNodeEventTaskAttemptSucceeded(nodeId, schedulerId, cId1, ta1));
    assertEquals(1, node.numSuccessfulTAs);

    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, schedulerId, cId2, ta2, true));
    assertEquals(1, node.numSuccessfulTAs);
    assertEquals(1, node.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node.getState());
    // duplicate should not affect anything
    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, schedulerId, cId2, ta2, true));
    assertEquals(1, node.numSuccessfulTAs);
    assertEquals(1, node.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node.getState());

    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId, schedulerId, cId3, ta3, true));
    dispatcher.await();
    assertEquals(1, node.numSuccessfulTAs);
    assertEquals(2, node.numFailedTAs);
    assertEquals(AMNodeState.BLOCKLISTED, node.getState());

    assertEquals(4, handler.events.size());
    assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(0).getType());
    assertEquals(cId1, ((AMContainerEventNodeFailed)handler.events.get(0)).getContainerId());
    assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(1).getType());
    assertEquals(cId2, ((AMContainerEventNodeFailed)handler.events.get(1)).getContainerId());
    assertEquals(AMContainerEventType.C_NODE_FAILED, handler.events.get(2).getType());
    assertEquals(cId3, ((AMContainerEventNodeFailed)handler.events.get(2)).getContainerId());
    assertEquals(AMSchedulerEventType.S_NODE_BLOCKLISTED, handler.events.get(3).getType());
    assertEquals(node.getNodeId(), ((AMSchedulerEventNodeBlocklistUpdate)handler.events.get(3)).getNodeId());


    // Trigger one more node failure, which should cause BLOCKLISTING to be disabled
    ContainerId cId4 = mock(ContainerId.class);
    ContainerId cId5 = mock(ContainerId.class);
    TezTaskAttemptID ta4 = mock(TezTaskAttemptID.class);
    TezTaskAttemptID ta5 = mock(TezTaskAttemptID.class);
    AMNodeImpl node2 = (AMNodeImpl) amNodeTracker.get(nodeId2, schedulerId);
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId2, schedulerId, cId4));
    amNodeTracker.handle(new AMNodeEventContainerAllocated(nodeId2, schedulerId, cId5));

    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId2, schedulerId, cId4, ta4, true));
    assertEquals(1, node2.numFailedTAs);
    assertEquals(AMNodeState.ACTIVE, node2.getState());

    handler.events.clear();
    amNodeTracker.handle(new AMNodeEventTaskAttemptEnded(nodeId2, schedulerId, cId5, ta5, true));
    dispatcher.await();
    assertEquals(2, node2.numFailedTAs);
    assertEquals(AMNodeState.FORCED_ACTIVE, node2.getState());
    AMNodeImpl node3 = (AMNodeImpl) amNodeTracker.get(nodeId3, schedulerId);
    assertEquals(AMNodeState.FORCED_ACTIVE, node3.getState());
    assertEquals(5, handler.events.size());

    // Blocklisting Disabled, the node causing this will not be blocklisted. The single node that
    // was blocklisted will be unblocklisted.
    int numIgnoreBlocklistingEnabledEvents = 0;
    int numUnblocklistedEvents = 0;
    for (Event event : handler.events) {
      if (event.getType() == AMNodeEventType.N_IGNORE_BLOCKLISTING_ENABLED) {
        numIgnoreBlocklistingEnabledEvents++;
      } else if (event.getType() == AMSchedulerEventType.S_NODE_UNBLOCKLISTED) {
        numUnblocklistedEvents++;
      } else {
        fail("Unexpected event of type: " + event.getType());
      }
    }
    assertEquals(4, numIgnoreBlocklistingEnabledEvents);
    assertEquals(1, numUnblocklistedEvents);

    // drain all previous events
    dispatcher.await();


    // Increase the number of nodes. BLOCKLISTING should be re-enabled.
    // Node 1 and Node 2 should go into BLOCKLISTED state
    handler.events.clear();
    amNodeTracker.handle(new AMNodeEventNodeCountUpdated(8, schedulerId));
    dispatcher.await();
    LOG.info(("Completed waiting for dispatcher to process all pending events"));
    assertEquals(AMNodeState.BLOCKLISTED, node.getState());
    assertEquals(AMNodeState.BLOCKLISTED, node2.getState());
    assertEquals(AMNodeState.ACTIVE, node3.getState());
    assertEquals(8, handler.events.size());

    int index = 0;
    int numIgnoreBlocklistingDisabledEvents = 0;
    int numBlocklistedEvents = 0;
    int numNodeFailedEvents = 0;
    for (Event event : handler.events) {
      LOG.info("Logging event: index:" + index++
          + " type: " + event.getType());
      if (event.getType() == AMNodeEventType.N_IGNORE_BLOCKLISTING_DISABLED) {
        numIgnoreBlocklistingDisabledEvents++;
      } else if (event.getType() == AMSchedulerEventType.S_NODE_BLOCKLISTED) {
        numBlocklistedEvents++;
      } else if (event.getType() == AMContainerEventType.C_NODE_FAILED) {
        numNodeFailedEvents++;
        // Node2 is now blocklisted so the container's will be informed
        assertTrue(((AMContainerEventNodeFailed) event).getContainerId() == cId4 ||
            ((AMContainerEventNodeFailed) event).getContainerId() == cId5);
      } else {
        fail("Unexpected event of type: " + event.getType());
      }
    }
    assertEquals(4, numIgnoreBlocklistingDisabledEvents);
    assertEquals(2, numBlocklistedEvents);
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
