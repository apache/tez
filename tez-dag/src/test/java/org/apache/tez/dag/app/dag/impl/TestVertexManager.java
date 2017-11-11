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

package org.apache.tez.dag.app.dag.impl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.app.dag.Vertex;
import org.apache.tez.dag.app.dag.event.CallableEvent;
import org.apache.tez.dag.app.dag.event.VertexEventInputDataInformation;
import org.apache.tez.dag.app.dag.event.VertexEventRouteEvent;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskAttemptIdentifier;
import org.apache.tez.runtime.api.events.CustomProcessorEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class TestVertexManager {
  AppContext mockAppContext;
  ListeningExecutorService execService;
  Vertex mockVertex;
  EventHandler mockHandler;
  ArgumentCaptor<VertexEventInputDataInformation> requestCaptor;
  
  @Before
  public void setup() {
    mockAppContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    execService = mock(ListeningExecutorService.class);
    final ListenableFuture<Void> mockFuture = mock(ListenableFuture.class);
    Mockito.doAnswer(new Answer() {
      public ListenableFuture<Void> answer(InvocationOnMock invocation) {
          Object[] args = invocation.getArguments();
          CallableEvent e = (CallableEvent) args[0];
          new CallableEventDispatcher().handle(e);
          return mockFuture;
      }})
    .when(execService).submit((Callable<Void>) any());
    doReturn(execService).when(mockAppContext).getExecService();
    mockVertex = mock(Vertex.class, RETURNS_DEEP_STUBS);
    doReturn("vertex1").when(mockVertex).getName();
    mockHandler = mock(EventHandler.class);
    when(mockAppContext.getEventHandler()).thenReturn(mockHandler);
    when(
        mockAppContext.getCurrentDAG().getVertex(any(String.class))
            .getTotalTasks()).thenReturn(1);
    requestCaptor = ArgumentCaptor.forClass(VertexEventInputDataInformation.class);

  }

  public static class CheckUserPayloadVertexManagerPlugin extends VertexManagerPlugin {
    public CheckUserPayloadVertexManagerPlugin(VertexManagerPluginContext context) {
      super(context);
      assertNotNull(context.getUserPayload());
    }

    @Override
    public void initialize() throws Exception {}

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) throws Exception {}

    @Override
    public void onRootVertexInitialized(String inputName, InputDescriptor inputDescriptor,
                                        List<Event> events) throws Exception {}
  }

  @Test(timeout = 5000)
  public void testVertexManagerPluginCtorAccessUserPayload() throws IOException, TezException {
    byte[] randomUserPayload = {1,2,3};
    UserPayload userPayload = UserPayload.create(ByteBuffer.wrap(randomUserPayload));
    VertexManager vm =
        new VertexManager(
            VertexManagerPluginDescriptor.create(CheckUserPayloadVertexManagerPlugin.class
                .getName()).setUserPayload(userPayload), UserGroupInformation.getCurrentUser(),
            mockVertex, mockAppContext, mock(StateChangeNotifier.class));
  }

  
  @Test(timeout = 5000)
  public void testOnRootVertexInitialized() throws Exception {
    Configuration conf = new Configuration();
    VertexManager vm =
        new VertexManager(RootInputVertexManager.createConfigBuilder(conf)
            .build(), UserGroupInformation.getCurrentUser(),
            mockVertex, mockAppContext, mock(StateChangeNotifier.class));
    vm.initialize();
    InputDescriptor id1 = mock(InputDescriptor.class);
    List<Event> events1 = new LinkedList<Event>();
    InputDataInformationEvent diEvent1 =
        InputDataInformationEvent.createWithSerializedPayload(0, null);
    events1.add(diEvent1);
    vm.onRootVertexInitialized("input1", id1, events1);
    verify(mockHandler, times(1)).handle(requestCaptor.capture());
    List<TezEvent> tezEvents1 = requestCaptor.getValue().getEvents();
    assertEquals(1, tezEvents1.size());
    assertEquals(diEvent1, tezEvents1.get(0).getEvent());

    InputDescriptor id2 = mock(InputDescriptor.class);
    List<Event> events2 = new LinkedList<Event>();
    InputDataInformationEvent diEvent2 =
        InputDataInformationEvent.createWithSerializedPayload(0, null);
    events2.add(diEvent2);
    vm.onRootVertexInitialized("input1", id2, events2);
    verify(mockHandler, times(2)).handle(requestCaptor.capture());
    List<TezEvent> tezEvents2 = requestCaptor.getValue().getEvents();
    assertEquals(tezEvents2.size(), 1);
    assertEquals(diEvent2, tezEvents2.get(0).getEvent());
  }

  /**
   * TEZ-1647
   * custom vertex manager generates events only when both i1 and i2 are initialized.
   * @throws Exception
   */
  @Test(timeout = 5000)
  public void testOnRootVertexInitialized2() throws Exception {
    VertexManager vm =
        new VertexManager(
            VertexManagerPluginDescriptor.create(CustomVertexManager.class
                .getName()), UserGroupInformation.getCurrentUser(),
                mockVertex, mockAppContext, mock(StateChangeNotifier.class));
    vm.initialize();
    InputDescriptor id1 = mock(InputDescriptor.class);
    List<Event> events1 = new LinkedList<Event>();
    InputDataInformationEvent diEvent1 =
        InputDataInformationEvent.createWithSerializedPayload(0, null);
    events1.add(diEvent1);

    // do not call context.addRootInputEvents, just cache the TezEvent
    vm.onRootVertexInitialized("input1", id1, events1);
    verify(mockHandler, times(1)).handle(requestCaptor.capture());
    List<TezEvent> tezEventsAfterInput1 = requestCaptor.getValue().getEvents();
    assertEquals(0, tezEventsAfterInput1.size());
    
    InputDescriptor id2 = mock(InputDescriptor.class);
    List<Event> events2 = new LinkedList<Event>();
    InputDataInformationEvent diEvent2 =
        InputDataInformationEvent.createWithSerializedPayload(0, null);
    events2.add(diEvent2);
    // call context.addRootInputEvents(input1), context.addRootInputEvents(input2)
    vm.onRootVertexInitialized("input2", id2, events2);
    verify(mockHandler, times(2)).handle(requestCaptor.capture());
    List<TezEvent> tezEventsAfterInput2 = requestCaptor.getValue().getEvents();
    assertEquals(2, tezEventsAfterInput2.size());

    // also verify the EventMetaData
    Set<String> edgeVertexSet = new HashSet<String>();
    for (TezEvent tezEvent : tezEventsAfterInput2) {
      edgeVertexSet.add(tezEvent.getDestinationInfo().getEdgeVertexName());
    }
    assertEquals(Sets.newHashSet("input1","input2"), edgeVertexSet);
  }

  @Test(timeout = 5000)
  public void testVMPluginCtxGetInputVertexGroup() throws Exception {
    VertexManager vm =
      new VertexManager(VertexManagerPluginDescriptor.create(CustomVertexManager.class.getName()),
        UserGroupInformation.getCurrentUser(), mockVertex, mockAppContext,
        mock(StateChangeNotifier.class));

    assertTrue(vm.pluginContext.getInputVertexGroups().isEmpty());

    String group = "group", v1 = "v1", v2 = "v2";
    when(mockVertex.getGroupInputSpecList())
      .thenReturn(Arrays.asList(new GroupInputSpec(group, Arrays.asList(v1, v2), null)));
    Map<String, List<String>> groups = vm.pluginContext.getInputVertexGroups();
    assertEquals(1, groups.size());
    assertTrue(groups.containsKey(group));
    assertEquals(2, groups.get(group).size());
    assertTrue(groups.get(group).contains(v1));
    assertTrue(groups.get(group).contains(v2));
  }

  @Test(timeout = 5000)
  public void testSendCustomProcessorEvent() throws Exception {
    VertexManager vm =
      new VertexManager(VertexManagerPluginDescriptor.create(CustomVertexManager.class.getName()),
        UserGroupInformation.getCurrentUser(), mockVertex, mockAppContext,
        mock(StateChangeNotifier.class));
    ArgumentCaptor<VertexEventRouteEvent> requestCaptor =
      ArgumentCaptor.forClass(VertexEventRouteEvent.class);

    when(mockVertex.getTotalTasks()).thenReturn(2);

    List<CustomProcessorEvent> events = new ArrayList<>();
    // task id too small, should fail
    try {
      vm.pluginContext.sendEventToProcessor(events, -1);
      fail("Should fail for invalid task id");
    } catch (IllegalArgumentException exception) {
      assertTrue(exception.getMessage().contains("Invalid taskId"));
    }
    // task id too large, should fail
    try {
      vm.pluginContext.sendEventToProcessor(events, 10);
      fail("Should fail for invalid task id");
    } catch (IllegalArgumentException exception) {
      assertTrue(exception.getMessage().contains("Invalid taskId"));
    }

    // null event, do nothing
    vm.pluginContext.sendEventToProcessor(null, 0);
    verify(mockHandler, never()).handle(requestCaptor.capture());

    // empty event
    vm.pluginContext.sendEventToProcessor(events, 1);
    verify(mockHandler, never()).handle(requestCaptor.capture());

    //events.add();
    byte[] payload = new byte[] {1,2,3};
    events.add(CustomProcessorEvent.create(ByteBuffer.wrap(payload)));
    vm.pluginContext.sendEventToProcessor(events, 1);
    verify(mockHandler, times(1)).handle(requestCaptor.capture());
    CustomProcessorEvent cpe =
      (CustomProcessorEvent)(requestCaptor.getValue().getEvents().get(0).getEvent());
    assertArrayEquals(payload, cpe.getPayload().array());
  }

  public static class CustomVertexManager extends VertexManagerPlugin {

    private Map<String,List<Event>> cachedEventMap = new HashMap<String, List<Event>>();

    public CustomVertexManager(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
    }

    @Override
    public void onVertexStarted(List<TaskAttemptIdentifier> completions) {
    }

    @Override
    public void onSourceTaskCompleted(TaskAttemptIdentifier attempt) {
    }

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
    }

    /**
     * only addRootInputEvents when it is "input2", otherwise just cache it.
     */
    @Override
    public void onRootVertexInitialized(String inputName,
        InputDescriptor inputDescriptor, List<Event> events) {
      cachedEventMap.put(inputName, events);
      if (inputName.equals("input2")) {
        for (Map.Entry<String, List<Event>> entry : cachedEventMap.entrySet()) {
          List<InputDataInformationEvent> riEvents = Lists.newLinkedList();
          for (Event event : events) {
            riEvents.add((InputDataInformationEvent)event);
          }
          getContext().addRootInputEvents(entry.getKey(), riEvents);
        }
      }
    }
  }
}
