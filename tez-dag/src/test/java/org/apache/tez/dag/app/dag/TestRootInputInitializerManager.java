/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.dag;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.RootInputLeafOutput;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.oldrecords.TaskState;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.hadoop.shim.DefaultHadoopShim;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestRootInputInitializerManager {

  // Simple testing. No events if task doesn't succeed.
  // Also exercises path where two attempts are reported as successful via the stateChangeNotifier.
  // Primarily a failure scenario, when a Task moves back to running from success
  // Order event1, success1, event2, success2
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testEventBeforeSuccess() throws Exception {
    InputDescriptor id = mock(InputDescriptor.class);
    InputInitializerDescriptor iid = mock(InputInitializerDescriptor.class);
    RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> rootInput =
        new RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>("InputName", id, iid);

    InputInitializer initializer = mock(InputInitializer.class);
    InputInitializerContext initializerContext = mock(InputInitializerContext.class);
    Vertex vertex = mock(Vertex.class);
    StateChangeNotifier stateChangeNotifier = mock(StateChangeNotifier.class);
    AppContext appContext = mock(AppContext.class, RETURNS_DEEP_STUBS);

    RootInputInitializerManager.InitializerWrapper initializerWrapper =
        new RootInputInitializerManager.InitializerWrapper(rootInput, initializer,
            initializerContext, vertex, stateChangeNotifier, appContext);

    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID srcVertexId = TezVertexID.getInstance(dagId, 2);
    TezTaskID srcTaskId1 = TezTaskID.getInstance(srcVertexId, 3);
    Vertex srcVertex = mock(Vertex.class);
    Task srcTask1 = mock(Task.class);
    doReturn(TaskState.RUNNING).when(srcTask1).getState();
    doReturn(srcTask1).when(srcVertex).getTask(srcTaskId1.getId());
    when(appContext.getCurrentDAG().getVertex(any(String.class))).thenReturn(srcVertex);

    String srcVertexName = "srcVertexName";
    List<TezEvent> eventList = Lists.newLinkedList();


    // First Attempt send event
    TezTaskAttemptID srcTaskAttemptId11 = TezTaskAttemptID.getInstance(srcTaskId1, 1);
    EventMetaData sourceInfo11 =
        new EventMetaData(EventMetaData.EventProducerConsumerType.PROCESSOR, srcVertexName, null,
            srcTaskAttemptId11);
    InputInitializerEvent e1 = InputInitializerEvent.create("fakeVertex", "fakeInput", null);
    TezEvent te1 = new TezEvent(e1, sourceInfo11);
    eventList.add(te1);
    initializerWrapper.handleInputInitializerEvents(eventList);

    verify(initializer, never()).handleInputInitializerEvent(any(List.class));
    eventList.clear();

    // First attempt, Task success notification
    initializerWrapper.onTaskSucceeded(srcVertexName, srcTaskId1, srcTaskAttemptId11.getId());
    ArgumentCaptor<List> argumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(initializer, times(1)).handleInputInitializerEvent(argumentCaptor.capture());
    List<InputInitializerEvent> invokedEvents = argumentCaptor.getValue();
    assertEquals(1, invokedEvents.size());

    reset(initializer);

    // 2nd attempt send event
    TezTaskAttemptID srcTaskAttemptId12 = TezTaskAttemptID.getInstance(srcTaskId1, 2);
    EventMetaData sourceInfo12 =
        new EventMetaData(EventMetaData.EventProducerConsumerType.PROCESSOR, srcVertexName, null,
            srcTaskAttemptId12);
    InputInitializerEvent e2 = InputInitializerEvent.create("fakeVertex", "fakeInput", null);
    TezEvent te2 = new TezEvent(e2, sourceInfo12);
    eventList.add(te2);
    initializerWrapper.handleInputInitializerEvents(eventList);

    verify(initializer, never()).handleInputInitializerEvent(any(List.class));
    eventList.clear();
    reset(initializer);

    // 2nd attempt succeeded
    initializerWrapper.onTaskSucceeded(srcVertexName, srcTaskId1, srcTaskAttemptId12.getId());
    verify(initializer, never()).handleInputInitializerEvent(argumentCaptor.capture());
  }

  // Order event1 success1, success2, event2
  // Primarily a failure scenario, when a Task moves back to running from success
  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testSuccessBeforeEvent() throws Exception {
    InputDescriptor id = mock(InputDescriptor.class);
    InputInitializerDescriptor iid = mock(InputInitializerDescriptor.class);
    RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> rootInput =
        new RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor>("InputName", id, iid);

    InputInitializer initializer = mock(InputInitializer.class);
    InputInitializerContext initializerContext = mock(InputInitializerContext.class);
    Vertex vertex = mock(Vertex.class);
    StateChangeNotifier stateChangeNotifier = mock(StateChangeNotifier.class);
    AppContext appContext = mock(AppContext.class, RETURNS_DEEP_STUBS);

    RootInputInitializerManager.InitializerWrapper initializerWrapper =
        new RootInputInitializerManager.InitializerWrapper(rootInput, initializer,
            initializerContext, vertex, stateChangeNotifier, appContext);

    ApplicationId appId = ApplicationId.newInstance(1000, 1);
    TezDAGID dagId = TezDAGID.getInstance(appId, 1);
    TezVertexID srcVertexId = TezVertexID.getInstance(dagId, 2);
    TezTaskID srcTaskId1 = TezTaskID.getInstance(srcVertexId, 3);
    Vertex srcVertex = mock(Vertex.class);
    Task srcTask1 = mock(Task.class);
    doReturn(TaskState.RUNNING).when(srcTask1).getState();
    doReturn(srcTask1).when(srcVertex).getTask(srcTaskId1.getId());
    when(appContext.getCurrentDAG().getVertex(any(String.class))).thenReturn(srcVertex);

    String srcVertexName = "srcVertexName";
    List<TezEvent> eventList = Lists.newLinkedList();


    // First Attempt send event
    TezTaskAttemptID srcTaskAttemptId11 = TezTaskAttemptID.getInstance(srcTaskId1, 1);
    EventMetaData sourceInfo11 =
        new EventMetaData(EventMetaData.EventProducerConsumerType.PROCESSOR, srcVertexName, null,
            srcTaskAttemptId11);
    InputInitializerEvent e1 = InputInitializerEvent.create("fakeVertex", "fakeInput", null);
    TezEvent te1 = new TezEvent(e1, sourceInfo11);
    eventList.add(te1);
    initializerWrapper.handleInputInitializerEvents(eventList);

    verify(initializer, never()).handleInputInitializerEvent(any(List.class));
    eventList.clear();

    // First attempt, Task success notification
    initializerWrapper.onTaskSucceeded(srcVertexName, srcTaskId1, srcTaskAttemptId11.getId());
    ArgumentCaptor<List> argumentCaptor = ArgumentCaptor.forClass(List.class);
    verify(initializer, times(1)).handleInputInitializerEvent(argumentCaptor.capture());
    List<InputInitializerEvent> invokedEvents = argumentCaptor.getValue();
    assertEquals(1, invokedEvents.size());

    reset(initializer);


    TezTaskAttemptID srcTaskAttemptId12 = TezTaskAttemptID.getInstance(srcTaskId1, 2);
    // 2nd attempt succeeded
    initializerWrapper.onTaskSucceeded(srcVertexName, srcTaskId1, srcTaskAttemptId12.getId());
    verify(initializer, never()).handleInputInitializerEvent(any(List.class));

    // 2nd attempt send event
    EventMetaData sourceInfo12 =
        new EventMetaData(EventMetaData.EventProducerConsumerType.PROCESSOR, srcVertexName, null,
            srcTaskAttemptId12);
    InputInitializerEvent e2 = InputInitializerEvent.create("fakeVertex", "fakeInput", null);
    TezEvent te2 = new TezEvent(e2, sourceInfo12);
    eventList.add(te2);
    initializerWrapper.handleInputInitializerEvents(eventList);

    verify(initializer, never()).handleInputInitializerEvent(any(List.class));
  }


  @Test (timeout = 5000)
  public void testCorrectUgiUsage() throws TezException, InterruptedException {
    Vertex vertex = mock(Vertex.class);
    doReturn(mock(TezVertexID.class)).when(vertex).getVertexId();
    AppContext appContext = mock(AppContext.class);
    doReturn(new DefaultHadoopShim()).when(appContext).getHadoopShim();
    doReturn(mock(EventHandler.class)).when(appContext).getEventHandler();
    UserGroupInformation dagUgi = UserGroupInformation.createRemoteUser("fakeuser");
    StateChangeNotifier stateChangeNotifier = mock(StateChangeNotifier.class);
    RootInputInitializerManager rootInputInitializerManager = new RootInputInitializerManager(vertex, appContext, dagUgi, stateChangeNotifier);

    InputDescriptor id = mock(InputDescriptor.class);
    InputInitializerDescriptor iid = InputInitializerDescriptor.create(InputInitializerForUgiTest.class.getName());
    RootInputLeafOutput<InputDescriptor, InputInitializerDescriptor> rootInput =
        new RootInputLeafOutput<>("InputName", id, iid);
    rootInputInitializerManager.runInputInitializers(Collections.singletonList(rootInput));

    InputInitializerForUgiTest.awaitInitialize();

    assertEquals(dagUgi, InputInitializerForUgiTest.ctorUgi);
    assertEquals(dagUgi, InputInitializerForUgiTest.initializeUgi);
  }

  public static class InputInitializerForUgiTest extends InputInitializer {

    static volatile UserGroupInformation ctorUgi;
    static volatile UserGroupInformation initializeUgi;

    static boolean initialized = false;
    static final Object initializeSync = new Object();

    public InputInitializerForUgiTest(InputInitializerContext initializerContext) {
      super(initializerContext);
      try {
        ctorUgi = UserGroupInformation.getCurrentUser();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public List<Event> initialize() throws Exception {
      initializeUgi = UserGroupInformation.getCurrentUser();
      synchronized (initializeSync) {
        initialized = true;
        initializeSync.notify();
      }
      return null;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws Exception {
    }

    static void awaitInitialize() throws InterruptedException {
      synchronized (initializeSync) {
        while (!initialized) {
          initializeSync.wait();
        }
      }
    }
  }
}
