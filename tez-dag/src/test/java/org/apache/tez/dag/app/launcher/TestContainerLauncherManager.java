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

package org.apache.tez.dag.app.launcher;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.event.Event;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventType;
import org.apache.tez.dag.app.dag.event.DAGAppMasterEventUserServiceFatalError;
import org.apache.tez.dag.app.dag.event.DAGEventTerminateDag;
import org.apache.tez.dag.app.rm.ContainerLauncherLaunchRequestEvent;
import org.apache.tez.dag.app.rm.ContainerLauncherStopRequestEvent;
import org.apache.tez.dag.helpers.DagInfoImplForTest;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.serviceplugins.api.ContainerLaunchRequest;
import org.apache.tez.serviceplugins.api.ContainerLauncher;
import org.apache.tez.serviceplugins.api.ContainerLauncherContext;
import org.apache.tez.serviceplugins.api.ContainerStopRequest;
import org.apache.tez.serviceplugins.api.ServicePluginErrorDefaults;
import org.apache.tez.serviceplugins.api.ServicePluginException;
import org.apache.tez.serviceplugins.api.TaskCommunicatorDescriptor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class TestContainerLauncherManager {

  @Before
  @After
  public void resetTest() {
    ContainerLaucherRouterForMultipleLauncherTest.reset();
  }

  @Test(timeout = 5000)
  public void testNoLaunchersSpecified() throws IOException, TezException {

    AppContext appContext = mock(AppContext.class);
    TaskCommunicatorManagerInterface tal = mock(TaskCommunicatorManagerInterface.class);

    try {

      new ContainerLaucherRouterForMultipleLauncherTest(appContext, tal, null, null,
          false);
      fail("Expecting a failure without any launchers being specified");
    } catch (IllegalArgumentException e) {

    }
  }

  @Test(timeout = 5000)
  public void testCustomLauncherSpecified() throws IOException, TezException {
    Configuration conf = new Configuration(false);

    AppContext appContext = mock(AppContext.class);
    TaskCommunicatorManagerInterface tal = mock(TaskCommunicatorManagerInterface.class);

    String customLauncherName = "customLauncher";
    List<NamedEntityDescriptor> launcherDescriptors = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload customPayload = UserPayload.create(bb);
    launcherDescriptors.add(
        new NamedEntityDescriptor(customLauncherName, FakeContainerLauncher.class.getName())
            .setUserPayload(customPayload));

    ContainerLaucherRouterForMultipleLauncherTest clr =
        new ContainerLaucherRouterForMultipleLauncherTest(appContext, tal, null,
            launcherDescriptors,
            true);
    try {
      clr.init(conf);
      clr.start();

      assertEquals(1, clr.getNumContainerLaunchers());
      assertFalse(clr.getYarnContainerLauncherCreated());
      assertFalse(clr.getUberContainerLauncherCreated());
      assertEquals(customLauncherName, clr.getContainerLauncherName(0));
      assertEquals(bb, clr.getContainerLauncherContext(0).getInitialUserPayload().getPayload());
    } finally {
      clr.stop();
    }
  }

  @Test(timeout = 5000)
  public void testMultipleContainerLaunchers() throws IOException, TezException {
    Configuration conf = new Configuration(false);
    conf.set("testkey", "testvalue");
    UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);

    AppContext appContext = mock(AppContext.class);
    TaskCommunicatorManagerInterface tal = mock(TaskCommunicatorManagerInterface.class);

    String customLauncherName = "customLauncher";
    List<NamedEntityDescriptor> launcherDescriptors = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload customPayload = UserPayload.create(bb);
    launcherDescriptors.add(
        new NamedEntityDescriptor(customLauncherName, FakeContainerLauncher.class.getName())
            .setUserPayload(customPayload));
    launcherDescriptors
        .add(new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
            .setUserPayload(userPayload));

    ContainerLaucherRouterForMultipleLauncherTest clr =
        new ContainerLaucherRouterForMultipleLauncherTest(appContext, tal, null,
            launcherDescriptors,
            true);
    try {
      clr.init(conf);
      clr.start();

      assertEquals(2, clr.getNumContainerLaunchers());
      assertTrue(clr.getYarnContainerLauncherCreated());
      assertFalse(clr.getUberContainerLauncherCreated());
      assertEquals(customLauncherName, clr.getContainerLauncherName(0));
      assertEquals(bb, clr.getContainerLauncherContext(0).getInitialUserPayload().getPayload());

      assertEquals(TezConstants.getTezYarnServicePluginName(), clr.getContainerLauncherName(1));
      Configuration confParsed = TezUtils
          .createConfFromUserPayload(clr.getContainerLauncherContext(1).getInitialUserPayload());
      assertEquals("testvalue", confParsed.get("testkey"));
    } finally {
      clr.stop();
    }
  }

  @Test(timeout = 5000)
  public void testEventRouting() throws Exception {
    Configuration conf = new Configuration(false);
    UserPayload userPayload = TezUtils.createUserPayloadFromConf(conf);

    AppContext appContext = mock(AppContext.class);
    TaskCommunicatorManagerInterface tal = mock(TaskCommunicatorManagerInterface.class);

    String customLauncherName = "customLauncher";
    List<NamedEntityDescriptor> launcherDescriptors = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload customPayload = UserPayload.create(bb);
    launcherDescriptors.add(
        new NamedEntityDescriptor(customLauncherName, FakeContainerLauncher.class.getName())
            .setUserPayload(customPayload));
    launcherDescriptors
        .add(new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null)
            .setUserPayload(userPayload));

    ContainerLaucherRouterForMultipleLauncherTest clr =
        new ContainerLaucherRouterForMultipleLauncherTest(appContext, tal, null,
            launcherDescriptors,
            true);
    try {
      clr.init(conf);
      clr.start();

      assertEquals(2, clr.getNumContainerLaunchers());
      assertTrue(clr.getYarnContainerLauncherCreated());
      assertFalse(clr.getUberContainerLauncherCreated());
      assertEquals(customLauncherName, clr.getContainerLauncherName(0));
      assertEquals(TezConstants.getTezYarnServicePluginName(), clr.getContainerLauncherName(1));

      verify(clr.getTestContainerLauncher(0)).initialize();
      verify(clr.getTestContainerLauncher(0)).start();
      verify(clr.getTestContainerLauncher(1)).initialize();
      verify(clr.getTestContainerLauncher(1)).start();

      ContainerLaunchContext clc1 = mock(ContainerLaunchContext.class);
      Container container1 = mock(Container.class);

      ContainerLaunchContext clc2 = mock(ContainerLaunchContext.class);
      Container container2 = mock(Container.class);

      ContainerLauncherLaunchRequestEvent launchRequestEvent1 =
          new ContainerLauncherLaunchRequestEvent(clc1, container1, 0, 0, 0);
      ContainerLauncherLaunchRequestEvent launchRequestEvent2 =
          new ContainerLauncherLaunchRequestEvent(clc2, container2, 1, 0, 0);

      clr.handle(launchRequestEvent1);


      ArgumentCaptor<ContainerLaunchRequest> captor =
          ArgumentCaptor.forClass(ContainerLaunchRequest.class);
      verify(clr.getTestContainerLauncher(0)).launchContainer(captor.capture());
      assertEquals(1, captor.getAllValues().size());
      ContainerLaunchRequest launchRequest1 = captor.getValue();
      assertEquals(clc1, launchRequest1.getContainerLaunchContext());

      clr.handle(launchRequestEvent2);
      captor = ArgumentCaptor.forClass(ContainerLaunchRequest.class);
      verify(clr.getTestContainerLauncher(1)).launchContainer(captor.capture());
      assertEquals(1, captor.getAllValues().size());
      ContainerLaunchRequest launchRequest2 = captor.getValue();
      assertEquals(clc2, launchRequest2.getContainerLaunchContext());

    } finally {
      clr.stop();
      verify(clr.getTestContainerLauncher(0)).shutdown();
      verify(clr.getTestContainerLauncher(1)).shutdown();
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testReportFailureFromContainerLauncher() throws ServicePluginException, TezException {
    final String dagName = DAG_NAME;
    final int dagIndex = DAG_INDEX;
    TezDAGID dagId = TezDAGID.getInstance(ApplicationId.newInstance(0, 0), dagIndex);
    DAG dag = mock(DAG.class);
    doReturn(dagName).when(dag).getName();
    doReturn(dagId).when(dag).getID();
    EventHandler eventHandler = mock(EventHandler.class);
    AppContext appContext = mock(AppContext.class);
    doReturn(eventHandler).when(appContext).getEventHandler();
    doReturn(dag).when(appContext).getCurrentDAG();
    doReturn("testlauncher").when(appContext).getContainerLauncherName(0);

    NamedEntityDescriptor<TaskCommunicatorDescriptor> taskCommDescriptor =
        new NamedEntityDescriptor<>("testlauncher", ContainerLauncherForTest.class.getName());
    List<NamedEntityDescriptor> list = new LinkedList<>();
    list.add(taskCommDescriptor);
    ContainerLauncherManager containerLauncherManager =
        new ContainerLauncherManager(appContext, mock(TaskCommunicatorManagerInterface.class), "",
            list, false);

    try {
      ContainerLaunchContext clc1 = mock(ContainerLaunchContext.class);
      Container container1 = mock(Container.class);
      ContainerLauncherLaunchRequestEvent launchRequestEvent =
          new ContainerLauncherLaunchRequestEvent(clc1, container1, 0, 0, 0);


      containerLauncherManager.handle(launchRequestEvent);

      ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);
      verify(eventHandler, times(1)).handle(argumentCaptor.capture());

      Event rawEvent = argumentCaptor.getValue();
      assertTrue(rawEvent instanceof DAGAppMasterEventUserServiceFatalError);
      DAGAppMasterEventUserServiceFatalError event =
          (DAGAppMasterEventUserServiceFatalError) rawEvent;
      assertEquals(DAGAppMasterEventType.CONTAINER_LAUNCHER_SERVICE_FATAL_ERROR, event.getType());
      assertTrue(event.getDiagnosticInfo().contains("ReportedFatalError"));
      assertTrue(
          event.getDiagnosticInfo().contains(ServicePluginErrorDefaults.INCONSISTENT_STATE.name()));
      assertTrue(event.getDiagnosticInfo().contains("[0:testlauncher]"));

      reset(eventHandler);
      // stop container

      ContainerId containerId2 = mock(ContainerId.class);
      NodeId nodeId2 = mock(NodeId.class);
      ContainerLauncherStopRequestEvent stopRequestEvent =
          new ContainerLauncherStopRequestEvent(containerId2, nodeId2, null, 0, 0, 0);

      argumentCaptor = ArgumentCaptor.forClass(Event.class);

      containerLauncherManager.handle(stopRequestEvent);
      verify(eventHandler, times(1)).handle(argumentCaptor.capture());
      rawEvent = argumentCaptor.getValue();
      assertTrue(rawEvent instanceof DAGEventTerminateDag);
      DAGEventTerminateDag killEvent = (DAGEventTerminateDag) rawEvent;
      assertTrue(killEvent.getDiagnosticInfo().contains("ReportError"));
      assertTrue(killEvent.getDiagnosticInfo()
          .contains(ServicePluginErrorDefaults.SERVICE_UNAVAILABLE.name()));
      assertTrue(killEvent.getDiagnosticInfo().contains("[0:testlauncher]"));
    } finally {
      containerLauncherManager.stop();
    }
  }

  @SuppressWarnings("unchecked")
  @Test(timeout = 5000)
  public void testContainerLauncherUserError() throws ServicePluginException {

    ContainerLauncher containerLauncher = mock(ContainerLauncher.class);

    EventHandler eventHandler = mock(EventHandler.class);
    AppContext appContext = mock(AppContext.class);
    doReturn(eventHandler).when(appContext).getEventHandler();
    doReturn("testlauncher").when(appContext).getContainerLauncherName(0);

    Configuration conf = new Configuration(false);

    ContainerLauncherManager containerLauncherManager =
        new ContainerLauncherManager(appContext);
    containerLauncherManager.setContainerLauncher(containerLauncher);
    try {
      containerLauncherManager.init(conf);
      containerLauncherManager.start();

      // launch container
      doThrow(new RuntimeException("testexception")).when(containerLauncher)
          .launchContainer(any(ContainerLaunchRequest.class));
      ContainerLaunchContext clc1 = mock(ContainerLaunchContext.class);
      Container container1 = mock(Container.class);
      ContainerLauncherLaunchRequestEvent launchRequestEvent =
          new ContainerLauncherLaunchRequestEvent(clc1, container1, 0, 0, 0);


      containerLauncherManager.handle(launchRequestEvent);

      ArgumentCaptor<Event> argumentCaptor = ArgumentCaptor.forClass(Event.class);
      verify(eventHandler, times(1)).handle(argumentCaptor.capture());

      Event rawEvent = argumentCaptor.getValue();
      assertTrue(rawEvent instanceof DAGAppMasterEventUserServiceFatalError);
      DAGAppMasterEventUserServiceFatalError event =
          (DAGAppMasterEventUserServiceFatalError) rawEvent;
      assertEquals(DAGAppMasterEventType.CONTAINER_LAUNCHER_SERVICE_FATAL_ERROR, event.getType());
      assertTrue(event.getError().getMessage().contains("testexception"));
      assertTrue(event.getDiagnosticInfo().contains("launching container"));
      assertTrue(event.getDiagnosticInfo().contains("[0:testlauncher]"));

      reset(eventHandler);
      // stop container

      doThrow(new RuntimeException("teststopexception")).when(containerLauncher)
          .stopContainer(any(ContainerStopRequest.class));
      ContainerId containerId2 = mock(ContainerId.class);
      NodeId nodeId2 = mock(NodeId.class);
      ContainerLauncherStopRequestEvent stopRequestEvent =
          new ContainerLauncherStopRequestEvent(containerId2, nodeId2, null, 0, 0, 0);

      argumentCaptor = ArgumentCaptor.forClass(Event.class);

      containerLauncherManager.handle(stopRequestEvent);
      verify(eventHandler, times(1)).handle(argumentCaptor.capture());
      rawEvent = argumentCaptor.getValue();
      assertTrue(rawEvent instanceof DAGAppMasterEventUserServiceFatalError);
      event = (DAGAppMasterEventUserServiceFatalError) rawEvent;
      assertTrue(event.getError().getMessage().contains("teststopexception"));
      assertTrue(event.getDiagnosticInfo().contains("stopping container"));
      assertTrue(event.getDiagnosticInfo().contains("[0:testlauncher]"));
    } finally {
      containerLauncherManager.stop();
    }
  }

  private static class ContainerLaucherRouterForMultipleLauncherTest
      extends ContainerLauncherManager {

    // All variables setup as static since methods being overridden are invoked by the ContainerLauncherRouter ctor,
    // and regular variables will not be initialized at this point.
    private static final AtomicInteger numContainerLaunchers = new AtomicInteger(0);
    private static final Set<Integer> containerLauncherIndices = new HashSet<>();
    private static final ContainerLauncher yarnContainerLauncher = mock(ContainerLauncher.class);
    private static final ContainerLauncher uberContainerlauncher = mock(ContainerLauncher.class);
    private static final AtomicBoolean yarnContainerLauncherCreated = new AtomicBoolean(false);
    private static final AtomicBoolean uberContainerLauncherCreated = new AtomicBoolean(false);

    private static final List<ContainerLauncherContext> containerLauncherContexts =
        new LinkedList<>();
    private static final List<String> containerLauncherNames = new LinkedList<>();
    private static final List<ContainerLauncher> testContainerLaunchers = new LinkedList<>();


    public static void reset() {
      numContainerLaunchers.set(0);
      containerLauncherIndices.clear();
      yarnContainerLauncherCreated.set(false);
      uberContainerLauncherCreated.set(false);
      containerLauncherContexts.clear();
      containerLauncherNames.clear();
      testContainerLaunchers.clear();
    }

    public ContainerLaucherRouterForMultipleLauncherTest(AppContext context,
                                                         TaskCommunicatorManagerInterface taskCommunicatorManagerInterface,
                                                         String workingDirectory,
                                                         List<NamedEntityDescriptor> containerLauncherDescriptors,
                                                         boolean isPureLocalMode) throws
        UnknownHostException, TezException {
      super(context, taskCommunicatorManagerInterface, workingDirectory,
          containerLauncherDescriptors, isPureLocalMode);
    }

    @Override
    ContainerLauncher createContainerLauncher(NamedEntityDescriptor containerLauncherDescriptor,
                                              AppContext context,
                                              ContainerLauncherContext containerLauncherContext,
                                              TaskCommunicatorManagerInterface taskCommunicatorManagerInterface,
                                              String workingDirectory,
                                              int containerLauncherIndex,
                                              boolean isPureLocalMode) throws TezException {
      numContainerLaunchers.incrementAndGet();
      boolean added = containerLauncherIndices.add(containerLauncherIndex);
      assertTrue("Cannot add multiple launchers with the same index", added);
      containerLauncherNames.add(containerLauncherDescriptor.getEntityName());
      containerLauncherContexts.add(containerLauncherContext);
      return super
          .createContainerLauncher(containerLauncherDescriptor, context, containerLauncherContext,
              taskCommunicatorManagerInterface, workingDirectory, containerLauncherIndex, isPureLocalMode);
    }

    @Override
    ContainerLauncher createYarnContainerLauncher(
        ContainerLauncherContext containerLauncherContext) {
      yarnContainerLauncherCreated.set(true);
      testContainerLaunchers.add(yarnContainerLauncher);
      return yarnContainerLauncher;
    }

    @Override
    ContainerLauncher createUberContainerLauncher(ContainerLauncherContext containerLauncherContext,
                                                  AppContext context,
                                                  TaskCommunicatorManagerInterface taskCommunicatorManagerInterface,
                                                  String workingDirectory,
                                                  boolean isPureLocalMode) {
      uberContainerLauncherCreated.set(true);
      testContainerLaunchers.add(uberContainerlauncher);
      return uberContainerlauncher;
    }

    @Override
    ContainerLauncher createCustomContainerLauncher(
        ContainerLauncherContext containerLauncherContext,
        NamedEntityDescriptor containerLauncherDescriptor) throws TezException {
      ContainerLauncher spyLauncher = spy(super.createCustomContainerLauncher(
          containerLauncherContext, containerLauncherDescriptor));
      testContainerLaunchers.add(spyLauncher);
      return spyLauncher;
    }

    public int getNumContainerLaunchers() {
      return numContainerLaunchers.get();
    }

    public boolean getYarnContainerLauncherCreated() {
      return yarnContainerLauncherCreated.get();
    }

    public boolean getUberContainerLauncherCreated() {
      return uberContainerLauncherCreated.get();
    }

    public String getContainerLauncherName(int containerLauncherIndex) {
      return containerLauncherNames.get(containerLauncherIndex);
    }

    public ContainerLauncher getTestContainerLauncher(int containerLauncherIndex) {
      return testContainerLaunchers.get(containerLauncherIndex);
    }

    public ContainerLauncherContext getContainerLauncherContext(int containerLauncherIndex) {
      return containerLauncherContexts.get(containerLauncherIndex);
    }
  }

  public static class FakeContainerLauncher extends ContainerLauncher {

    public FakeContainerLauncher(
        ContainerLauncherContext containerLauncherContext) {
      super(containerLauncherContext);
    }

    @Override
    public void launchContainer(ContainerLaunchRequest launchRequest) {

    }

    @Override
    public void stopContainer(ContainerStopRequest stopRequest) {

    }
  }

  private static final String DAG_NAME = "dagName";
  private static final int DAG_INDEX = 1;
  public static class ContainerLauncherForTest extends ContainerLauncher {

    public ContainerLauncherForTest(
        ContainerLauncherContext containerLauncherContext) {
      super(containerLauncherContext);
    }

    @Override
    public void launchContainer(ContainerLaunchRequest launchRequest) throws
        ServicePluginException {
      getContext().reportError(ServicePluginErrorDefaults.INCONSISTENT_STATE, "ReportedFatalError", null);
    }

    @Override
    public void stopContainer(ContainerStopRequest stopRequest) throws ServicePluginException {
      getContext()
          .reportError(ServicePluginErrorDefaults.SERVICE_UNAVAILABLE, "ReportError", new DagInfoImplForTest(DAG_INDEX, DAG_NAME));
    }
  }

}
