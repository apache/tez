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

package org.apache.tez.dag.app;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TaskCommunicator;
import org.apache.tez.dag.api.TaskCommunicatorContext;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.serviceplugins.api.ContainerEndReason;
import org.apache.tez.serviceplugins.api.TaskAttemptEndReason;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestTaskCommunicatorManager {

  @Before
  @After
  public void reset() {
    TaskCommManagerForMultipleCommTest.reset();
  }

  @Test(timeout = 5000)
  public void testNoTaskCommSpecified() throws IOException, TezException {

    AppContext appContext = mock(AppContext.class);
    TaskHeartbeatHandler thh = mock(TaskHeartbeatHandler.class);
    ContainerHeartbeatHandler chh = mock(ContainerHeartbeatHandler.class);

    try {
      new TaskCommManagerForMultipleCommTest(appContext, thh, chh, null);
      fail("Initialization should have failed without a TaskComm specified");
    } catch (IllegalArgumentException e) {

    }


  }

  @Test(timeout = 5000)
  public void testCustomTaskCommSpecified() throws IOException, TezException {

    AppContext appContext = mock(AppContext.class);
    TaskHeartbeatHandler thh = mock(TaskHeartbeatHandler.class);
    ContainerHeartbeatHandler chh = mock(ContainerHeartbeatHandler.class);

    String customTaskCommName = "customTaskComm";
    List<NamedEntityDescriptor> taskCommDescriptors = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload customPayload = UserPayload.create(bb);
    taskCommDescriptors.add(
        new NamedEntityDescriptor(customTaskCommName, FakeTaskComm.class.getName())
            .setUserPayload(customPayload));

    TaskCommManagerForMultipleCommTest tcm =
        new TaskCommManagerForMultipleCommTest(appContext, thh, chh, taskCommDescriptors);

    try {
      tcm.init(new Configuration(false));
      tcm.start();

      assertEquals(1, tcm.getNumTaskComms());
      assertFalse(tcm.getYarnTaskCommCreated());
      assertFalse(tcm.getUberTaskCommCreated());

      assertEquals(customTaskCommName, tcm.getTaskCommName(0));
      assertEquals(bb, tcm.getTaskCommContext(0).getInitialUserPayload().getPayload());

    } finally {
      tcm.stop();
    }
  }

  @Test(timeout = 5000)
  public void testMultipleTaskComms() throws IOException, TezException {

    AppContext appContext = mock(AppContext.class);
    TaskHeartbeatHandler thh = mock(TaskHeartbeatHandler.class);
    ContainerHeartbeatHandler chh = mock(ContainerHeartbeatHandler.class);
    Configuration conf = new Configuration(false);
    conf.set("testkey", "testvalue");
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);

    String customTaskCommName = "customTaskComm";
    List<NamedEntityDescriptor> taskCommDescriptors = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload customPayload = UserPayload.create(bb);
    taskCommDescriptors.add(
        new NamedEntityDescriptor(customTaskCommName, FakeTaskComm.class.getName())
            .setUserPayload(customPayload));
    taskCommDescriptors
        .add(new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null).setUserPayload(defaultPayload));

    TaskCommManagerForMultipleCommTest tcm =
        new TaskCommManagerForMultipleCommTest(appContext, thh, chh, taskCommDescriptors);

    try {
      tcm.init(new Configuration(false));
      tcm.start();

      assertEquals(2, tcm.getNumTaskComms());
      assertTrue(tcm.getYarnTaskCommCreated());
      assertFalse(tcm.getUberTaskCommCreated());

      assertEquals(customTaskCommName, tcm.getTaskCommName(0));
      assertEquals(bb, tcm.getTaskCommContext(0).getInitialUserPayload().getPayload());

      assertEquals(TezConstants.getTezYarnServicePluginName(), tcm.getTaskCommName(1));
      Configuration confParsed = TezUtils
          .createConfFromUserPayload(tcm.getTaskCommContext(1).getInitialUserPayload());
      assertEquals("testvalue", confParsed.get("testkey"));
    } finally {
      tcm.stop();
    }
  }

  @Test(timeout = 5000)
  public void testEventRouting() throws Exception {

    AppContext appContext = mock(AppContext.class, RETURNS_DEEP_STUBS);
    NodeId nodeId = NodeId.newInstance("host1", 3131);
    when(appContext.getAllContainers().get(any(ContainerId.class)).getContainer().getNodeId())
        .thenReturn(nodeId);
    TaskHeartbeatHandler thh = mock(TaskHeartbeatHandler.class);
    ContainerHeartbeatHandler chh = mock(ContainerHeartbeatHandler.class);
    Configuration conf = new Configuration(false);
    conf.set("testkey", "testvalue");
    UserPayload defaultPayload = TezUtils.createUserPayloadFromConf(conf);

    String customTaskCommName = "customTaskComm";
    List<NamedEntityDescriptor> taskCommDescriptors = new LinkedList<>();
    ByteBuffer bb = ByteBuffer.allocate(4);
    bb.putInt(0, 3);
    UserPayload customPayload = UserPayload.create(bb);
    taskCommDescriptors.add(
        new NamedEntityDescriptor(customTaskCommName, FakeTaskComm.class.getName())
            .setUserPayload(customPayload));
    taskCommDescriptors
        .add(new NamedEntityDescriptor(TezConstants.getTezYarnServicePluginName(), null).setUserPayload(defaultPayload));

    TaskCommManagerForMultipleCommTest tcm =
        new TaskCommManagerForMultipleCommTest(appContext, thh, chh, taskCommDescriptors);

    try {
      tcm.init(new Configuration(false));
      tcm.start();

      assertEquals(2, tcm.getNumTaskComms());
      assertTrue(tcm.getYarnTaskCommCreated());
      assertFalse(tcm.getUberTaskCommCreated());

      verify(tcm.getTestTaskComm(0)).initialize();
      verify(tcm.getTestTaskComm(0)).start();
      verify(tcm.getTestTaskComm(1)).initialize();
      verify(tcm.getTestTaskComm(1)).start();


      ContainerId containerId1 = mock(ContainerId.class);
      tcm.registerRunningContainer(containerId1, 0);
      verify(tcm.getTestTaskComm(0)).registerRunningContainer(eq(containerId1), eq("host1"),
          eq(3131));

      ContainerId containerId2 = mock(ContainerId.class);
      tcm.registerRunningContainer(containerId2, 1);
      verify(tcm.getTestTaskComm(1)).registerRunningContainer(eq(containerId2), eq("host1"),
          eq(3131));

    } finally {
      tcm.stop();
      verify(tcm.getTaskCommunicator(0)).shutdown();
      verify(tcm.getTaskCommunicator(1)).shutdown();
    }
  }


  static class TaskCommManagerForMultipleCommTest extends TaskCommunicatorManager {

    // All variables setup as static since methods being overridden are invoked by the ContainerLauncherRouter ctor,
    // and regular variables will not be initialized at this point.
    private static final AtomicInteger numTaskComms = new AtomicInteger(0);
    private static final Set<Integer> taskCommIndices = new HashSet<>();
    private static final TaskCommunicator yarnTaskComm = mock(TaskCommunicator.class);
    private static final TaskCommunicator uberTaskComm = mock(TaskCommunicator.class);
    private static final AtomicBoolean yarnTaskCommCreated = new AtomicBoolean(false);
    private static final AtomicBoolean uberTaskCommCreated = new AtomicBoolean(false);

    private static final List<TaskCommunicatorContext> taskCommContexts =
        new LinkedList<>();
    private static final List<String> taskCommNames = new LinkedList<>();
    private static final List<TaskCommunicator> testTaskComms = new LinkedList<>();


    public static void reset() {
      numTaskComms.set(0);
      taskCommIndices.clear();
      yarnTaskCommCreated.set(false);
      uberTaskCommCreated.set(false);
      taskCommContexts.clear();
      taskCommNames.clear();
      testTaskComms.clear();
    }

    public TaskCommManagerForMultipleCommTest(AppContext context,
                                              TaskHeartbeatHandler thh,
                                              ContainerHeartbeatHandler chh,
                                              List<NamedEntityDescriptor> taskCommunicatorDescriptors) throws TezException {
      super(context, thh, chh, taskCommunicatorDescriptors);
    }

    @Override
    TaskCommunicator createTaskCommunicator(NamedEntityDescriptor taskCommDescriptor,
                                            int taskCommIndex) throws TezException {
      numTaskComms.incrementAndGet();
      boolean added = taskCommIndices.add(taskCommIndex);
      assertTrue("Cannot add multiple taskComms with the same index", added);
      taskCommNames.add(taskCommDescriptor.getEntityName());
      return super.createTaskCommunicator(taskCommDescriptor, taskCommIndex);
    }

    @Override
    TaskCommunicator createDefaultTaskCommunicator(
        TaskCommunicatorContext taskCommunicatorContext) {
      taskCommContexts.add(taskCommunicatorContext);
      yarnTaskCommCreated.set(true);
      testTaskComms.add(yarnTaskComm);
      return yarnTaskComm;
    }

    @Override
    TaskCommunicator createUberTaskCommunicator(TaskCommunicatorContext taskCommunicatorContext) {
      taskCommContexts.add(taskCommunicatorContext);
      uberTaskCommCreated.set(true);
      testTaskComms.add(uberTaskComm);
      return uberTaskComm;
    }

    @Override
    TaskCommunicator createCustomTaskCommunicator(TaskCommunicatorContext taskCommunicatorContext,
                                                  NamedEntityDescriptor taskCommDescriptor) throws TezException {
      taskCommContexts.add(taskCommunicatorContext);
      TaskCommunicator spyComm =
          spy(super.createCustomTaskCommunicator(taskCommunicatorContext, taskCommDescriptor));
      testTaskComms.add(spyComm);
      return spyComm;
    }

    public static int getNumTaskComms() {
      return numTaskComms.get();
    }

    public static boolean getYarnTaskCommCreated() {
      return yarnTaskCommCreated.get();
    }

    public static boolean getUberTaskCommCreated() {
      return uberTaskCommCreated.get();
    }

    public static TaskCommunicatorContext getTaskCommContext(int taskCommIndex) {
      return taskCommContexts.get(taskCommIndex);
    }

    public static String getTaskCommName(int taskCommIndex) {
      return taskCommNames.get(taskCommIndex);
    }

    public static TaskCommunicator getTestTaskComm(int taskCommIndex) {
      return testTaskComms.get(taskCommIndex);
    }
  }

  public static class FakeTaskComm extends TaskCommunicator {

    public FakeTaskComm(TaskCommunicatorContext taskCommunicatorContext) {
      super(taskCommunicatorContext);
    }

    @Override
    public void registerRunningContainer(ContainerId containerId, String hostname, int port) {

    }

    @Override
    public void registerContainerEnd(ContainerId containerId, ContainerEndReason endReason, String diagnostics) {

    }

    @Override
    public void registerRunningTaskAttempt(ContainerId containerId, TaskSpec taskSpec,
                                           Map<String, LocalResource> additionalResources,
                                           Credentials credentials, boolean credentialsChanged,
                                           int priority) {

    }

    @Override
    public void unregisterRunningTaskAttempt(TezTaskAttemptID taskAttemptID,
                                             TaskAttemptEndReason endReason, String diagnostics) {

    }

    @Override
    public InetSocketAddress getAddress() {
      return null;
    }

    @Override
    public void onVertexStateUpdated(VertexStateUpdate stateUpdate) throws Exception {

    }

    @Override
    public void dagComplete(String dagName) {

    }

    @Override
    public Object getMetaInfo() {
      return null;
    }
  }
}
