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

package org.apache.tez.dag.app.dag.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DagTypeConverters;
import org.apache.tez.dag.api.NamedEntityDescriptor;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex.VertexExecutionContext;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.api.records.DAGProtos.ConfigurationProto;
import org.apache.tez.dag.api.records.DAGProtos.PlanKeyValuePair;
import org.apache.tez.dag.api.records.DAGProtos.TezNamedEntityDescriptorProto;
import org.apache.tez.dag.api.records.DAGProtos.VertexPlan;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.DAGAppMaster;
import org.apache.tez.dag.app.TaskCommunicatorManagerInterface;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TaskSpecificLaunchCmdOption;
import org.junit.Assert;
import org.junit.Test;

/**
 * Contains additional tests for VertexImpl. Please avoid adding class parameters.
 */
public class TestVertexImpl2 {

  @Test(timeout = 5000)
  public void testTaskLoggingOptsPerLogger() {

    Configuration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_TASK_LOG_LEVEL, "DEBUG;org.apache.hadoop.ipc=INFO;org.apache.hadoop.server=INFO");

    LogTestInfoHolder testInfo = new LogTestInfoHolder();
    VertexWrapper vertexWrapper = createVertexWrapperForLogTests(testInfo, conf);

    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "DEBUG" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 0 ; i < testInfo.numTasks ; i++) {
      ContainerContext containerContext = vertexWrapper
          .vertex.getContainerContext(i);
      String javaOpts = containerContext.getJavaOpts();
      assertTrue(javaOpts.contains(testInfo.initialJavaOpts));
      for (String expectedCmd : expectedCommands) {
        assertTrue(javaOpts.contains(expectedCmd));
      }

      Map<String, String> env = containerContext.getEnvironment();
      String val = env.get(testInfo.envKey);
      assertEquals(testInfo.envVal, val);
      String logEnvVal = env.get(TezConstants.TEZ_CONTAINER_LOG_PARAMS);
      assertEquals("org.apache.hadoop.ipc=INFO;org.apache.hadoop.server=INFO", logEnvVal);
    }
  }

  @Test(timeout = 5000)
  public void testTaskLoggingOptsSimple() {

    Configuration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_TASK_LOG_LEVEL, "DEBUG");

    LogTestInfoHolder testInfo = new LogTestInfoHolder();
    VertexWrapper vertexWrapper = createVertexWrapperForLogTests(testInfo, conf);

    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "DEBUG" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 0 ; i < testInfo.numTasks ; i++) {
      ContainerContext containerContext = vertexWrapper.vertex.getContainerContext(i);
      String javaOpts = containerContext.getJavaOpts();
      assertTrue(javaOpts.contains(testInfo.initialJavaOpts));
      for (String expectedCmd : expectedCommands) {
        assertTrue(javaOpts.contains(expectedCmd));
      }

      Map<String, String> env = containerContext.getEnvironment();
      String val = env.get(testInfo.envKey);
      assertEquals(testInfo.envVal, val);
      String logEnvVal = env.get(TezConstants.TEZ_CONTAINER_LOG_PARAMS);
      assertNull(logEnvVal);
    }
  }

  @Test(timeout = 5000)
  public void testTaskSpecificLoggingOpts() {

    String vertexName = "testvertex";
    String customJavaOpts = "-Xmx128m";

    Configuration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_TASK_LOG_LEVEL, "INFO");
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST, vertexName + "[0,1,2]");
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LOG_LEVEL, "DEBUG;org.apache.tez=INFO");
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS, customJavaOpts);

    LogTestInfoHolder testInfo = new LogTestInfoHolder();
    VertexWrapper vertexWrapper = createVertexWrapperForLogTests(testInfo, conf);

    // Expected command opts for regular tasks
    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "INFO" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 3 ; i < testInfo.numTasks ; i++) {
      ContainerContext containerContext = vertexWrapper.vertex.getContainerContext(i);
      String javaOpts = containerContext.getJavaOpts();

      assertTrue(javaOpts.contains(testInfo.initialJavaOpts));
      for (String expectedCmd : expectedCommands) {
        assertTrue(javaOpts.contains(expectedCmd));
      }

      Map<String, String> env = containerContext.getEnvironment();
      String val = env.get(testInfo.envKey);
      assertEquals(testInfo.envVal, val);
      String logEnvVal = env.get(TezConstants.TEZ_CONTAINER_LOG_PARAMS);
      assertNull(logEnvVal);
    }

    // Expected command opts for instrumented tasks.
    expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "DEBUG" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 0 ; i < 3 ; i++) {
      ContainerContext containerContext = vertexWrapper.vertex.getContainerContext(i);
      String javaOpts = containerContext.getJavaOpts();

      assertTrue(javaOpts.contains(testInfo.initialJavaOpts));
      for (String expectedCmd : expectedCommands) {
        assertTrue(javaOpts.contains(expectedCmd));
      }

      Map<String, String> env = containerContext.getEnvironment();
      String val = env.get(testInfo.envKey);
      assertEquals(testInfo.envVal, val);
      String logEnvVal = env.get(TezConstants.TEZ_CONTAINER_LOG_PARAMS);
      assertEquals("org.apache.tez=INFO", logEnvVal);
    }
  }

  @Test(timeout = 5000)
  public void testTaskSpecificLoggingOpts2() {

    String vertexName = "testvertex";
    String customJavaOpts = "-Xmx128m";

    Configuration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_TASK_LOG_LEVEL, "WARN;org.apache.tez=INFO");
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS_LIST, vertexName + "[0,1,2]");
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LOG_LEVEL, "DEBUG");
    conf.set(TezConfiguration.TEZ_TASK_SPECIFIC_LAUNCH_CMD_OPTS, customJavaOpts);

    LogTestInfoHolder testInfo = new LogTestInfoHolder();
    VertexWrapper vertexWrapper = createVertexWrapperForLogTests(testInfo, conf);

    // Expected command opts for regular tasks
    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "WARN" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 3 ; i < testInfo.numTasks ; i++) {
      ContainerContext containerContext = vertexWrapper.vertex.getContainerContext(i);
      String javaOpts = containerContext.getJavaOpts();

      assertTrue(javaOpts.contains(testInfo.initialJavaOpts));
      for (String expectedCmd : expectedCommands) {
        assertTrue(javaOpts.contains(expectedCmd));
      }

      Map<String, String> env = containerContext.getEnvironment();
      String val = env.get(testInfo.envKey);
      assertEquals(testInfo.envVal, val);
      String logEnvVal = env.get(TezConstants.TEZ_CONTAINER_LOG_PARAMS);
      assertEquals("org.apache.tez=INFO", logEnvVal);
    }

    // Expected command opts for instrumented tasks.
    expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "DEBUG" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 0 ; i < 3 ; i++) {
      ContainerContext containerContext = vertexWrapper.vertex.getContainerContext(i);
      String javaOpts = containerContext.getJavaOpts();

      assertTrue(javaOpts.contains(testInfo.initialJavaOpts));
      for (String expectedCmd : expectedCommands) {
        assertTrue(javaOpts.contains(expectedCmd));
      }

      Map<String, String> env = containerContext.getEnvironment();
      String val = env.get(testInfo.envKey);
      assertEquals(testInfo.envVal, val);
      String logEnvVal = env.get(TezConstants.TEZ_CONTAINER_LOG_PARAMS);
      assertNull(logEnvVal);
    }
  }

  @Test(timeout = 5000)
  public void testNullExecutionContexts() {

    ExecutionContextTestInfoHolder info = new ExecutionContextTestInfoHolder(null, null);
    VertexWrapper vertexWrapper = createVertexWrapperForExecutionContextTest(info);

    assertEquals(0, vertexWrapper.vertex.taskSchedulerIdentifier);
    assertEquals(0, vertexWrapper.vertex.containerLauncherIdentifier);
    assertEquals(0, vertexWrapper.vertex.taskCommunicatorIdentifier);
  }

  @Test(timeout = 5000)
  public void testDefaultExecContextViaDag() {
    VertexExecutionContext defaultExecContext = VertexExecutionContext.create(
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.TASK_SCHEDULER_NAME_BASE, 0),
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.CONTAINER_LAUNCHER_NAME_BASE, 2),
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.TASK_COMM_NAME_BASE, 2));
    ExecutionContextTestInfoHolder info =
        new ExecutionContextTestInfoHolder(null, defaultExecContext, 3);
    VertexWrapper vertexWrapper = createVertexWrapperForExecutionContextTest(info);

    assertEquals(0, vertexWrapper.vertex.taskSchedulerIdentifier);
    assertEquals(2, vertexWrapper.vertex.containerLauncherIdentifier);
    assertEquals(2, vertexWrapper.vertex.taskCommunicatorIdentifier);
  }

  @Test(timeout = 5000)
  public void testVertexExecutionContextOnly() {
    VertexExecutionContext vertexExecutionContext = VertexExecutionContext.create(
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.TASK_SCHEDULER_NAME_BASE, 1),
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.CONTAINER_LAUNCHER_NAME_BASE, 1),
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.TASK_COMM_NAME_BASE, 1));
    ExecutionContextTestInfoHolder info =
        new ExecutionContextTestInfoHolder(vertexExecutionContext, null, 3);
    VertexWrapper vertexWrapper = createVertexWrapperForExecutionContextTest(info);

    assertEquals(1, vertexWrapper.vertex.taskSchedulerIdentifier);
    assertEquals(1, vertexWrapper.vertex.containerLauncherIdentifier);
    assertEquals(1, vertexWrapper.vertex.taskCommunicatorIdentifier);
  }

  @Test(timeout = 5000)
  public void testVertexExecutionContextOverride() {
    VertexExecutionContext defaultExecContext = VertexExecutionContext.create(
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.TASK_SCHEDULER_NAME_BASE, 0),
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.CONTAINER_LAUNCHER_NAME_BASE, 2),
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.TASK_COMM_NAME_BASE, 2));

    VertexExecutionContext vertexExecutionContext = VertexExecutionContext.create(
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.TASK_SCHEDULER_NAME_BASE, 1),
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.CONTAINER_LAUNCHER_NAME_BASE, 1),
        ExecutionContextTestInfoHolder
            .append(ExecutionContextTestInfoHolder.TASK_COMM_NAME_BASE, 1));
    ExecutionContextTestInfoHolder info =
        new ExecutionContextTestInfoHolder(vertexExecutionContext, defaultExecContext, 3);
    VertexWrapper vertexWrapper = createVertexWrapperForExecutionContextTest(info);

    assertEquals(1, vertexWrapper.vertex.taskSchedulerIdentifier);
    assertEquals(1, vertexWrapper.vertex.containerLauncherIdentifier);
    assertEquals(1, vertexWrapper.vertex.taskCommunicatorIdentifier);
  }


  private static class ExecutionContextTestInfoHolder {

    static final String TASK_SCHEDULER_NAME_BASE = "TASK_SCHEDULER";
    static final String CONTAINER_LAUNCHER_NAME_BASE = "CONTAINER_LAUNCHER";
    static final String TASK_COMM_NAME_BASE = "TASK_COMMUNICATOR";

    static String append(String base, int index) {
      return base + index;
    }

    final String vertexName;
    final VertexExecutionContext defaultExecutionContext;
    final VertexExecutionContext vertexExecutionContext;
    final BiMap<String, Integer> taskSchedulers = HashBiMap.create();
    final BiMap<String, Integer> containerLaunchers = HashBiMap.create();
    final BiMap<String, Integer> taskComms = HashBiMap.create();
    final AppContext appContext;

    public ExecutionContextTestInfoHolder(VertexExecutionContext vertexExecutionContext,
                                          VertexExecutionContext defaultDagExecutionContext) {
      this(vertexExecutionContext, defaultDagExecutionContext, 0);
    }

    public ExecutionContextTestInfoHolder(VertexExecutionContext vertexExecutionContext,
                                          VertexExecutionContext defaultDagExecitionContext,
                                          int numPlugins) {
      this.vertexName = "testvertex";
      this.vertexExecutionContext = vertexExecutionContext;
      this.defaultExecutionContext = defaultDagExecitionContext;
      if (numPlugins == 0) { // Add default container plugins only
        UserPayload defaultPayload;
        try {
          defaultPayload = TezUtils.createUserPayloadFromConf(new Configuration(false));
        } catch (IOException e) {
          throw new TezUncheckedException(e);
        }
        DAGAppMaster.parsePlugin(Lists.<NamedEntityDescriptor>newLinkedList(), taskSchedulers, null,
            true, false, defaultPayload);
        DAGAppMaster
            .parsePlugin(Lists.<NamedEntityDescriptor>newLinkedList(), containerLaunchers, null,
                true, false, defaultPayload);
        DAGAppMaster.parsePlugin(Lists.<NamedEntityDescriptor>newLinkedList(), taskComms, null,
            true, false, defaultPayload);
      } else { // Add N plugins, no YARN defaults
        List<TezNamedEntityDescriptorProto> schedulerList = new LinkedList<>();
        List<TezNamedEntityDescriptorProto> launcherList = new LinkedList<>();
        List<TezNamedEntityDescriptorProto> taskCommList = new LinkedList<>();
        for (int i = 0; i < numPlugins; i++) {
          schedulerList.add(TezNamedEntityDescriptorProto.newBuilder()
              .setName(append(TASK_SCHEDULER_NAME_BASE, i)).setEntityDescriptor(
                  DAGProtos.TezEntityDescriptorProto.newBuilder()
                      .setClassName(append(TASK_SCHEDULER_NAME_BASE, i))).build());
          launcherList.add(TezNamedEntityDescriptorProto.newBuilder()
              .setName(append(CONTAINER_LAUNCHER_NAME_BASE, i)).setEntityDescriptor(
                  DAGProtos.TezEntityDescriptorProto.newBuilder()
                      .setClassName(append(CONTAINER_LAUNCHER_NAME_BASE, i))).build());
          taskCommList.add(
              TezNamedEntityDescriptorProto.newBuilder().setName(append(TASK_COMM_NAME_BASE, i))
                  .setEntityDescriptor(
                      DAGProtos.TezEntityDescriptorProto.newBuilder()
                          .setClassName(append(TASK_COMM_NAME_BASE, i))).build());
        }
        DAGAppMaster.parsePlugin(Lists.<NamedEntityDescriptor>newLinkedList(), taskSchedulers,
            schedulerList, false, false, null);
        DAGAppMaster.parsePlugin(Lists.<NamedEntityDescriptor>newLinkedList(), containerLaunchers,
            launcherList, false, false, null);
        DAGAppMaster
            .parsePlugin(Lists.<NamedEntityDescriptor>newLinkedList(), taskComms, taskCommList,
                false, false, null);
      }

      this.appContext = createDefaultMockAppContext();
      DAG dag = appContext.getCurrentDAG();
      doReturn(defaultDagExecitionContext).when(dag).getDefaultExecutionContext();
      for (Map.Entry<String, Integer> entry : taskSchedulers.entrySet()) {
        doReturn(entry.getKey()).when(appContext).getTaskSchedulerName(entry.getValue());
        doReturn(entry.getValue()).when(appContext).getTaskScheduerIdentifier(entry.getKey());
      }
      for (Map.Entry<String, Integer> entry : containerLaunchers.entrySet()) {
        doReturn(entry.getKey()).when(appContext).getContainerLauncherName(entry.getValue());
        doReturn(entry.getValue()).when(appContext).getContainerLauncherIdentifier(entry.getKey());
      }
      for (Map.Entry<String, Integer> entry : taskComms.entrySet()) {
        doReturn(entry.getKey()).when(appContext).getTaskCommunicatorName(entry.getValue());
        doReturn(entry.getValue()).when(appContext).getTaskCommunicatorIdentifier(entry.getKey());
      }
    }
  }

  private VertexWrapper createVertexWrapperForExecutionContextTest(
      ExecutionContextTestInfoHolder vertexInfo) {
    VertexPlan vertexPlan = createVertexPlanForExeuctionContextTests(vertexInfo);
    VertexWrapper vertexWrapper =
        new VertexWrapper(vertexInfo.appContext, vertexPlan, new Configuration(false), true);
    return vertexWrapper;
  }

  private VertexPlan createVertexPlanForExeuctionContextTests(ExecutionContextTestInfoHolder info) {
    ConfigurationProto confProto = ConfigurationProto.newBuilder()
        .addConfKeyValues(PlanKeyValuePair.newBuilder().setKey("foo").setValue("bar").build())
        .addConfKeyValues(PlanKeyValuePair.newBuilder().setKey("foo1").setValue("bar2").build())
        .build();
    VertexPlan.Builder vertexPlanBuilder = VertexPlan.newBuilder()
        .setName(info.vertexName)
        .setVertexConf(confProto)
        .setTaskConfig(DAGProtos.PlanTaskConfiguration.newBuilder()
            .setNumTasks(10)
            .setJavaOpts("dontcare")
            .setMemoryMb(1024)
            .setVirtualCores(1)
            .setTaskModule("taskmodule")
            .build())
        .setType(DAGProtos.PlanVertexType.NORMAL);
    if (info.vertexExecutionContext != null) {
      vertexPlanBuilder
          .setExecutionContext(DagTypeConverters.convertToProto(info.vertexExecutionContext));
    }
    return vertexPlanBuilder.build();
  }

  private static class LogTestInfoHolder {
    final int numTasks = 10;
    final String initialJavaOpts = "initialJavaOpts";
    final String envKey = "key1";
    final String envVal = "val1";
    final String vertexName;

    public LogTestInfoHolder() {
      this("testvertex");
    }

    public LogTestInfoHolder(String vertexName) {
      this.vertexName = vertexName;
    }
  }

  private VertexWrapper createVertexWrapperForLogTests(LogTestInfoHolder logTestInfoHolder,
                                                       Configuration conf) {
    VertexPlan vertexPlan = createVertexPlanForLogTests(logTestInfoHolder);
    VertexWrapper vertexWrapper = new VertexWrapper(vertexPlan, conf);
    return vertexWrapper;
  }

  private VertexPlan createVertexPlanForLogTests(LogTestInfoHolder logTestInfoHolder) {
    VertexPlan vertexPlan = VertexPlan.newBuilder()
        .setName(logTestInfoHolder.vertexName)
        .setTaskConfig(DAGProtos.PlanTaskConfiguration.newBuilder()
            .setJavaOpts(logTestInfoHolder.initialJavaOpts)
            .setNumTasks(logTestInfoHolder.numTasks)
            .setMemoryMb(1024)
            .setVirtualCores(1)
            .setTaskModule("taskmodule")
            .addEnvironmentSetting(DAGProtos.PlanKeyValuePair.newBuilder()
                .setKey(logTestInfoHolder.envKey)
                .setValue(logTestInfoHolder.envVal)
                .build())
            .build())
        .setType(DAGProtos.PlanVertexType.NORMAL).build();
    return vertexPlan;
  }

  private static class VertexWrapper {

    final AppContext mockAppContext;
    final VertexImpl vertex;
    final VertexPlan vertexPlan;

    VertexWrapper(AppContext appContext, VertexPlan vertexPlan, Configuration conf,
                  boolean checkVertexOnlyConf) {
      if (appContext == null) {
        mockAppContext = createDefaultMockAppContext();
        DAG mockDag = mock(DAG.class);
        doReturn(new Credentials()).when(mockDag).getCredentials();
        doReturn(mockDag).when(mockAppContext).getCurrentDAG();
      } else {
        mockAppContext = appContext;
      }

      Configuration dagConf = new Configuration(false);
      dagConf.set("abc1", "xyz1");
      dagConf.set("foo1", "bar1");

      this.vertexPlan = vertexPlan;

      vertex =
          new VertexImpl(TezVertexID.fromString("vertex_1418197758681_0001_1_00"), vertexPlan,
              "testvertex", conf, mock(EventHandler.class), mock(TaskCommunicatorManagerInterface.class),
              mock(Clock.class), mock(TaskHeartbeatHandler.class), false, mockAppContext,
              VertexLocationHint.create(new LinkedList<TaskLocationHint>()), null,
              new TaskSpecificLaunchCmdOption(conf), mock(StateChangeNotifier.class),
              dagConf);

      if (checkVertexOnlyConf) {
        Assert.assertEquals("xyz1", vertex.vertexOnlyConf.get("abc1"));
        Assert.assertEquals("bar2", vertex.vertexOnlyConf.get("foo1"));
        Assert.assertEquals("bar", vertex.vertexOnlyConf.get("foo"));
      }

    }

    VertexWrapper(VertexPlan vertexPlan, Configuration conf) {
      this(null, vertexPlan, conf, false);
    }
  }

  private static AppContext createDefaultMockAppContext() {
    AppContext appContext = mock(AppContext.class);
    DAG mockDag = mock(DAG.class);
    doReturn(new Credentials()).when(mockDag).getCredentials();
    doReturn(mockDag).when(appContext).getCurrentDAG();
    return appContext;
  }
}
