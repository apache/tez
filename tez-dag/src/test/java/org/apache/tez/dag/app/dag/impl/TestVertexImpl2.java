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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.tez.dag.api.TaskLocationHint;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.VertexLocationHint;
import org.apache.tez.dag.api.records.DAGProtos;
import org.apache.tez.dag.app.AppContext;
import org.apache.tez.dag.app.ContainerContext;
import org.apache.tez.dag.app.TaskAttemptListener;
import org.apache.tez.dag.app.TaskHeartbeatHandler;
import org.apache.tez.dag.app.dag.DAG;
import org.apache.tez.dag.app.dag.StateChangeNotifier;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.dag.utils.TaskSpecificLaunchCmdOption;
import org.junit.Test;

/**
 * Contains additional tests for VertexImpl. Please avoid adding class parameters.
 */
public class TestVertexImpl2 {

  @Test(timeout = 5000)
  public void testTaskLoggingOptsPerLogger() {

    Configuration conf = new TezConfiguration();
    conf.set(TezConfiguration.TEZ_TASK_LOG_LEVEL, "DEBUG;org.apache.hadoop.ipc=INFO;org.apache.hadoop.server=INFO");

    LogTestInfoHolder testInfo = new LogTestInfoHolder(conf);

    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "DEBUG" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 0 ; i < testInfo.numTasks ; i++) {
      ContainerContext containerContext = testInfo.vertex.getContainerContext(i);
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

    LogTestInfoHolder testInfo = new LogTestInfoHolder(conf);

    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "DEBUG" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 0 ; i < testInfo.numTasks ; i++) {
      ContainerContext containerContext = testInfo.vertex.getContainerContext(i);
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

    LogTestInfoHolder testInfo = new LogTestInfoHolder(conf);

    // Expected command opts for regular tasks
    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "INFO" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 3 ; i < testInfo.numTasks ; i++) {
      ContainerContext containerContext = testInfo.vertex.getContainerContext(i);
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
      ContainerContext containerContext = testInfo.vertex.getContainerContext(i);
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

    LogTestInfoHolder testInfo = new LogTestInfoHolder(conf);

    // Expected command opts for regular tasks
    List<String> expectedCommands = new LinkedList<String>();
    expectedCommands.add("-Dlog4j.configuratorClass=org.apache.tez.common.TezLog4jConfigurator");
    expectedCommands.add("-Dlog4j.configuration=" + TezConstants.TEZ_CONTAINER_LOG4J_PROPERTIES_FILE);
    expectedCommands.add("-D" + YarnConfiguration.YARN_APP_CONTAINER_LOG_DIR + "=" +
        ApplicationConstants.LOG_DIR_EXPANSION_VAR);
    expectedCommands.add("-D" + TezConstants.TEZ_ROOT_LOGGER_NAME + "=" + "WARN" + "," +
        TezConstants.TEZ_CONTAINER_LOGGER_NAME);

    for (int i = 3 ; i < testInfo.numTasks ; i++) {
      ContainerContext containerContext = testInfo.vertex.getContainerContext(i);
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
      ContainerContext containerContext = testInfo.vertex.getContainerContext(i);
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


  private static class LogTestInfoHolder {

    final AppContext mockAppContext;
    final DAG mockDag;
    final VertexImpl vertex;
    final DAGProtos.VertexPlan vertexPlan;

    final int numTasks = 10;
    final String initialJavaOpts = "initialJavaOpts";
    final String envKey = "key1";
    final String envVal = "val1";

    LogTestInfoHolder(Configuration conf) {
      this(conf, "testvertex");
    }

    LogTestInfoHolder(Configuration conf, String vertexName) {
      mockAppContext = mock(AppContext.class);
      mockDag = mock(DAG.class);
      doReturn(new Credentials()).when(mockDag).getCredentials();
      doReturn(mockDag).when(mockAppContext).getCurrentDAG();

      vertexPlan = DAGProtos.VertexPlan.newBuilder()
          .setName(vertexName)
          .setTaskConfig(DAGProtos.PlanTaskConfiguration.newBuilder()
              .setJavaOpts(initialJavaOpts)
              .setNumTasks(numTasks)
              .setMemoryMb(1024)
              .setVirtualCores(1)
              .setTaskModule("taskmodule")
              .addEnvironmentSetting(DAGProtos.PlanKeyValuePair.newBuilder()
                  .setKey(envKey)
                  .setValue(envVal)
                  .build())
              .build())
          .setType(DAGProtos.PlanVertexType.NORMAL).build();

      vertex =
          new VertexImpl(TezVertexID.fromString("vertex_1418197758681_0001_1_00"), vertexPlan,
              "testvertex", conf, mock(EventHandler.class), mock(TaskAttemptListener.class),
              mock(Clock.class), mock(TaskHeartbeatHandler.class), false, mockAppContext,
              VertexLocationHint.create(new LinkedList<TaskLocationHint>()), null,
              new TaskSpecificLaunchCmdOption(conf), mock(StateChangeNotifier.class));
    }
  }
}
