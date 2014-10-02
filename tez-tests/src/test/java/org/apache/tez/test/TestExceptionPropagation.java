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

package org.apache.tez.test;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSinkDescriptor;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;

public class TestExceptionPropagation {

  private static final Log LOG = LogFactory
      .getLog(TestExceptionPropagation.class);

  private static TezConfiguration tezConf;
  private static Configuration conf = new Configuration();
  private static MiniTezCluster miniTezCluster = null;
  private static String TEST_ROOT_DIR = "target" + Path.SEPARATOR
      + TestExceptionPropagation.class.getName() + "-tmpDir";
  private static MiniDFSCluster dfsCluster = null;
  private static FileSystem remoteFs = null;

  private static TezClient tezSession = null;
  private static TezClient tezClient = null;

  private void startMiniTezCluster() {
    LOG.info("Starting mini clusters");
    try {
      conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, TEST_ROOT_DIR);
      dfsCluster =
          new MiniDFSCluster.Builder(conf).numDataNodes(3).format(true)
              .racks(null).build();
      remoteFs = dfsCluster.getFileSystem();
    } catch (IOException io) {
      throw new RuntimeException("problem starting mini dfs cluster", io);
    }
    miniTezCluster =
        new MiniTezCluster(TestExceptionPropagation.class.getName(), 1, 1, 1);
    Configuration miniTezconf = new Configuration(conf);
    miniTezconf.setInt(YarnConfiguration.RM_AM_MAX_ATTEMPTS, 4);
    miniTezconf.set("fs.defaultFS", remoteFs.getUri().toString()); // use HDFS
    miniTezCluster.init(miniTezconf);
    miniTezCluster.start();
  }

  private void stopTezMiniCluster() {
    if (miniTezCluster != null) {
      try {
        LOG.info("Stopping MiniTezCluster");
        miniTezCluster.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    if (dfsCluster != null) {
      try {
        LOG.info("Stopping DFSCluster");
        dfsCluster.shutdown();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  private void startSessionClient() throws Exception {
    LOG.info("Starting session");
    tezConf = new TezConfiguration();
    tezConf.setInt(TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, 0);
    tezConf
        .setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);
    tezConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 4);
    tezConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 500);
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx256m");
    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, true);
    // for local mode
    tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf.set("fs.defaultFS", "file:///");
    tezConf.setBoolean(
        TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);

    tezSession = TezClient.create("TestExceptionPropagation", tezConf);
    tezSession.start();
  }

  private void stopSessionClient() {
    if (tezSession != null) {
      try {
        LOG.info("Stopping Tez Session");
        tezSession.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    tezSession = null;
  }

  private void startNonSessionClient() throws Exception {
    LOG.info("Starting Client");
    tezConf = new TezConfiguration(miniTezCluster.getConfig());
    tezConf.setInt(TezConfiguration.DAG_RECOVERY_MAX_UNFLUSHED_EVENTS, 0);
    tezConf
        .setBoolean(TezConfiguration.TEZ_AM_NODE_BLACKLISTING_ENABLED, false);
    tezConf.setInt(TezConfiguration.TEZ_AM_MAX_APP_ATTEMPTS, 4);
    tezConf.setInt(TezConfiguration.TEZ_AM_RESOURCE_MEMORY_MB, 500);
    tezConf.set(TezConfiguration.TEZ_AM_LAUNCH_CMD_OPTS, " -Xmx256m");
    tezConf.setBoolean(TezConfiguration.TEZ_AM_SESSION_MODE, false);

    tezClient = TezClient.create("TestExceptionPropagation", tezConf);
    tezClient.start();
  }

  private void stopNonSessionClient() {
    if (tezClient != null) {
      try {
        LOG.info("Stopping Tez Client");
        tezClient.stop();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    tezClient = null;
  }

  /**
   * verify the diagnostics in DAGStatus is correct in session mode, using local
   * mode for fast speed
   * 
   * @throws Exception
   * 
   */
  @Test(timeout = 120000)
  public void testExceptionPropagationSession() throws Exception {
    try {
      startSessionClient();
      for (ExceptionLocation exLocation : ExceptionLocation.values()) {
        LOG.info("Session mode, Test for Exception from:" + exLocation.name());
        DAG dag = createDAG(exLocation);
        DAGClient dagClient = tezSession.submitDAG(dag);
        DAGStatus dagStatus = dagClient.waitForCompletion();
        String diagnostics = StringUtils.join(dagStatus.getDiagnostics(), ",");
        LOG.info("Diagnostics:" + diagnostics);
        assertTrue(diagnostics.contains(exLocation.name()));
      }
    } finally {
      stopSessionClient();
    }
  }

  /**
   * verify the diagnostics in {@link DAGStatus} is correct in non-session mode,
   * and also verify that diagnostics from {@link DAGStatus} should match that
   * from {@link ApplicationReport}
   * 
   * @throws Exception
   */
  @Test(timeout = 120000)
  public void testExceptionPropagationNonSession() throws Exception {
    try {
      startMiniTezCluster();
      startNonSessionClient();

      ExceptionLocation exLocation = ExceptionLocation.INPUT_START;
      LOG.info("NonSession mode, Test for Exception from:" + exLocation.name());
      DAG dag = createDAG(exLocation);
      DAGClient dagClient = tezClient.submitDAG(dag);
      DAGStatus dagStatus = dagClient.waitForCompletion();
      String diagnostics = StringUtils.join(dagStatus.getDiagnostics(), ",");
      LOG.info("Diagnostics:" + diagnostics);
      assertTrue(diagnostics.contains(exLocation.name()));

      // wait for app complete (unregisterApplicationMaster is done)
      ApplicationId appId = tezClient.getAppMasterApplicationId();
      YarnClient yarnClient = YarnClient.createYarnClient();
      yarnClient.init(tezConf);
      yarnClient.start();
      Set<YarnApplicationState> FINAL_APPLICATION_STATES =
          EnumSet.of(YarnApplicationState.KILLED, YarnApplicationState.FAILED,
              YarnApplicationState.FINISHED);
      ApplicationReport appReport = null;
      while (true) {
        appReport = yarnClient.getApplicationReport(appId);
        Thread.sleep(1000);
        LOG.info("FinalAppStatus:" + appReport.getFinalApplicationStatus());
        LOG.info("Diagnostics from appReport:" + appReport.getDiagnostics());
        if (FINAL_APPLICATION_STATES.contains(appReport
            .getYarnApplicationState())) {
          break;
        }
      }
      // wait for 1 second and call getApplicationReport again to ensure get the
      // diagnostics
      // TODO remove it after YARN-2560
      Thread.sleep(1000);
      appReport = yarnClient.getApplicationReport(appId);

      LOG.info("FinalAppStatus:" + appReport.getFinalApplicationStatus());
      LOG.info("Diagnostics from appReport:" + appReport.getDiagnostics());
      assertTrue(appReport.getDiagnostics().contains(exLocation.name()));
      // use "\n" as separator, because we also use it in Tez internally when
      // assembling the application diagnostics.
      assertEquals(StringUtils.join(dagStatus.getDiagnostics(), "\n").trim(),
          appReport.getDiagnostics().trim());
    } finally {
      stopNonSessionClient();
      Thread.sleep(10*1000);
      stopTezMiniCluster();
    }
  }

  public static enum ExceptionLocation {
    INPUT_START, INPUT_GET_READER, INPUT_HANDLE_EVENTS, INPUT_CLOSE, INPUT_INITIALIZE, OUTPUT_START, OUTPUT_GET_WRITER,
    // Not Supported yet
    // OUTPUT_HANDLE_EVENTS,
    OUTPUT_CLOSE, OUTPUT_INITIALIZE,
    // Not Supported yet
    // PROCESSOR_HANDLE_EVENTS
    PROCESSOR_RUN_ERROR, PROCESSOR_CLOSE_ERROR, PROCESSOR_INITIALIZE_ERROR,
    PROCESSOR_RUN_EXCEPTION, PROCESSOR_CLOSE_EXCEPTION, PROCESSOR_INITIALIZE_EXCEPTION,
  }

  /**
   * create a DAG with single vertex, set payload on Input/Output/Processor to
   * control where throw exception
   * 
   * @param exLocation
   * @return
   */
  private DAG createDAG(ExceptionLocation exLocation) {
    DAG dag = DAG.create("dag_" + exLocation.name());
    UserPayload payload =
        UserPayload.create(ByteBuffer.wrap(exLocation.name().getBytes()));
    Vertex v1 =
        Vertex.create("v1", ProcessorWithException.getProcDesc(payload), 1);
    InputDescriptor inputDesc = InputWithException.getInputDesc(payload);
    InputInitializerDescriptor iiDesc =
        InputInitializerWithException.getIIDesc(payload);
    v1.addDataSource("input",
        DataSourceDescriptor.create(inputDesc, iiDesc, null));
    OutputDescriptor outputDesc = OutputWithException.getOutputDesc(payload);
    v1.addDataSink("output", DataSinkDescriptor.create(outputDesc, null, null));
    dag.addVertex(v1);
    return dag;
  }

  public static class InputInitializerWithException extends InputInitializer {

    private ExceptionLocation exLocation;

    public InputInitializerWithException(
        InputInitializerContext initializerContext) {
      super(initializerContext);
      this.exLocation =
          ExceptionLocation.valueOf(new String(getContext().getUserPayload()
              .deepCopyAsArray()));
    }

    @Override
    public List<Event> initialize() throws Exception {
      List<Event> events = new ArrayList<Event>();
      events.add(InputDataInformationEvent.createWithObjectPayload(0, null));
      return events;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events)
        throws Exception {
    }

    public static InputInitializerDescriptor getIIDesc(UserPayload payload) {
      return InputInitializerDescriptor.create(
          InputInitializerWithException.class.getName())
          .setUserPayload(payload);
    }
  }

  public static class InputWithException extends AbstractLogicalInput {

    private ExceptionLocation exLocation;
    private Object condition = new Object();

    public InputWithException(InputContext inputContext, int numPhysicalInputs) {
      super(inputContext, numPhysicalInputs);
      this.exLocation =
          ExceptionLocation.valueOf(new String(getContext().getUserPayload()
              .deepCopyAsArray()));
    }

    @Override
    public void start() throws Exception {
      if (this.exLocation == ExceptionLocation.INPUT_START) {
        throw new Exception(this.exLocation.name());
      }
    }

    @Override
    public Reader getReader() throws Exception {
      if (this.exLocation == ExceptionLocation.INPUT_HANDLE_EVENTS) {
        synchronized (condition) {
          // wait for exception thrown from handleEvents. Otherwise,
          // processor may exit before the exception from handleEvents is
          // caught.
          condition.wait();
        }
      }
      if (this.exLocation == ExceptionLocation.INPUT_GET_READER) {
        throw new Exception(this.exLocation.name());
      }
      return null;
    }

    @Override
    public void handleEvents(List<Event> inputEvents) throws Exception {
      if (this.exLocation == ExceptionLocation.INPUT_HANDLE_EVENTS) {
        throw new Exception(this.exLocation.name());
      }
    }

    @Override
    public List<Event> close() throws Exception {
      if (this.exLocation == ExceptionLocation.INPUT_CLOSE) {
        throw new Exception(this.exLocation.name());
      }
      return null;
    }

    @Override
    public List<Event> initialize() throws Exception {
      getContext().requestInitialMemory(0l, null); // mandatory call
      if (this.exLocation == ExceptionLocation.INPUT_INITIALIZE) {
        throw new Exception(this.exLocation.name());
      }
      return null;
    }

    public static InputDescriptor getInputDesc(UserPayload payload) {
      return InputDescriptor.create(InputWithException.class.getName())
          .setUserPayload(payload);
    }
  }

  public static class OutputWithException extends AbstractLogicalOutput {

    private ExceptionLocation exLocation;

    public OutputWithException(OutputContext outputContext,
        int numPhysicalOutputs) {
      super(outputContext, numPhysicalOutputs);
      this.exLocation =
          ExceptionLocation.valueOf(new String(getContext().getUserPayload()
              .deepCopyAsArray()));
    }

    @Override
    public void start() throws Exception {
      if (this.exLocation == ExceptionLocation.OUTPUT_START) {
        throw new Exception(this.exLocation.name());
      }

    }

    @Override
    public Writer getWriter() throws Exception {
      if (this.exLocation == ExceptionLocation.OUTPUT_GET_WRITER) {
        throw new Exception(this.exLocation.name());
      }
      return null;
    }

    @Override
    public void handleEvents(List<Event> outputEvents) {
    }

    @Override
    public List<Event> close() throws Exception {
      if (this.exLocation == ExceptionLocation.OUTPUT_CLOSE) {
        throw new RuntimeException(this.exLocation.name());
      }
      return null;
    }

    @Override
    public List<Event> initialize() throws Exception {
      getContext().requestInitialMemory(0l, null); // mandatory call
      if (this.exLocation == ExceptionLocation.OUTPUT_INITIALIZE) {
        throw new RuntimeException(this.exLocation.name());
      }
      return null;
    }

    public static OutputDescriptor getOutputDesc(UserPayload payload) {
      return OutputDescriptor.create(OutputWithException.class.getName())
          .setUserPayload(payload);
    }
  }

  public static class ProcessorWithException extends AbstractLogicalIOProcessor {

    private ExceptionLocation exLocation;

    public ProcessorWithException(ProcessorContext context) {
      super(context);
      this.exLocation =
          ExceptionLocation.valueOf(new String(getContext().getUserPayload()
              .deepCopyAsArray()));
    }

    @Override
    public void run(Map<String, LogicalInput> inputs,
        Map<String, LogicalOutput> outputs) throws Exception {
      InputWithException input = (InputWithException) inputs.get("input");
      input.start();
      input.getReader();

      OutputWithException output = (OutputWithException) outputs.get("output");
      output.start();
      output.getWriter();

      if (this.exLocation == ExceptionLocation.PROCESSOR_RUN_ERROR) {
        throw new Error(this.exLocation.name());
      } else if (this.exLocation == ExceptionLocation.PROCESSOR_RUN_EXCEPTION) {
        throw new Exception(this.exLocation.name());
      }
    }

    @Override
    public void handleEvents(List<Event> processorEvents) {
    }

    @Override
    public void close() throws Exception {
      if (this.exLocation == ExceptionLocation.PROCESSOR_CLOSE_ERROR) {
        throw new Error(this.exLocation.name());
      } else if (this.exLocation == ExceptionLocation.PROCESSOR_CLOSE_EXCEPTION) {
        throw new Exception(this.exLocation.name());
      }
    }

    @Override
    public void initialize() throws Exception {
      if (this.exLocation == ExceptionLocation.PROCESSOR_INITIALIZE_ERROR) {
        throw new Error(this.exLocation.name());
      } else if (this.exLocation == ExceptionLocation.PROCESSOR_INITIALIZE_EXCEPTION) {
        throw new Exception(this.exLocation.name());
      }
    }

    public static ProcessorDescriptor getProcDesc(UserPayload payload) {
      return ProcessorDescriptor.create(ProcessorWithException.class.getName())
          .setUserPayload(payload);
    }
  }
}
