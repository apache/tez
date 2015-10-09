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
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.DataSourceDescriptor;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeManagerPluginContext;
import org.apache.tez.dag.api.EdgeManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.InputInitializerDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.dag.api.event.VertexStateUpdate;
import org.apache.tez.dag.app.dag.impl.OneToOneEdgeManager;
import org.apache.tez.dag.app.dag.impl.RootInputVertexManager;
import org.apache.tez.dag.library.vertexmanager.InputReadyVertexManager;
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
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.test.TestAMRecovery.DoNothingProcessor;
import org.apache.tez.test.dag.MultiAttemptDAG.NoOpInput;
import org.junit.Test;

import com.google.common.collect.Lists;

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
  @Test(timeout = 600000)
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
        if (exLocation == ExceptionLocation.PROCESSOR_COUNTER_EXCEEDED) {
          assertTrue(diagnostics.contains("Too many counters"));
        } else {
          assertTrue(diagnostics.contains(exLocation.name()));
        }
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

      ExceptionLocation exLocation = ExceptionLocation.EM_GetNumSourceTaskPhysicalOutputs;
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
    PROCESSOR_COUNTER_EXCEEDED,

    // VM
    VM_INITIALIZE, VM_ON_ROOTVERTEX_INITIALIZE,VM_ON_SOURCETASK_COMPLETED, VM_ON_VERTEX_STARTED,
    VM_ON_VERTEXMANAGEREVENT_RECEIVED,

    // EdgeManager
    EM_Initialize, EM_GetNumDestinationTaskPhysicalInputs, EM_GetNumSourceTaskPhysicalOutputs,
    EM_RouteDataMovementEventToDestination, EM_GetNumDestinationConsumerTasks,
    EM_RouteInputErrorEventToSource,
    // Not Supported yet
    // EM_RouteInputSourceTaskFailedEventToDestination,

    // II
    II_Initialize, II_HandleInputInitializerEvents, II_OnVertexStateUpdated

  }

  /**
   * create a DAG with 2 vertices (v1 --> v2), set payload on Input/Output/Processor/VertexManagerPlugin to
   * control where throw exception
   * 
   * @param exLocation
   * @return
   * @throws IOException
   */
  private DAG createDAG(ExceptionLocation exLocation) throws IOException {
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
    v1.setVertexManagerPlugin(RootInputVertexManagerWithException.getVMDesc(payload));

    Vertex v2 = 
        Vertex.create("v2", DoNothingProcessor.getProcDesc(), 1);
    v2.addDataSource("input2",
        DataSourceDescriptor.create(InputDescriptor.create(NoOpInput.class.getName()),
          InputInitializerWithException2.getIIDesc(payload), null));

    dag.addVertex(v1)
      .addVertex(v2);
    if (exLocation.name().startsWith("EM_")) {
      dag.addEdge(Edge.create(v1, v2, EdgeProperty.create(
          EdgeManagerPluginDescriptor.create(CustomEdgeManager.class.getName())
            .setUserPayload(payload),
          DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
          OutputWithException.getOutputDesc(payload), InputWithException.getInputDesc(payload))));
    } else {
      // set Customized VertexManager here, it can't been used for CustomEdge
      v2.setVertexManagerPlugin(InputReadyVertexManagerWithException.getVMDesc(exLocation));
      dag.addEdge(Edge.create(v1, v2, EdgeProperty.create(DataMovementType.ONE_TO_ONE,
          DataSourceType.PERSISTED, SchedulingType.SEQUENTIAL,
          OutputWithException.getOutputDesc(payload), InputWithException.getInputDesc(payload))));
    }

    return dag;
  }

  // InputInitializer of vertex1
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

  // InputInitializer of vertex2
  public static class InputInitializerWithException2 extends InputInitializer {

    private ExceptionLocation exLocation;
    private Object condition = new Object();

    public InputInitializerWithException2(
        InputInitializerContext initializerContext) {
      super(initializerContext);
      this.exLocation =
          ExceptionLocation.valueOf(new String(getContext().getUserPayload()
              .deepCopyAsArray()));
    }

    @Override
    public List<Event> initialize() throws Exception {
      if (exLocation == ExceptionLocation.II_Initialize) {
        throw new Exception(exLocation.name());
      }
      if (exLocation == ExceptionLocation.II_OnVertexStateUpdated) {
        getContext().registerForVertexStateUpdates("v1", null);
      }

      if (exLocation == ExceptionLocation.II_HandleInputInitializerEvents
          || exLocation == ExceptionLocation.II_OnVertexStateUpdated) {
        // wait for handleInputInitializerEvent() and onVertexStateUpdated() is invoked
        synchronized (condition) {
          condition.wait();
        }
      }

      return null;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events)
        throws Exception {
      if (exLocation == ExceptionLocation.II_HandleInputInitializerEvents) {
        throw new RuntimeException(exLocation.name());
      }
    }

    @Override
    public void onVertexStateUpdated(VertexStateUpdate stateUpdate)
        throws Exception {
      if (exLocation == ExceptionLocation.II_OnVertexStateUpdated) {
        throw new Exception(exLocation.name());
      }
      super.onVertexStateUpdated(stateUpdate);
    }

    public static InputInitializerDescriptor getIIDesc(UserPayload payload) {
      return InputInitializerDescriptor.create(
          InputInitializerWithException2.class.getName())
          .setUserPayload(payload);
    }
  }

  // Input of vertex2
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
      } else if ( getContext().getSourceVertexName().equals("v1")) {
        if (this.exLocation == ExceptionLocation.EM_RouteInputErrorEventToSource
            || this.exLocation == ExceptionLocation.EM_GetNumDestinationConsumerTasks) {
          Event errorEvent = InputReadErrorEvent.create("read error", 0, 0);
          return Lists.newArrayList(errorEvent);
        }
      }
      return null;
    }

    public static InputDescriptor getInputDesc(UserPayload payload) {
      return InputDescriptor.create(InputWithException.class.getName())
          .setUserPayload(payload);
    }
  }

  // Output of vertex1
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
      } else if (this.exLocation == ExceptionLocation.VM_ON_VERTEXMANAGEREVENT_RECEIVED) {
        // send VertexManagerEvent to v2
        List<Event> events = new ArrayList<Event>();
        events.add(VertexManagerEvent.create("v2", ByteBuffer.wrap(new byte[0])));
        return events;
      } else if (this.exLocation == ExceptionLocation.EM_RouteDataMovementEventToDestination) {
        // send DataMovementEvent to v2
        List<Event> events = new ArrayList<Event>();
        events.add(DataMovementEvent.create(0, ByteBuffer.wrap(new byte[0])));
        return events;
      } else if (this.exLocation == ExceptionLocation.II_HandleInputInitializerEvents) {
        // send InputInitliazer to InputInitializer of v2
        List<Event> events = new ArrayList<Event>();
        events.add(InputInitializerEvent.create("v2", "input2", ByteBuffer.wrap(new byte[0])));
        return events;
      } else {
        return null;
      }
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

      OutputWithException output = (OutputWithException) outputs.get("v2");
      output.start();
      output.getWriter();

      Thread.sleep(3*1000);
      if (this.exLocation == ExceptionLocation.PROCESSOR_RUN_ERROR) {
        throw new Error(this.exLocation.name());
      } else if (this.exLocation == ExceptionLocation.PROCESSOR_RUN_EXCEPTION) {
        throw new Exception(this.exLocation.name());
      } else if (this.exLocation == ExceptionLocation.PROCESSOR_COUNTER_EXCEEDED) {
        // simulate the counter limitation exceeded
        for (int i=0;i< TezConfiguration.TEZ_COUNTERS_MAX_DEFAULT+1; ++i) {
          getContext().getCounters().findCounter("mycounter", "counter_"+i).increment(1);
        }
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

  // VertexManager of vertex1
  public static class RootInputVertexManagerWithException extends RootInputVertexManager {

    private ExceptionLocation exLocation;

    public RootInputVertexManagerWithException(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
      super.initialize();
      this.exLocation =
          ExceptionLocation.valueOf(new String(getContext().getUserPayload()
              .deepCopyAsArray()));
      if (this.exLocation == ExceptionLocation.VM_INITIALIZE) {
        throw new RuntimeException(this.exLocation.name());
      }
    }

    @Override
    public void onRootVertexInitialized(String inputName,
        InputDescriptor inputDescriptor, List<Event> events) {
      if (this.exLocation == ExceptionLocation.VM_ON_ROOTVERTEX_INITIALIZE) {
        throw new RuntimeException(this.exLocation.name());
      }
      super.onRootVertexInitialized(inputName, inputDescriptor, events);
    }

    @Override
    public void onVertexStarted(Map<String, List<Integer>> completions) {
      if (this.exLocation == ExceptionLocation.VM_ON_VERTEX_STARTED) {
        throw new RuntimeException(this.exLocation.name());
      }
      super.onVertexStarted(completions);
    }

    public static VertexManagerPluginDescriptor getVMDesc(UserPayload payload) {
      return VertexManagerPluginDescriptor.create(RootInputVertexManagerWithException.class.getName())
              .setUserPayload(payload);
    }
  }

  // VertexManager of vertex2
  public static class InputReadyVertexManagerWithException extends InputReadyVertexManager {

    private ExceptionLocation exLocation;
    private static final String Test_ExceptionLocation = "Test.ExceptionLocation";

    public InputReadyVertexManagerWithException(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
      try {
        super.initialize();
      } catch (TezUncheckedException e) {
        // workaround for testing
        if (!e.getMessage().equals("Atleast 1 bipartite source should exist")) {
          throw e;
        }
      }
      Configuration conf;
      try {
        conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
        this.exLocation = ExceptionLocation.valueOf(conf.get(Test_ExceptionLocation));
      } catch (IOException e) {
        throw new TezUncheckedException(e);
      }
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer attemptId) {
      if (this.exLocation == ExceptionLocation.VM_ON_SOURCETASK_COMPLETED) {
        throw new RuntimeException(this.exLocation.name());
      }
      super.onSourceTaskCompleted(srcVertexName, attemptId);
    }

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
      if (this.exLocation == ExceptionLocation.VM_ON_VERTEXMANAGEREVENT_RECEIVED) {
        throw new RuntimeException(this.exLocation.name());
      }
      super.onVertexManagerEventReceived(vmEvent);
    }

    public static VertexManagerPluginDescriptor getVMDesc(ExceptionLocation exLocation) throws IOException {
      Configuration conf = new Configuration();
      conf.set(Test_ExceptionLocation, exLocation.name());
      UserPayload payload = TezUtils.createUserPayloadFromConf(conf);
      return VertexManagerPluginDescriptor.create(InputReadyVertexManagerWithException.class.getName())
              .setUserPayload(payload);
    }
  }

  // EdgeManager for edge linking vertex1 and vertex2
  public static class CustomEdgeManager extends OneToOneEdgeManager {

    private ExceptionLocation exLocation;

    public CustomEdgeManager(EdgeManagerPluginContext context) {
      super(context);
      this.exLocation =
          ExceptionLocation.valueOf(new String(context.getUserPayload()
              .deepCopyAsArray()));
    }

    @Override
    public void initialize() {
      if (exLocation == ExceptionLocation.EM_Initialize) {
        throw new RuntimeException(exLocation.name());
      }
      try {
        super.initialize();
      } catch (TezUncheckedException e) {
        // workaround for testing
        if (!e.getMessage().equals("Atleast 1 bipartite source should exist")) {
          throw e;
        }
      }
    }

    @Override
    public int getNumDestinationConsumerTasks(int sourceTaskIndex) {
      if (exLocation == ExceptionLocation.EM_GetNumDestinationConsumerTasks) {
        throw new RuntimeException(exLocation.name());
      }
      return super.getNumDestinationConsumerTasks(sourceTaskIndex);
    }

    @Override
    public int getNumSourceTaskPhysicalOutputs(int sourceTaskIndex) {
      if (exLocation == ExceptionLocation.EM_GetNumSourceTaskPhysicalOutputs) {
        throw new RuntimeException(exLocation.name());
      }
      LOG.info("ExLocation:" + exLocation);
      return super.getNumSourceTaskPhysicalOutputs(sourceTaskIndex);
    }

    @Override
    public int getNumDestinationTaskPhysicalInputs(int destinationTaskIndex) {
      if (exLocation == ExceptionLocation.EM_GetNumDestinationTaskPhysicalInputs) {
        throw new RuntimeException(exLocation.name());
      }
      return super.getNumDestinationTaskPhysicalInputs(destinationTaskIndex);
    }

    @Override
    public void routeDataMovementEventToDestination(DataMovementEvent event,
        int sourceTaskIndex, int sourceOutputIndex,
        Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
      if (exLocation == ExceptionLocation.EM_RouteDataMovementEventToDestination) {
        throw new RuntimeException(exLocation.name());
      }
      super.routeDataMovementEventToDestination(event, sourceTaskIndex,
          sourceOutputIndex, destinationTaskAndInputIndices);
    }

    @Override
    public int routeInputErrorEventToSource(InputReadErrorEvent event,
        int destinationTaskIndex, int destinationFailedInputIndex) {
      if (exLocation == ExceptionLocation.EM_RouteInputErrorEventToSource) {
        throw new RuntimeException(exLocation.name());
      }
      return super.routeInputErrorEventToSource(event, destinationTaskIndex,
          destinationFailedInputIndex);
    }

    @Override
    public void routeInputSourceTaskFailedEventToDestination(
        int sourceTaskIndex,
        Map<Integer, List<Integer>> destinationTaskAndInputIndices) {
      super.routeInputSourceTaskFailedEventToDestination(sourceTaskIndex,
          destinationTaskAndInputIndices);
    }
  }
}
