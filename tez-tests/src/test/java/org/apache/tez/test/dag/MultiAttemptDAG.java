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

package org.apache.tez.test.dag;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;

import java.nio.ByteBuffer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.Edge;
import org.apache.tez.dag.api.EdgeProperty;
import org.apache.tez.dag.api.EdgeProperty.DataMovementType;
import org.apache.tez.dag.api.EdgeProperty.DataSourceType;
import org.apache.tez.dag.api.EdgeProperty.SchedulingType;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.VertexManagerPlugin;
import org.apache.tez.dag.api.VertexManagerPluginContext;
import org.apache.tez.dag.api.VertexManagerPluginDescriptor;
import org.apache.tez.dag.api.VertexManagerPluginContext.TaskWithLocationHint;
import org.apache.tez.dag.api.client.VertexStatus.State;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.MemoryUpdateCallback;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.InputInitializer;
import org.apache.tez.runtime.api.InputInitializerContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.api.events.InputDataInformationEvent;
import org.apache.tez.runtime.api.events.InputInitializerEvent;
import org.apache.tez.runtime.api.events.VertexManagerEvent;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.test.TestInput;
import org.apache.tez.test.TestOutput;
import org.apache.tez.test.TestProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiAttemptDAG {

  private static final Log LOG =
      LogFactory.getLog(MultiAttemptDAG.class);

  static Resource defaultResource = Resource.newInstance(100, 0);
  public static String MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS =
      "tez.multi-attempt-dag.vertex.num-tasks";
  public static int MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS_DEFAULT = 2;

  public static String MULTI_ATTEMPT_DAG_USE_FAILING_COMMITTER =
      "tez.multi-attempt-dag.use-failing-committer";
  public static boolean MULTI_ATTEMPT_DAG_USE_FAILING_COMMITTER_DEFAULT = false;

  public static class FailOnAttemptVertexManagerPlugin extends VertexManagerPlugin {
    private int numSourceTasks = 0;
    private AtomicInteger numCompletions = new AtomicInteger();
    private boolean tasksScheduled = false;

    public FailOnAttemptVertexManagerPlugin(VertexManagerPluginContext context) {
      super(context);
    }

    @Override
    public void initialize() {
      for (String input :
          getContext().getInputVertexEdgeProperties().keySet()) {
        LOG.info("Adding sourceTasks for Vertex " + input);
        numSourceTasks += getContext().getVertexNumTasks(input);
        LOG.info("Current numSourceTasks=" + numSourceTasks);
      }
    }

    @Override
    public void onVertexStarted(Map<String, List<Integer>> completions) {
      if (completions != null) {
        for (Entry<String, List<Integer>> entry : completions.entrySet()) {
          LOG.info("Received completion events on vertexStarted"
              + ", vertex=" + entry.getKey()
              + ", completions=" + entry.getValue().size());
          numCompletions.addAndGet(entry.getValue().size());
        }
      }
      maybeScheduleTasks();
    }

    private synchronized void maybeScheduleTasks() {
      if (numCompletions.get() >= numSourceTasks
          && !tasksScheduled) {
        tasksScheduled = true;
        String payload = new String(getContext().getUserPayload().deepCopyAsArray());
        int successAttemptId = Integer.valueOf(payload);
        LOG.info("Checking whether to crash AM or schedule tasks"
            + ", successfulAttemptID=" + successAttemptId
            + ", currentAttempt=" + getContext().getDAGAttemptNumber());
        if (successAttemptId > getContext().getDAGAttemptNumber()) {
          Runtime.getRuntime().halt(-1);
        } else if (successAttemptId == getContext().getDAGAttemptNumber()) {
          LOG.info("Scheduling tasks for vertex=" + getContext().getVertexName());
          int numTasks = getContext().getVertexNumTasks(getContext().getVertexName());
          List<TaskWithLocationHint> scheduledTasks = Lists.newArrayListWithCapacity(numTasks);
          for (int i=0; i<numTasks; ++i) {
            scheduledTasks.add(new TaskWithLocationHint(new Integer(i), null));
          }
          getContext().scheduleVertexTasks(scheduledTasks);
        }
      }
    }

    @Override
    public void onSourceTaskCompleted(String srcVertexName, Integer taskId) {
      LOG.info("Received completion events for source task"
          + ", vertex=" + srcVertexName
          + ", taskIdx=" + taskId);
      numCompletions.incrementAndGet();
      maybeScheduleTasks();
    }

    @Override
    public void onVertexManagerEventReceived(VertexManagerEvent vmEvent) {
      // Nothing to do
    }

    @Override
    public void onRootVertexInitialized(String inputName,
        InputDescriptor inputDescriptor, List<Event> events) {
      List<InputDataInformationEvent> inputInfoEvents = new ArrayList<InputDataInformationEvent>();
      for (Event event: events) {
        if (event instanceof InputDataInformationEvent) {
          inputInfoEvents.add((InputDataInformationEvent)event);
        }
      }
      getContext().addRootInputEvents(inputName, inputInfoEvents);
    }
  }

  public static class FailingOutputCommitter extends OutputCommitter {

    boolean failOnCommit = false;

    public FailingOutputCommitter(OutputCommitterContext committerContext) {
      super(committerContext);
    }

    @Override
    public void initialize() throws Exception {
      FailingOutputCommitterConfig config = new
          FailingOutputCommitterConfig();
      config.fromUserPayload(
          getContext().getOutputUserPayload().deepCopyAsArray());
      failOnCommit = config.failOnCommit;
    }

    @Override
    public void setupOutput() throws Exception {

    }

    @Override
    public void commitOutput() throws Exception {
      if (failOnCommit) {
        LOG.info("Committer causing AM to shutdown");
        Runtime.getRuntime().halt(-1);
      }
    }

    @Override
    public void abortOutput(State finalState) throws Exception {

    }

    public static class FailingOutputCommitterConfig {
      boolean failOnCommit;

      public FailingOutputCommitterConfig() {
        this(false);
      }

      public FailingOutputCommitterConfig(boolean failOnCommit) {
        this.failOnCommit = failOnCommit;
      }

      public byte[] toUserPayload() {
        return Ints.toByteArray((failOnCommit ? 1 : 0));
      }

      public void fromUserPayload(byte[] userPayload) {
        int failInt = Ints.fromByteArray(userPayload);
        if (failInt == 0) {
          failOnCommit = false;
        } else {
          failOnCommit = true;
        }
      }
    }
  }

  public static class TestRootInputInitializer extends InputInitializer {

    public TestRootInputInitializer(InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {
      List<Event> events = new ArrayList<Event>();
      events.add(InputDataInformationEvent.createWithSerializedPayload(0, ByteBuffer.allocate(0)));
      return events;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events)
        throws Exception {
      throw new UnsupportedOperationException("Not supported");
    }
  }

  public static class FailingInputInitializer extends InputInitializer {

    public FailingInputInitializer(InputInitializerContext initializerContext) {
      super(initializerContext);
    }

    @Override
    public List<Event> initialize() throws Exception {
      try {
        Thread.sleep(2000l);
      } catch (InterruptedException e) {
        // Ignore
      }
      if (getContext().getDAGAttemptNumber() == 1) {
        LOG.info("Shutting down the AM in 1st attempt");
        Runtime.getRuntime().halt(-1);
      }
      return null;
    }

    @Override
    public void handleInputInitializerEvent(List<InputInitializerEvent> events) throws
        Exception {
      throw new UnsupportedOperationException("Not supported");
    }
  }

  public static class NoOpInput extends AbstractLogicalInput {

    public NoOpInput(InputContext inputContext, int numPhysicalInputs) {
      super(inputContext, numPhysicalInputs);
    }

    @Override
    public List<Event> initialize() throws Exception {
      getContext().requestInitialMemory(1l, new MemoryUpdateCallback() {
        @Override
        public void memoryAssigned(long assignedSize) {}
      });
      return null;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public Reader getReader() throws Exception {
      return null;
    }

    @Override
    public void handleEvents(List<Event> inputEvents) throws Exception {

    }

    @Override
    public List<Event> close() throws Exception {
      return null;
    }
  }

  public static class NoOpOutput extends AbstractLogicalOutput {

    public NoOpOutput(OutputContext outputContext,
                      int numPhysicalOutputs) {
      super(outputContext, numPhysicalOutputs);
    }

    @Override
    public List<Event> initialize() throws Exception {
      getContext().requestInitialMemory(1l, new MemoryUpdateCallback() {
        @Override
        public void memoryAssigned(long assignedSize) {
        }
      });
      return null;
    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public Writer getWriter() throws Exception {
      return null;
    }

    @Override
    public void handleEvents(List<Event> outputEvents) {

    }

    @Override
    public List<Event> close() throws Exception {
      return null;
    }
  }




  public static DAG createDAG(String name,
      Configuration conf) throws Exception {
    UserPayload payload = UserPayload.create(null);
    int taskCount = MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS_DEFAULT;
    if (conf != null) {
      taskCount = conf.getInt(MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS, MULTI_ATTEMPT_DAG_VERTEX_NUM_TASKS_DEFAULT);
      payload = TezUtils.createUserPayloadFromConf(conf);
    }
    DAG dag = DAG.create(name);
    Vertex v1 = Vertex.create("v1", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v2 = Vertex.create("v2", TestProcessor.getProcDesc(payload), taskCount, defaultResource);
    Vertex v3 = Vertex.create("v3", TestProcessor.getProcDesc(payload), taskCount, defaultResource);

    // Make each vertex manager fail on appropriate attempt
    v1.setVertexManagerPlugin(VertexManagerPluginDescriptor.create(
        FailOnAttemptVertexManagerPlugin.class.getName())
        .setUserPayload(UserPayload.create(ByteBuffer.wrap(new String("1").getBytes()))));
    v2.setVertexManagerPlugin(VertexManagerPluginDescriptor.create(
        FailOnAttemptVertexManagerPlugin.class.getName())
        .setUserPayload(UserPayload.create(ByteBuffer.wrap(new String("2").getBytes()))));
    v3.setVertexManagerPlugin(VertexManagerPluginDescriptor.create(
        FailOnAttemptVertexManagerPlugin.class.getName())
        .setUserPayload(UserPayload.create(ByteBuffer.wrap(new String("3").getBytes()))));
    dag.addVertex(v1).addVertex(v2).addVertex(v3);
    dag.addEdge(Edge.create(v1, v2,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL,
            TestOutput.getOutputDesc(payload),
            TestInput.getInputDesc(payload))));
    dag.addEdge(Edge.create(v2, v3,
        EdgeProperty.create(DataMovementType.SCATTER_GATHER,
            DataSourceType.PERSISTED,
            SchedulingType.SEQUENTIAL,
            TestOutput.getOutputDesc(payload),
            TestInput.getInputDesc(payload))));
    return dag;
  }

  public static DAG createDAG(Configuration conf) throws Exception {
    return createDAG("SimpleVTestDAG", conf);
  }

}
