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

package org.apache.tez.runtime;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.dag.records.TezDAGID;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.dag.records.TezTaskID;
import org.apache.tez.dag.records.TezVertexID;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.AbstractLogicalOutput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Writer;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.common.resources.ScalingAllocator;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class TestLogicalIOProcessorRuntimeTask {

  @Test(timeout = 5000)
  public void testAutoStart() throws Exception {
    TezDAGID dagId = createTezDagId();
    TezVertexID vertexId = createTezVertexId(dagId);
    Map<String, ByteBuffer> serviceConsumerMetadata = new HashMap<String, ByteBuffer>();
    Multimap<String, String> startedInputsMap = HashMultimap.create();
    TezUmbilical umbilical = mock(TezUmbilical.class);
    TezConfiguration tezConf = new TezConfiguration();
    tezConf.set(TezConfiguration.TEZ_TASK_SCALE_MEMORY_ALLOCATOR_CLASS,
        ScalingAllocator.class.getName());

    TezTaskAttemptID taId1 = createTaskAttemptID(vertexId, 1);
    TaskSpec task1 = createTaskSpec(taId1, "dag1", "vertex1", 30);

    TezTaskAttemptID taId2 = createTaskAttemptID(vertexId, 2);
    TaskSpec task2 = createTaskSpec(taId2, "dag2", "vertex1", 10);

    LogicalIOProcessorRuntimeTask lio1 = new LogicalIOProcessorRuntimeTask(task1, 0, tezConf, null,
        umbilical, serviceConsumerMetadata, new HashMap<String, String>(), startedInputsMap, null,
        "", new ExecutionContextImpl("localhost"), Runtime.getRuntime().maxMemory());

    try {
      lio1.initialize();
      lio1.run();
      lio1.close();

      // Input should've been started, Output should not have been started
      assertEquals(1, TestProcessor.runCount);
      assertEquals(1, TestInput.startCount);
      assertEquals(0, TestOutput.startCount);
      assertEquals(30, TestInput.vertexParallelism);
      assertEquals(0, TestOutput.vertexParallelism);
      assertEquals(30, lio1.getProcessorContext().getVertexParallelism());
      assertEquals(30, lio1.getInputContexts().iterator().next().getVertexParallelism());
      assertEquals(30, lio1.getOutputContexts().iterator().next().getVertexParallelism());
    } catch(Exception e) {
      fail();
    } finally {
      cleanupAndTest(lio1);
    }



    // local mode 
    tezConf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    LogicalIOProcessorRuntimeTask lio2 = new LogicalIOProcessorRuntimeTask(task2, 0, tezConf, null,
        umbilical, serviceConsumerMetadata, new HashMap<String, String>(), startedInputsMap, null,
        "", new ExecutionContextImpl("localhost"), Runtime.getRuntime().maxMemory());
    try {
      lio2.initialize();
      lio2.run();
      lio2.close();

      // Input should not have been started again, Output should not have been started
      assertEquals(2, TestProcessor.runCount);
      assertEquals(1, TestInput.startCount);
      assertEquals(0, TestOutput.startCount);
      assertEquals(30, TestInput.vertexParallelism);
      assertEquals(0, TestOutput.vertexParallelism);
      //Check if parallelism is available in processor/ i/p / o/p contexts
      assertEquals(10, lio2.getProcessorContext().getVertexParallelism());
      assertEquals(10, lio2.getInputContexts().iterator().next().getVertexParallelism());
      assertEquals(10, lio2.getOutputContexts().iterator().next().getVertexParallelism());
    } catch(Exception e) {
      fail();
    } finally {
      cleanupAndTest(lio2);
    }

  }

  private void cleanupAndTest(LogicalIOProcessorRuntimeTask lio) throws InterruptedException {

    ProcessorContext procContext = lio.getProcessorContext();
    List<InputContext> inputContexts = new LinkedList<InputContext>();
    inputContexts.addAll(lio.getInputContexts());
    List<OutputContext> outputContexts = new LinkedList<OutputContext>();
    outputContexts.addAll(lio.getOutputContexts());

    lio.cleanup();

    assertTrue(procContext.getUserPayload() == null);
    assertTrue(procContext.getObjectRegistry() == null);

    for (InputContext inputContext : inputContexts) {
      assertTrue(inputContext.getUserPayload() == null);
      assertTrue(inputContext.getObjectRegistry() == null);
    }

    for (OutputContext outputContext : outputContexts) {
      assertTrue(outputContext.getUserPayload() == null);
      assertTrue(outputContext.getObjectRegistry() == null);
    }
    boolean localMode = lio.tezConf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE,
        TezConfiguration.TEZ_LOCAL_MODE_DEFAULT);
    if (localMode) {
      assertEquals(1, lio.inputSpecs.size());
      assertEquals(1, lio.outputSpecs.size());
      assertTrue(lio.groupInputSpecs == null || lio.groupInputSpecs.size() == 0);
    } else {
      assertEquals(0, lio.inputSpecs.size());
      assertEquals(0, lio.outputSpecs.size());
      assertTrue(lio.groupInputSpecs == null || lio.groupInputSpecs.size() == 0);
    }
    
    assertEquals(0, lio.inputsMap.size());
    assertEquals(0, lio.inputContextMap.size());
    assertEquals(0, lio.outputsMap.size());
    assertEquals(0, lio.outputContextMap.size());
    assertNull(lio.groupInputsMap);
    assertNull(lio.processor);
    assertNull(lio.processorContext);
    assertEquals(0, lio.runInputMap.size());
    assertEquals(0, lio.runOutputMap.size());
    assertEquals(0, lio.eventsToBeProcessed.size());
    assertNull(lio.eventRouterThread);
  }

  private TaskSpec createTaskSpec(TezTaskAttemptID taskAttemptID,
      String dagName, String vertexName, int parallelism) {
    ProcessorDescriptor processorDesc = createProcessorDescriptor();
    TaskSpec taskSpec = new TaskSpec(taskAttemptID,
        dagName, vertexName, parallelism, processorDesc,
        createInputSpecList(), createOutputSpecList(), null);
    return taskSpec;
  }

  private List<InputSpec> createInputSpecList() {
    InputDescriptor inputDesc = InputDescriptor.create(TestInput.class.getName());
    InputSpec inputSpec = new InputSpec("inedge", inputDesc, 1);
    return Lists.newArrayList(inputSpec);
  }

  private List<OutputSpec> createOutputSpecList() {
    OutputDescriptor outputtDesc = OutputDescriptor.create(TestOutput.class.getName());
    OutputSpec outputSpec = new OutputSpec("outedge", outputtDesc, 1);
    return Lists.newArrayList(outputSpec);
  }

  private ProcessorDescriptor createProcessorDescriptor() {
    ProcessorDescriptor desc = ProcessorDescriptor.create(TestProcessor.class.getName());
    return desc;
  }

  private TezTaskAttemptID createTaskAttemptID(TezVertexID vertexId, int taskIndex) {
    TezTaskID taskId = TezTaskID.getInstance(vertexId, taskIndex);
    TezTaskAttemptID taskAttemptId = TezTaskAttemptID.getInstance(taskId, taskIndex);
    return taskAttemptId;
  }

  private TezVertexID createTezVertexId(TezDAGID dagId) {
    return TezVertexID.getInstance(dagId, 1);
  }

  private TezDAGID createTezDagId() {
    return TezDAGID.getInstance("2000", 100, 1);
  }

  public static class TestProcessor extends AbstractLogicalIOProcessor {

    public static volatile int runCount = 0;

    public TestProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {
    }

    @Override
    public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
        throws Exception {
      runCount++;
    }

	@Override
	public void handleEvents(List<Event> processorEvents) {
		
	}

	@Override
	public void close() throws Exception {
		
	}

  }

  public static class TestInput extends AbstractLogicalInput {

    public static volatile int startCount = 0;
    public static volatile int vertexParallelism;

    public TestInput(InputContext inputContext, int numPhysicalInputs) {
      super(inputContext, numPhysicalInputs);
    }

    @Override
    public List<Event> initialize() throws Exception {
      getContext().requestInitialMemory(0, null);
      getContext().inputIsReady();
      return null;
    }

    @Override
    public void start() throws Exception {
      startCount++;
      this.vertexParallelism = getContext().getVertexParallelism();
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

  public static class TestOutput extends AbstractLogicalOutput {

    public static volatile int startCount = 0;
    public static volatile int vertexParallelism;

    public TestOutput(OutputContext outputContext, int numPhysicalOutputs) {
      super(outputContext, numPhysicalOutputs);
    }


    @Override
    public List<Event> initialize() throws Exception {
      getContext().requestInitialMemory(0, null);
      return null;
    }

    @Override
    public void start() throws Exception {
      System.err.println("Out started");
      startCount++;
      this.vertexParallelism = getContext().getVertexParallelism();
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
}
