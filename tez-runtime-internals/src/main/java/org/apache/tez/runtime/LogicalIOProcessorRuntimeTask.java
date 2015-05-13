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

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.tez.runtime.api.TaskContext;
import org.apache.tez.runtime.api.impl.TezProcessorContextImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.CallableWithNdc;
import org.apache.tez.common.ReflectionUtils;
import org.apache.tez.common.RunnableWithNdc;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.InputFrameworkInterface;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.LogicalOutputFrameworkInterface;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.api.Output;
import org.apache.tez.runtime.api.OutputFrameworkInterface;
import org.apache.tez.runtime.api.Processor;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.MergedInputContext;
import org.apache.tez.runtime.api.OutputContext;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezInputContextImpl;
import org.apache.tez.runtime.api.impl.TezMergedInputContextImpl;
import org.apache.tez.runtime.api.impl.TezOutputContextImpl;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.common.resources.MemoryDistributor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

@Private
public class LogicalIOProcessorRuntimeTask extends RuntimeTask {

  private static final Logger LOG = LoggerFactory
      .getLogger(LogicalIOProcessorRuntimeTask.class);

  @VisibleForTesting // All fields non private for testing.
  private final String[] localDirs;
  /** Responsible for maintaining order of Inputs */
  final List<InputSpec> inputSpecs;
  final ConcurrentMap<String, LogicalInput> inputsMap;
  final ConcurrentMap<String, InputContext> inputContextMap;
  /** Responsible for maintaining order of Outputs */
  final List<OutputSpec> outputSpecs;
  final ConcurrentMap<String, LogicalOutput> outputsMap;
  final ConcurrentMap<String, OutputContext> outputContextMap;

  final List<GroupInputSpec> groupInputSpecs;
  ConcurrentHashMap<String, MergedLogicalInput> groupInputsMap;

  final ProcessorDescriptor processorDescriptor;
  AbstractLogicalIOProcessor processor;
  ProcessorContext processorContext;

  private final MemoryDistributor initialMemoryDistributor;

  /** Maps which will be provided to the processor run method */
  final LinkedHashMap<String, LogicalInput> runInputMap;
  final LinkedHashMap<String, LogicalOutput> runOutputMap;
  
  private final Map<String, ByteBuffer> serviceConsumerMetadata;
  private final Map<String, String> envMap;

  final ExecutorService initializerExecutor;
  private final CompletionService<Void> initializerCompletionService;

  private final Multimap<String, String> startedInputsMap;

  LinkedBlockingQueue<TezEvent> eventsToBeProcessed;
  Thread eventRouterThread = null;

  private final int appAttemptNumber;

  private volatile InputReadyTracker inputReadyTracker;
  
  private volatile ObjectRegistry objectRegistry;
  private final ExecutionContext ExecutionContext;
  private final long memAvailable;

  public LogicalIOProcessorRuntimeTask(TaskSpec taskSpec, int appAttemptNumber,
      Configuration tezConf, String[] localDirs, TezUmbilical tezUmbilical,
      Map<String, ByteBuffer> serviceConsumerMetadata, Map<String, String> envMap,
      Multimap<String, String> startedInputsMap, ObjectRegistry objectRegistry,
      String pid, ExecutionContext ExecutionContext, long memAvailable) throws IOException {
    // Note: If adding any fields here, make sure they're cleaned up in the cleanupContext method.
    // TODO Remove jobToken from here post TEZ-421
    super(taskSpec, tezConf, tezUmbilical, pid);
    LOG.info("Initializing LogicalIOProcessorRuntimeTask with TaskSpec: "
        + taskSpec);
    int numInputs = taskSpec.getInputs().size();
    int numOutputs = taskSpec.getOutputs().size();
    this.localDirs = localDirs;
    this.inputSpecs = taskSpec.getInputs();
    this.inputsMap = new ConcurrentHashMap<String, LogicalInput>(numInputs);
    this.inputContextMap = new ConcurrentHashMap<String, InputContext>(numInputs);
    this.outputSpecs = taskSpec.getOutputs();
    this.outputsMap = new ConcurrentHashMap<String, LogicalOutput>(numOutputs);
    this.outputContextMap = new ConcurrentHashMap<String, OutputContext>(numOutputs);

    this.runInputMap = new LinkedHashMap<String, LogicalInput>();
    this.runOutputMap = new LinkedHashMap<String, LogicalOutput>();

    this.processorDescriptor = taskSpec.getProcessorDescriptor();
    this.serviceConsumerMetadata = serviceConsumerMetadata;
    this.envMap = envMap;
    this.eventsToBeProcessed = new LinkedBlockingQueue<TezEvent>();
    this.state.set(State.NEW);
    this.appAttemptNumber = appAttemptNumber;
    int numInitializers = numInputs + numOutputs; // Processor is initialized in the main thread.
    numInitializers = (numInitializers == 0 ? 1 : numInitializers);
    this.initializerExecutor = Executors.newFixedThreadPool(
        numInitializers,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Initializer %d").build());
    this.initializerCompletionService = new ExecutorCompletionService<Void>(
        this.initializerExecutor);
    this.groupInputSpecs = taskSpec.getGroupInputs();
    initialMemoryDistributor = new MemoryDistributor(numInputs, numOutputs, tezConf);
    this.startedInputsMap = startedInputsMap;
    this.inputReadyTracker = new InputReadyTracker();
    this.objectRegistry = objectRegistry;
    this.ExecutionContext = ExecutionContext;
    this.memAvailable = memAvailable;
  }

  /**
   * @throws Exception
   */
  public void initialize() throws Exception {
    LOG.info("Initializing LogicalProcessorIORuntimeTask");
    Preconditions.checkState(this.state.get() == State.NEW, "Already initialized");
    this.state.set(State.INITED);

    LOG.info("Creating processor" + ", processorClassName=" + processorDescriptor.getClassName());
    this.processorContext = createProcessorContext();
    this.processor = createProcessor(processorDescriptor.getClassName(), processorContext);

    int numTasks = 0;

    int inputIndex = 0;
    for (InputSpec inputSpec : taskSpec.getInputs()) {
      this.initializerCompletionService.submit(
          new InitializeInputCallable(inputSpec, inputIndex++));
      numTasks++;
    }

    int outputIndex = 0;
    for (OutputSpec outputSpec : taskSpec.getOutputs()) {
      this.initializerCompletionService.submit(
          new InitializeOutputCallable(outputSpec, outputIndex++));
      numTasks++;
    }

    // Initialize processor in the current thread.
    initializeLogicalIOProcessor();

    int completedTasks = 0;
    while (completedTasks < numTasks) {
      LOG.info("Waiting for " + (numTasks-completedTasks) + " initializers to finish");
      Future<Void> future = initializerCompletionService.take();
      try {
        future.get();
        completedTasks++;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        } else {
          throw new Exception(e);
        }
      }
    }
    LOG.info("All initializers finished");
    // group inputs depend on inputs beings initialized. So must be done after.
    initializeGroupInputs();
    // Register the groups so that appropriate calls can be made.
    this.inputReadyTracker
        .setGroupedInputs(groupInputsMap == null ? null : groupInputsMap.values());
    // Grouped input start will be controlled by the start of the GroupedInput

    // Construct the set of groupedInputs up front so that start is not invoked on them.
    Set<String> groupInputs = Sets.newHashSet();
    // Construct Inputs/Outputs map argument for processor.run()
    // first add the group inputs
    if (groupInputSpecs !=null && !groupInputSpecs.isEmpty()) {
      for (GroupInputSpec groupInputSpec : groupInputSpecs) {
        runInputMap.put(groupInputSpec.getGroupName(),
                                 groupInputsMap.get(groupInputSpec.getGroupName()));
        groupInputs.addAll(groupInputSpec.getGroupVertices());
      }
    }

    initialMemoryDistributor.makeInitialAllocations();

    LOG.info("Starting Inputs/Outputs");
    int numAutoStarts = 0;
    for (InputSpec inputSpec : inputSpecs) {
      if (groupInputs.contains(inputSpec.getSourceVertexName())) {
        LOG.info("Ignoring " + inputSpec.getSourceVertexName()
            + " for start, since it will be controlled via it's Group");
        continue;
      }
      if (!inputAlreadyStarted(taskSpec.getVertexName(), inputSpec.getSourceVertexName())) {
        startedInputsMap.put(taskSpec.getVertexName(), inputSpec.getSourceVertexName());
        numAutoStarts++;
        this.initializerCompletionService.submit(new StartInputCallable(inputsMap.get(inputSpec
            .getSourceVertexName()), inputSpec.getSourceVertexName()));
        LOG.info("Input: " + inputSpec.getSourceVertexName()
            + " being auto started by the framework. Subsequent instances will not be auto-started");
      }
    }

    if (groupInputSpecs != null) {
      for (GroupInputSpec group : groupInputSpecs) {
        if (!inputAlreadyStarted(taskSpec.getVertexName(), group.getGroupName())) {
          numAutoStarts++;
          this.initializerCompletionService.submit(new StartInputCallable(groupInputsMap.get(group
              .getGroupName()), group.getGroupName()));
          LOG.info("InputGroup: " + group.getGroupName()
              + " being auto started by the framework. Subsequent instance will not be auto-started");
        }
      }
    }

    // Shutdown after all tasks complete.
    this.initializerExecutor.shutdown();

    completedTasks = 0;
    LOG.info("Num IOs determined for AutoStart: " + numAutoStarts);
    while (completedTasks < numAutoStarts) {
      LOG.info("Waiting for " + (numAutoStarts - completedTasks) + " IOs to start");
      Future<Void> future = initializerCompletionService.take();
      try {
        future.get();
        completedTasks++;
      } catch (ExecutionException e) {
        if (e.getCause() instanceof Exception) {
          throw (Exception) e.getCause();
        } else {
          throw new Exception(e);
        }
      }
    }
    LOG.info("AutoStartComplete");



    // then add the non-grouped inputs
    for (InputSpec inputSpec : inputSpecs) {
      if (!groupInputs.contains(inputSpec.getSourceVertexName())) {
        LogicalInput input = inputsMap.get(inputSpec.getSourceVertexName());
        runInputMap.put(inputSpec.getSourceVertexName(), input);
      }
    }

    for (OutputSpec outputSpec : outputSpecs) {
      LogicalOutput output = outputsMap.get(outputSpec.getDestinationVertexName());
      String outputName = outputSpec.getDestinationVertexName();
      runOutputMap.put(outputName, output);
    }

    // TODO Maybe close initialized inputs / outputs in case of failure to
    // initialize.

    startRouterThread();
  }

  public void run() throws Exception {
    Preconditions.checkState(this.state.get() == State.INITED,
        "Can only run while in INITED state. Current: " + this.state);
    this.state.set(State.RUNNING);
    processor.run(runInputMap, runOutputMap);
  }

  public void close() throws Exception {
    try {
      Preconditions.checkState(this.state.get() == State.RUNNING,
          "Can only run while in RUNNING state. Current: " + this.state);
      this.state.set(State.CLOSED);

      // Close the Processor.
      processor.close();

      // Close the Inputs.
      for (InputSpec inputSpec : inputSpecs) {
        String srcVertexName = inputSpec.getSourceVertexName();
        List<Event> closeInputEvents = ((InputFrameworkInterface)inputsMap.get(srcVertexName)).close();
        sendTaskGeneratedEvents(closeInputEvents,
            EventProducerConsumerType.INPUT, taskSpec.getVertexName(),
            srcVertexName, taskSpec.getTaskAttemptID());
      }

      // Close the Outputs.
      for (OutputSpec outputSpec : outputSpecs) {
        String destVertexName = outputSpec.getDestinationVertexName();
        List<Event> closeOutputEvents = ((LogicalOutputFrameworkInterface)outputsMap.get(destVertexName)).close();
        sendTaskGeneratedEvents(closeOutputEvents,
            EventProducerConsumerType.OUTPUT, taskSpec.getVertexName(),
            destVertexName, taskSpec.getTaskAttemptID());
      }
    } finally {
      setTaskDone();
      if (eventRouterThread != null) {
        eventRouterThread.interrupt();
        LOG.info("Joining on EventRouter");
        try {
          eventRouterThread.join();
        } catch (InterruptedException e) {
          LOG.info("Ignoring interrupt while waiting for the router thread to die");
          Thread.currentThread().interrupt();
        }
        eventRouterThread = null;
      }
      String timeStamp = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(Calendar.getInstance().getTime());
      System.err.println(timeStamp + " Completed running task attempt: " + taskSpec.getTaskAttemptID().toString());
      System.out.println(timeStamp + " Completed running task attempt: " + taskSpec.getTaskAttemptID().toString());
    }
  }

  private class InitializeInputCallable extends CallableWithNdc<Void> {

    private final InputSpec inputSpec;
    private final int inputIndex;

    public InitializeInputCallable(InputSpec inputSpec, int inputIndex) {
      this.inputSpec = inputSpec;
      this.inputIndex = inputIndex;
    }

    @Override
    protected Void callInternal() throws Exception {
      LOG.info("Initializing Input using InputSpec: " + inputSpec);
      String edgeName = inputSpec.getSourceVertexName();
      InputContext inputContext = createInputContext(inputsMap, inputSpec, inputIndex);
      LogicalInput input = createInput(inputSpec, inputContext);

      inputsMap.put(edgeName, input);
      inputContextMap.put(edgeName, inputContext);

      LOG.info("Initializing Input with src edge: " + edgeName);
      List<Event> events = ((InputFrameworkInterface)input).initialize();
      sendTaskGeneratedEvents(events, EventProducerConsumerType.INPUT,
          inputContext.getTaskVertexName(), inputContext.getSourceVertexName(),
          taskSpec.getTaskAttemptID());
      LOG.info("Initialized Input with src edge: " + edgeName);
      return null;
    }
  }

  private static class StartInputCallable extends CallableWithNdc<Void> {
    private final LogicalInput input;
    private final String srcVertexName;

    public StartInputCallable(LogicalInput input, String srcVertexName) {
      this.input = input;
      this.srcVertexName = srcVertexName;
    }

    @Override
    protected Void callInternal() throws Exception {
      LOG.info("Starting Input with src edge: " + srcVertexName);
      input.start();
      LOG.info("Started Input with src edge: " + srcVertexName);
      return null;
    }
  }

  private class InitializeOutputCallable extends CallableWithNdc<Void> {

    private final OutputSpec outputSpec;
    private final int outputIndex;

    public InitializeOutputCallable(OutputSpec outputSpec, int outputIndex) {
      this.outputSpec = outputSpec;
      this.outputIndex = outputIndex;
    }

    @Override
    protected Void callInternal() throws Exception {
      LOG.info("Initializing Output using OutputSpec: " + outputSpec);
      String edgeName = outputSpec.getDestinationVertexName();
      OutputContext outputContext = createOutputContext(outputSpec, outputIndex);
      LogicalOutput output = createOutput(outputSpec, outputContext);

      outputsMap.put(edgeName, output);
      outputContextMap.put(edgeName, outputContext);

      LOG.info("Initializing Output with dest edge: " + edgeName);
      List<Event> events = ((OutputFrameworkInterface)output).initialize();
      sendTaskGeneratedEvents(events, EventProducerConsumerType.OUTPUT,
          outputContext.getTaskVertexName(),
          outputContext.getDestinationVertexName(), taskSpec.getTaskAttemptID());
      LOG.info("Initialized Output with dest edge: " + edgeName);
      return null;
    }
  }

  private boolean inputAlreadyStarted(String vertexName, String edgeVertexName) {
    if (startedInputsMap.containsKey(vertexName)
        && startedInputsMap.get(vertexName).contains(edgeVertexName)) {
      return true;
    }
    return false;
  }

  private void initializeGroupInputs() {
    if (groupInputSpecs != null && !groupInputSpecs.isEmpty()) {
     groupInputsMap = new ConcurrentHashMap<String, MergedLogicalInput>(groupInputSpecs.size());
     for (GroupInputSpec groupInputSpec : groupInputSpecs) {
        LOG.info("Initializing GroupInput using GroupInputSpec: " + groupInputSpec);
       MergedInputContext mergedInputContext =
           new TezMergedInputContextImpl(groupInputSpec.getMergedInputDescriptor().getUserPayload(),
               groupInputSpec.getGroupName(), groupInputsMap, inputReadyTracker, localDirs);
       List<Input> inputs = Lists.newArrayListWithCapacity(groupInputSpec.getGroupVertices().size());
       for (String groupVertex : groupInputSpec.getGroupVertices()) {
         inputs.add(inputsMap.get(groupVertex));
       }

       MergedLogicalInput groupInput =
           (MergedLogicalInput) createMergedInput(groupInputSpec.getMergedInputDescriptor(),
               mergedInputContext, inputs);

        groupInputsMap.put(groupInputSpec.getGroupName(), groupInput);
      }
    }
  }

  private void initializeLogicalIOProcessor() throws Exception {
    LOG.info("Initializing processor" + ", processorClassName="
        + processorDescriptor.getClassName());
    processor.initialize();
    LOG.info("Initialized processor" + ", processorClassName="
        + processorDescriptor.getClassName());
  }

  private InputContext createInputContext(Map<String, LogicalInput> inputMap,
                                             InputSpec inputSpec, int inputIndex) {
    InputContext inputContext = new TezInputContextImpl(tezConf, localDirs,
        appAttemptNumber, tezUmbilical,
        taskSpec.getDAGName(), taskSpec.getVertexName(),
        inputSpec.getSourceVertexName(),
        taskSpec.getVertexParallelism(),
        taskSpec.getTaskAttemptID(),
        inputIndex,
        inputSpec.getInputDescriptor().getUserPayload(), this,
        serviceConsumerMetadata, envMap, initialMemoryDistributor,
        inputSpec.getInputDescriptor(), inputMap, inputReadyTracker, objectRegistry,
        ExecutionContext, memAvailable);
    return inputContext;
  }

  private OutputContext createOutputContext(OutputSpec outputSpec, int outputIndex) {
    OutputContext outputContext = new TezOutputContextImpl(tezConf, localDirs,
        appAttemptNumber, tezUmbilical,
        taskSpec.getDAGName(), taskSpec.getVertexName(),
        outputSpec.getDestinationVertexName(),
        taskSpec.getVertexParallelism(),
        taskSpec.getTaskAttemptID(),
        outputIndex,
        outputSpec.getOutputDescriptor().getUserPayload(), this,
        serviceConsumerMetadata, envMap, initialMemoryDistributor,
        outputSpec.getOutputDescriptor(), objectRegistry, ExecutionContext,
        memAvailable);
    return outputContext;
  }

  private ProcessorContext createProcessorContext() {
    ProcessorContext processorContext = new TezProcessorContextImpl(tezConf, localDirs,
        appAttemptNumber, tezUmbilical,
        taskSpec.getDAGName(), taskSpec.getVertexName(),
        taskSpec.getVertexParallelism(),
        taskSpec.getTaskAttemptID(),
        processorDescriptor.getUserPayload(), this,
        serviceConsumerMetadata, envMap, initialMemoryDistributor,
        processorDescriptor, inputReadyTracker, objectRegistry, ExecutionContext, memAvailable);
    return processorContext;
  }

  private LogicalInput createInput(InputSpec inputSpec, InputContext inputContext) {
    LOG.info("Creating Input");
    InputDescriptor inputDesc = inputSpec.getInputDescriptor();
    Input input = ReflectionUtils.createClazzInstance(inputDesc.getClassName(),
        new Class[]{InputContext.class, Integer.TYPE},
        new Object[]{inputContext, inputSpec.getPhysicalEdgeCount()});
    if (!(input instanceof LogicalInput)) {
      throw new TezUncheckedException(inputDesc.getClass().getName()
          + " is not a sub-type of LogicalInput."
          + " Only LogicalInput sub-types supported by LogicalIOProcessor.");
    }
    return (LogicalInput) input;
  }

  private LogicalInput createMergedInput(InputDescriptor inputDesc,
                                         MergedInputContext mergedInputContext,
                                         List<Input> constituentInputs) {
    LogicalInput input = ReflectionUtils.createClazzInstance(inputDesc.getClassName(),
        new Class[]{MergedInputContext.class, List.class},
        new Object[]{mergedInputContext, constituentInputs});
    return input;
  }

  private LogicalOutput createOutput(OutputSpec outputSpec, OutputContext outputContext) {
    LOG.info("Creating Output");
    OutputDescriptor outputDesc = outputSpec.getOutputDescriptor();
    Output output = ReflectionUtils.createClazzInstance(outputDesc.getClassName(),
        new Class[]{OutputContext.class, Integer.TYPE},
        new Object[]{outputContext, outputSpec.getPhysicalEdgeCount()});

    if (!(output instanceof LogicalOutput)) {
      throw new TezUncheckedException(output.getClass().getName()
          + " is not a sub-type of LogicalOutput."
          + " Only LogicalOutput sub-types supported by LogicalIOProcessor.");
    }
    return (LogicalOutput) output;
  }

  private AbstractLogicalIOProcessor createProcessor(
      String processorClassName, ProcessorContext processorContext) {
    Processor processor = ReflectionUtils.createClazzInstance(processorClassName,
        new Class[]{ProcessorContext.class}, new Object[]{processorContext});
    if (!(processor instanceof AbstractLogicalIOProcessor)) {
      throw new TezUncheckedException(processor.getClass().getName()
          + " is not a sub-type of AbstractLogicalIOProcessor."
          + " Only AbstractLogicalIOProcessor sub-types supported by LogicalIOProcessorRuntimeTask.");
    }
    return (AbstractLogicalIOProcessor) processor;
  }

  private void sendTaskGeneratedEvents(List<Event> events,
      EventProducerConsumerType generator, String taskVertexName,
      String edgeVertexName, TezTaskAttemptID taskAttemptID) {
    if (events == null || events.isEmpty()) {
      return;
    }
    EventMetaData eventMetaData = new EventMetaData(generator,
        taskVertexName, edgeVertexName, taskAttemptID);
    List<TezEvent> tezEvents = new ArrayList<TezEvent>(events.size());
    for (Event e : events) {
      TezEvent te = new TezEvent(e, eventMetaData);
      tezEvents.add(te);
    }
    if (LOG.isDebugEnabled()) {
      for (TezEvent e : tezEvents) {
        LOG.debug("Generated event info"
            + ", eventMetaData=" + eventMetaData.toString()
            + ", eventType=" + e.getEventType());
      }
    }
    tezUmbilical.addEvents(tezEvents);
  }

  private boolean handleEvent(TezEvent e) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Handling TezEvent in task"
          + ", taskAttemptId=" + taskSpec.getTaskAttemptID()
          + ", eventType=" + e.getEventType()
          + ", eventSourceInfo=" + e.getSourceInfo()
          + ", eventDestinationInfo=" + e.getDestinationInfo());
    }
    try {
      switch (e.getDestinationInfo().getEventGenerator()) {
      case INPUT:
        LogicalInput input = inputsMap.get(
            e.getDestinationInfo().getEdgeVertexName());
        if (input != null) {
          ((InputFrameworkInterface)input).handleEvents(Collections.singletonList(e.getEvent()));
        } else {
          throw new TezUncheckedException("Unhandled event for invalid target: "
              + e);
        }
        break;
      case OUTPUT:
        LogicalOutput output = outputsMap.get(
            e.getDestinationInfo().getEdgeVertexName());
        if (output != null) {
          ((OutputFrameworkInterface)output).handleEvents(Collections.singletonList(e.getEvent()));
        } else {
          throw new TezUncheckedException("Unhandled event for invalid target: "
              + e);
        }
        break;
      case PROCESSOR:
        processor.handleEvents(Collections.singletonList(e.getEvent()));
        break;
      case SYSTEM:
        LOG.warn("Trying to send a System event in a Task: " + e);
        break;
      }
    } catch (Throwable t) {
      LOG.warn("Failed to handle event", t);
      setFatalError(t, "Failed to handle event");
      EventMetaData sourceInfo = new EventMetaData(
          e.getDestinationInfo().getEventGenerator(),
          taskSpec.getVertexName(), e.getDestinationInfo().getEdgeVertexName(),
          getTaskAttemptID());
      setFrameworkCounters();
      tezUmbilical.signalFatalError(getTaskAttemptID(),
          t, ExceptionUtils.getStackTrace(t), sourceInfo);
      return false;
    }
    return true;
  }

  @Override
  public synchronized void handleEvents(Collection<TezEvent> events) {
    if (events == null || events.isEmpty()) {
      return;
    }
    eventCounter.addAndGet(events.size());
    if (LOG.isDebugEnabled()) {
      LOG.debug("Received events to be processed by task"
          + ", taskAttemptId=" + taskSpec.getTaskAttemptID()
          + ", eventCount=" + events.size()
          + ", newEventCounter=" + eventCounter.get());
    }
    eventsToBeProcessed.addAll(events);
  }

  private void startRouterThread() {
    eventRouterThread = new Thread(new RunnableWithNdc() {
      public void runInternal() {
        while (!isTaskDone() && !Thread.currentThread().isInterrupted()) {
          try {
            TezEvent e = eventsToBeProcessed.take();
            if (e == null) {
              continue;
            }
            // TODO TODONEWTEZ
            if (!handleEvent(e)) {
              LOG.warn("Stopping Event Router thread as failed to handle"
                  + " event: " + e);
              return;
            }
          } catch (InterruptedException e) {
            if (!isTaskDone()) {
              LOG.warn("Event Router thread interrupted. Returning.");
            }
            return;
          }
        }
      }
    });

    eventRouterThread.setName("TezTaskEventRouter["
        + taskSpec.getTaskAttemptID().toString() + "]");
    eventRouterThread.start();
  }

  private void closeContexts() throws IOException {
    closeContext(inputContextMap);
    closeContext(outputContextMap);
    closeContext(processorContext);
  }

  private void closeContext(Map<String, ? extends TaskContext> contextMap) throws IOException {
    if (contextMap == null) {
      return;
    }

    for(TaskContext context : contextMap.values()) {
      closeContext(context);
    }
    contextMap.clear();
  }

  private void closeContext(TaskContext context) throws IOException {
    if (context != null && (context instanceof Closeable)) {
      ((Closeable) context).close();
    }
  }

  public void cleanup() throws InterruptedException {
    LOG.info("Final Counters : " + getCounters().toShortString());
    setTaskDone();
    if (eventRouterThread != null) {
      eventRouterThread.interrupt();
      LOG.info("Joining on EventRouter");
      try {
        eventRouterThread.join();
      } catch (InterruptedException e) {
        LOG.info("Ignoring interrupt while waiting for the router thread to die");
        Thread.currentThread().interrupt();
      }
      eventRouterThread = null;
    }
    try {
      closeContexts();
      // Cleanup references which may be held by misbehaved tasks.
      cleanupStructures();
    } catch (IOException e) {
      LOG.info("Error while cleaning up contexts ", e);
    }
  }

  private void cleanupStructures() {
    if (initializerExecutor != null && !initializerExecutor.isShutdown()) {
      initializerExecutor.shutdownNow();
    }
    inputsMap.clear();
    outputsMap.clear();



    inputsMap.clear();
    outputsMap.clear();

    inputContextMap.clear();
    outputContextMap.clear();

    // only clean up objects in non-local mode, because local mode share the same 
    // taskSpec in AM rather than getting it through RPC in non-local mode
    /** Put other objects here when they are shared between AM & TezChild in local mode **/
    if (!tezConf.getBoolean(TezConfiguration.TEZ_LOCAL_MODE, TezConfiguration.TEZ_LOCAL_MODE_DEFAULT)) {
      inputSpecs.clear();
      outputSpecs.clear();
      if (groupInputSpecs != null) {
        groupInputSpecs.clear();
      }
    }
    if (groupInputsMap != null) {
      groupInputsMap.clear();
      groupInputsMap = null;
    }

    processor = null;
    processorContext = null;

    runInputMap.clear();
    runOutputMap.clear();

    eventsToBeProcessed.clear();
    inputReadyTracker = null;
    objectRegistry = null;
  }
  
  @Private
  @VisibleForTesting
  public Collection<InputContext> getInputContexts() {
    return this.inputContextMap.values();
  }
  
  @Private
  @VisibleForTesting
  public Collection<OutputContext> getOutputContexts() {
    return this.outputContextMap.values();
  }

  @Private
  @VisibleForTesting
  public ProcessorContext getProcessorContext() {
    return this.processorContext;
  }
  
  @Private
  @VisibleForTesting
  public LogicalIOProcessor getProcessor() {
    return this.processor;
  }

  @Private
  @VisibleForTesting
  public Map<String, LogicalInput> getInputs() {
    return this.inputsMap;
  }

  @Private
  @VisibleForTesting
  public Map<String, LogicalOutput> getOutputs() {
    return this.outputsMap;
  }

}
