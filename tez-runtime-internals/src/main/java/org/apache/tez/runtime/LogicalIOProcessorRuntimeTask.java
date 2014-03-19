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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.MergedLogicalInput;
import org.apache.tez.runtime.api.Output;
import org.apache.tez.runtime.api.Processor;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.runtime.api.impl.GroupInputSpec;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezInputContextImpl;
import org.apache.tez.runtime.api.impl.TezOutputContextImpl;
import org.apache.tez.runtime.api.impl.TezProcessorContextImpl;
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

  private static final Log LOG = LogFactory
      .getLog(LogicalIOProcessorRuntimeTask.class);

  /** Responsible for maintaining order of Inputs */
  private final List<InputSpec> inputSpecs;
  private final ConcurrentHashMap<String, LogicalInput> inputsMap;
  private final ConcurrentHashMap<String, TezInputContext> inputContextMap;
  /** Responsible for maintaining order of Outputs */
  private final List<OutputSpec> outputSpecs;
  private final ConcurrentHashMap<String, LogicalOutput> outputsMap;
  private final ConcurrentHashMap<String, TezOutputContext> outputContextMap;
  
  private final List<GroupInputSpec> groupInputSpecs;
  private ConcurrentHashMap<String, MergedLogicalInput> groupInputsMap;
  
  private final ProcessorDescriptor processorDescriptor;
  private final LogicalIOProcessor processor;
  private TezProcessorContext processorContext;
  
  private final MemoryDistributor initialMemoryDistributor;

  /** Maps which will be provided to the processor run method */
  private final LinkedHashMap<String, LogicalInput> runInputMap;
  private final LinkedHashMap<String, LogicalOutput> runOutputMap;
  
  private final Map<String, ByteBuffer> serviceConsumerMetadata;
  
  private final ExecutorService initializerExecutor;
  private final CompletionService<Void> initializerCompletionService;
  
  private final Multimap<String, String> startedInputsMap;

  private LinkedBlockingQueue<TezEvent> eventsToBeProcessed;
  private Thread eventRouterThread = null;

  private final int appAttemptNumber;
  
  private final InputReadyTracker inputReadyTracker;

  public LogicalIOProcessorRuntimeTask(TaskSpec taskSpec, int appAttemptNumber,
      Configuration tezConf, TezUmbilical tezUmbilical,
      Map<String, ByteBuffer> serviceConsumerMetadata,
      Multimap<String, String> startedInputsMap) throws IOException {
    // TODO Remove jobToken from here post TEZ-421
    super(taskSpec, tezConf, tezUmbilical);
    LOG.info("Initializing LogicalIOProcessorRuntimeTask with TaskSpec: "
        + taskSpec);
    int numInputs = taskSpec.getInputs().size();
    int numOutputs = taskSpec.getOutputs().size();
    this.inputSpecs = taskSpec.getInputs();
    this.inputsMap = new ConcurrentHashMap<String, LogicalInput>(numInputs);
    this.inputContextMap = new ConcurrentHashMap<String, TezInputContext>(numInputs);
    this.outputSpecs = taskSpec.getOutputs();
    this.outputsMap = new ConcurrentHashMap<String, LogicalOutput>(numOutputs);
    this.outputContextMap = new ConcurrentHashMap<String, TezOutputContext>(numOutputs);

    this.runInputMap = new LinkedHashMap<String, LogicalInput>();
    this.runOutputMap = new LinkedHashMap<String, LogicalOutput>();

    this.processorDescriptor = taskSpec.getProcessorDescriptor();
    this.processor = createProcessor(processorDescriptor);
    this.serviceConsumerMetadata = serviceConsumerMetadata;
    this.eventsToBeProcessed = new LinkedBlockingQueue<TezEvent>();
    this.state = State.NEW;
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
  }

  /**
   * @throws Exception
   */
  public void initialize() throws Exception {
    LOG.info("Initializing LogicalProcessorIORuntimeTask");
    Preconditions.checkState(this.state == State.NEW, "Already initialized");
    this.state = State.INITED;
    
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
    synchronized (this.state) {
      Preconditions.checkState(this.state == State.INITED,
          "Can only run while in INITED state. Current: " + this.state);
      this.state = State.RUNNING;
    }
    LogicalIOProcessor lioProcessor = (LogicalIOProcessor) processor;
    lioProcessor.run(runInputMap, runOutputMap);
  }

  public void close() throws Exception {
    try {
      Preconditions.checkState(this.state == State.RUNNING,
          "Can only run while in RUNNING state. Current: " + this.state);
      this.state = State.CLOSED;

      // Close the Processor.
      processor.close();

      // Close the Inputs.
      for (InputSpec inputSpec : inputSpecs) {
        String srcVertexName = inputSpec.getSourceVertexName();
        List<Event> closeInputEvents = inputsMap.get(srcVertexName).close();
        sendTaskGeneratedEvents(closeInputEvents,
            EventProducerConsumerType.INPUT, taskSpec.getVertexName(),
            srcVertexName, taskSpec.getTaskAttemptID());
      }

      // Close the Outputs.
      for (OutputSpec outputSpec : outputSpecs) {
        String destVertexName = outputSpec.getDestinationVertexName();
        List<Event> closeOutputEvents = outputsMap.get(destVertexName).close();
        sendTaskGeneratedEvents(closeOutputEvents,
            EventProducerConsumerType.OUTPUT, taskSpec.getVertexName(),
            destVertexName, taskSpec.getTaskAttemptID());
      }
    } finally {
      setTaskDone();
      if (eventRouterThread != null) {
        eventRouterThread.interrupt();
      }
    }
  }

  private class InitializeInputCallable implements Callable<Void> {

    private final InputSpec inputSpec;
    private final int inputIndex;

    public InitializeInputCallable(InputSpec inputSpec, int inputIndex) {
      this.inputSpec = inputSpec;
      this.inputIndex = inputIndex;
    }

    @Override
    public Void call() throws Exception {
      LOG.info("Initializing Input using InputSpec: " + inputSpec);
      String edgeName = inputSpec.getSourceVertexName();
      LogicalInput input = createInput(inputSpec);
      TezInputContext inputContext = createInputContext(input, inputSpec, inputIndex);
      inputsMap.put(edgeName, input);
      inputContextMap.put(edgeName, inputContext);

      if (input instanceof LogicalInput) {
        ((LogicalInput) input).setNumPhysicalInputs(inputSpec
            .getPhysicalEdgeCount());
      }
      LOG.info("Initializing Input with src edge: " + edgeName);
      List<Event> events = input.initialize(inputContext);
      sendTaskGeneratedEvents(events, EventProducerConsumerType.INPUT,
          inputContext.getTaskVertexName(), inputContext.getSourceVertexName(),
          taskSpec.getTaskAttemptID());
      LOG.info("Initialized Input with src edge: " + edgeName);
      return null;
    }
  }

  private class StartInputCallable implements Callable<Void> {
    private final LogicalInput input;
    private final String srcVertexName;
    
    public StartInputCallable(LogicalInput input, String srcVertexName) {
      this.input = input;
      this.srcVertexName = srcVertexName;
    }
    
    @Override
    public Void call() throws Exception {
      LOG.info("Starting Input with src edge: " + srcVertexName);
      input.start();
      LOG.info("Started Input with src edge: " + srcVertexName);
      return null;
    }
  }

  private class InitializeOutputCallable implements Callable<Void> {

    private final OutputSpec outputSpec;
    private final int outputIndex;

    public InitializeOutputCallable(OutputSpec outputSpec, int outputIndex) {
      this.outputSpec = outputSpec;
      this.outputIndex = outputIndex;
    }

    @Override
    public Void call() throws Exception {
      LOG.info("Initializing Output using OutputSpec: " + outputSpec);
      String edgeName = outputSpec.getDestinationVertexName();
      LogicalOutput output = createOutput(outputSpec);
      TezOutputContext outputContext = createOutputContext(outputSpec, outputIndex);
      outputsMap.put(edgeName, output);
      outputContextMap.put(edgeName, outputContext);

      if (output instanceof LogicalOutput) {
        ((LogicalOutput) output).setNumPhysicalOutputs(outputSpec
            .getPhysicalEdgeCount());
      }
      LOG.info("Initializing Output with dest edge: " + edgeName);
      List<Event> events = output.initialize(outputContext);
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
        MergedLogicalInput groupInput = (MergedLogicalInput) createInputFromDescriptor(
            groupInputSpec.getMergedInputDescriptor());
        List<Input> inputs = Lists.newArrayListWithCapacity(groupInputSpec.getGroupVertices().size());
        for (String groupVertex : groupInputSpec.getGroupVertices()) {
          inputs.add(inputsMap.get(groupVertex));
        }
        groupInput.initialize(inputs);
        groupInputsMap.put(groupInputSpec.getGroupName(), groupInput);
      }
    }
  }
  
  private void initializeLogicalIOProcessor() throws Exception {
    LOG.info("Initializing processor" + ", processorClassName="
        + processorDescriptor.getClassName());
    TezProcessorContext processorContext = createProcessorContext();
    this.processorContext = processorContext;
    processor.initialize(processorContext);
    LOG.info("Initialized processor" + ", processorClassName="
        + processorDescriptor.getClassName());
  }

  private TezInputContext createInputContext(Input input, InputSpec inputSpec, int inputIndex) {
    TezInputContext inputContext = new TezInputContextImpl(tezConf,
        appAttemptNumber, tezUmbilical,
        taskSpec.getDAGName(), taskSpec.getVertexName(),
        inputSpec.getSourceVertexName(), taskSpec.getTaskAttemptID(),
        tezCounters, inputIndex,
        inputSpec.getInputDescriptor().getUserPayload() == null ? taskSpec
            .getProcessorDescriptor().getUserPayload() : inputSpec
            .getInputDescriptor().getUserPayload(), this,
        serviceConsumerMetadata, System.getenv(), initialMemoryDistributor,
        inputSpec.getInputDescriptor(), input, inputReadyTracker);
    return inputContext;
  }

  private TezOutputContext createOutputContext(OutputSpec outputSpec, int outputIndex) {
    TezOutputContext outputContext = new TezOutputContextImpl(tezConf,
        appAttemptNumber, tezUmbilical,
        taskSpec.getDAGName(), taskSpec.getVertexName(),
        outputSpec.getDestinationVertexName(), taskSpec.getTaskAttemptID(),
        tezCounters, outputIndex,
        outputSpec.getOutputDescriptor().getUserPayload() == null ? taskSpec
            .getProcessorDescriptor().getUserPayload() : outputSpec
            .getOutputDescriptor().getUserPayload(), this,
        serviceConsumerMetadata, System.getenv(), initialMemoryDistributor,
        outputSpec.getOutputDescriptor());
    return outputContext;
  }

  private TezProcessorContext createProcessorContext() {
    TezProcessorContext processorContext = new TezProcessorContextImpl(tezConf,
        appAttemptNumber, tezUmbilical,
        taskSpec.getDAGName(), taskSpec.getVertexName(),
        taskSpec.getTaskAttemptID(),
        tezCounters, processorDescriptor.getUserPayload(), this,
        serviceConsumerMetadata, System.getenv(), initialMemoryDistributor,
        processorDescriptor, inputReadyTracker);
    return processorContext;
  }

  private LogicalInput createInput(InputSpec inputSpec) {
    LOG.info("Creating Input");
    return createInputFromDescriptor(inputSpec.getInputDescriptor());
  }

  private LogicalInput createInputFromDescriptor(InputDescriptor inputDesc) {
    Input input = RuntimeUtils.createClazzInstance(inputDesc.getClassName());
    if (!(input instanceof LogicalInput)) {
      throw new TezUncheckedException(inputDesc.getClass().getName()
          + " is not a sub-type of LogicalInput."
          + " Only LogicalInput sub-types supported by LogicalIOProcessor.");
    }
    return (LogicalInput)input;
  }
  
  private LogicalOutput createOutput(OutputSpec outputSpec) {
    LOG.info("Creating Output");
    Output output = RuntimeUtils.createClazzInstance(outputSpec
        .getOutputDescriptor().getClassName());
    if (!(output instanceof LogicalOutput)) {
      throw new TezUncheckedException(output.getClass().getName()
          + " is not a sub-type of LogicalOutput."
          + " Only LogicalOutput sub-types supported by LogicalIOProcessor.");
    }
    return (LogicalOutput) output;
  }

  private LogicalIOProcessor createProcessor(
      ProcessorDescriptor processorDescriptor) {
    Processor processor = RuntimeUtils.createClazzInstance(processorDescriptor
        .getClassName());
    if (!(processor instanceof LogicalIOProcessor)) {
      throw new TezUncheckedException(processor.getClass().getName()
          + " is not a sub-type of LogicalIOProcessor."
          + " Only LogicalIOProcessor sub-types supported by LogicalIOProcessorRuntimeTask.");
    }
    return (LogicalIOProcessor) processor;
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
          input.handleEvents(Collections.singletonList(e.getEvent()));
        } else {
          throw new TezUncheckedException("Unhandled event for invalid target: "
              + e);
        }
        break;
      case OUTPUT:
        LogicalOutput output = outputsMap.get(
            e.getDestinationInfo().getEdgeVertexName());
        if (output != null) {
          output.handleEvents(Collections.singletonList(e.getEvent()));
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
      tezUmbilical.signalFatalError(getTaskAttemptID(),
          StringUtils.stringifyException(t), sourceInfo);
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
    eventRouterThread = new Thread(new Runnable() {
      public void run() {
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

  public synchronized void cleanup() {
    setTaskDone();
    if (eventRouterThread != null) {
      eventRouterThread.interrupt();
    }
  }
  
  @Private
  @VisibleForTesting
  public Collection<TezInputContext> getInputContexts() {
    return this.inputContextMap.values();
  }
  
  @Private
  @VisibleForTesting
  public Collection<TezOutputContext> getOutputContexts() {
    return this.outputContextMap.values();
  }

  @Private
  @VisibleForTesting
  public TezProcessorContext getProcessorContext() {
    return this.processorContext;
  }
  
  @Private
  @VisibleForTesting
  public LogicalIOProcessor getProcessor() {
    return this.processor;
  }

}
