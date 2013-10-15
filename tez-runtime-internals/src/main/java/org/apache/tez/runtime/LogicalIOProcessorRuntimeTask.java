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
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.Output;
import org.apache.tez.runtime.api.Processor;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.api.TezOutputContext;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.api.impl.EventMetaData;
import org.apache.tez.runtime.api.impl.InputSpec;
import org.apache.tez.runtime.api.impl.OutputSpec;
import org.apache.tez.runtime.api.impl.TaskSpec;
import org.apache.tez.runtime.api.impl.TezEvent;
import org.apache.tez.runtime.api.impl.TezInputContextImpl;
import org.apache.tez.runtime.api.impl.TezOutputContextImpl;
import org.apache.tez.runtime.api.impl.TezProcessorContextImpl;
import org.apache.tez.runtime.api.impl.TezUmbilical;
import org.apache.tez.runtime.api.impl.EventMetaData.EventProducerConsumerType;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
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
  
  private final ProcessorDescriptor processorDescriptor;
  private final LogicalIOProcessor processor;
  private TezProcessorContext processorContext;

  /** Maps which will be provided to the processor run method */
  private final LinkedHashMap<String, LogicalInput> runInputMap;
  private final LinkedHashMap<String, LogicalOutput> runOutputMap;
  
  private final Map<String, ByteBuffer> serviceConsumerMetadata;
  
  private final ExecutorService initializerExecutor;
  private final CompletionService<Void> initializerCompletionService;

  private LinkedBlockingQueue<TezEvent> eventsToBeProcessed;
  private Thread eventRouterThread = null;

  private final int appAttemptNumber;

  public LogicalIOProcessorRuntimeTask(TaskSpec taskSpec, int appAttemptNumber,
      Configuration tezConf, TezUmbilical tezUmbilical,
      Map<String, ByteBuffer> serviceConsumerMetadata) throws IOException {
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
    int numInitializers = numInputs + numOutputs;
    this.initializerExecutor = Executors.newFixedThreadPool(
        numInitializers,
        new ThreadFactoryBuilder().setDaemon(true)
            .setNameFormat("Initializer %d").build());
    this.initializerCompletionService = new ExecutorCompletionService<Void>(
        this.initializerExecutor);
  }

  public void initialize() throws Exception {
    LOG.info("Initializing LogicalProcessorIORuntimeTask");
    Preconditions.checkState(this.state == State.NEW, "Already initialized");
    this.state = State.INITED;
    
    int numTasks = 0;
    
    for (InputSpec inputSpec : taskSpec.getInputs()) {
      this.initializerCompletionService.submit(new InitializeInputCallable(inputSpec));
      numTasks++;
    }
    
    for (OutputSpec outputSpec : taskSpec.getOutputs()) {
      this.initializerCompletionService.submit(new InitializeOutputCallable(outputSpec));
      numTasks++;
    }
    // Shutdown after all tasks complete.
    this.initializerExecutor.shutdown();
    
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

    // Construct Inputs/Outputs map argument for processor.run()
    for (InputSpec inputSpec : inputSpecs) {
      LogicalInput input = inputsMap.get(inputSpec.getSourceVertexName());
      runInputMap.put(inputSpec.getSourceVertexName(), input);
    }
    for (OutputSpec outputSpec : outputSpecs) {
      LogicalOutput output = outputsMap.get(outputSpec.getDestinationVertexName());
      runOutputMap.put(outputSpec.getDestinationVertexName(), output);
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

      // Close the Inputs.
      for (InputSpec inputSpec : inputSpecs) {
        String srcVertexName = inputSpec.getSourceVertexName();
        List<Event> closeInputEvents = inputsMap.get(srcVertexName).close();
        sendTaskGeneratedEvents(closeInputEvents,
            EventProducerConsumerType.INPUT, taskSpec.getVertexName(),
            srcVertexName, taskSpec.getTaskAttemptID());
      }

      // Close the Processor.
      processor.close();

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

    public InitializeInputCallable(InputSpec inputSpec) {
      this.inputSpec = inputSpec;
    }

    @Override
    public Void call() throws Exception {
      LOG.info("Initializing Input using InputSpec: " + inputSpec);
      String edgeName = inputSpec.getSourceVertexName();
      LogicalInput input = createInput(inputSpec);
      TezInputContext inputContext = createInputContext(inputSpec);
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

  private class InitializeOutputCallable implements Callable<Void> {

    private final OutputSpec outputSpec;

    public InitializeOutputCallable(OutputSpec outputSpec) {
      this.outputSpec = outputSpec;
    }

    @Override
    public Void call() throws Exception {
      LOG.info("Initializing Output using OutputSpec: " + outputSpec);
      String edgeName = outputSpec.getDestinationVertexName();
      LogicalOutput output = createOutput(outputSpec);
      TezOutputContext outputContext = createOutputContext(outputSpec);
      outputsMap.put(edgeName, output);
      outputContextMap.put(edgeName, outputContext);

      if (output instanceof LogicalOutput) {
        ((LogicalOutput) output).setNumPhysicalOutputs(outputSpec
            .getPhysicalEdgeCount());
      }
      LOG.info("Initializing Input with dest edge: " + edgeName);
      List<Event> events = output.initialize(outputContext);
      sendTaskGeneratedEvents(events, EventProducerConsumerType.OUTPUT,
          outputContext.getTaskVertexName(),
          outputContext.getDestinationVertexName(), taskSpec.getTaskAttemptID());
      LOG.info("Initialized Output with dest edge: " + edgeName);
      return null;
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

  private TezInputContext createInputContext(InputSpec inputSpec) {
    TezInputContext inputContext = new TezInputContextImpl(tezConf,
        appAttemptNumber, tezUmbilical, taskSpec.getVertexName(),
        inputSpec.getSourceVertexName(), taskSpec.getTaskAttemptID(),
        tezCounters,
        inputSpec.getInputDescriptor().getUserPayload() == null ? taskSpec
            .getProcessorDescriptor().getUserPayload() : inputSpec
            .getInputDescriptor().getUserPayload(), this,
        serviceConsumerMetadata, System.getenv());
    return inputContext;
  }

  private TezOutputContext createOutputContext(OutputSpec outputSpec) {
    TezOutputContext outputContext = new TezOutputContextImpl(tezConf,
        appAttemptNumber, tezUmbilical, taskSpec.getVertexName(),
        outputSpec.getDestinationVertexName(), taskSpec.getTaskAttemptID(),
        tezCounters,
        outputSpec.getOutputDescriptor().getUserPayload() == null ? taskSpec
            .getProcessorDescriptor().getUserPayload() : outputSpec
            .getOutputDescriptor().getUserPayload(), this,
        serviceConsumerMetadata, System.getenv());
    return outputContext;
  }

  private TezProcessorContext createProcessorContext() {
    TezProcessorContext processorContext = new TezProcessorContextImpl(tezConf,
        appAttemptNumber, tezUmbilical, taskSpec.getVertexName(), taskSpec.getTaskAttemptID(),
        tezCounters, processorDescriptor.getUserPayload(), this,
        serviceConsumerMetadata, System.getenv());
    return processorContext;
  }

  private LogicalInput createInput(InputSpec inputSpec) {
    LOG.info("Creating Input");
    Input input = RuntimeUtils.createClazzInstance(inputSpec
        .getInputDescriptor().getClassName());
    if (!(input instanceof LogicalInput)) {
      throw new TezUncheckedException(input.getClass().getName()
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
