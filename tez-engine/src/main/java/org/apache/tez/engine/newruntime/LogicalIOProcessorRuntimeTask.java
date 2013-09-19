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

package org.apache.tez.engine.newruntime;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
import org.apache.tez.dag.records.TezTaskAttemptID;
import org.apache.tez.engine.common.security.JobTokenIdentifier;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.Input;
import org.apache.tez.engine.newapi.LogicalIOProcessor;
import org.apache.tez.engine.newapi.LogicalInput;
import org.apache.tez.engine.newapi.LogicalOutput;
import org.apache.tez.engine.newapi.Output;
import org.apache.tez.engine.newapi.Processor;
import org.apache.tez.engine.newapi.TezInputContext;
import org.apache.tez.engine.newapi.TezOutputContext;
import org.apache.tez.engine.newapi.TezProcessorContext;
import org.apache.tez.engine.newapi.impl.EventMetaData;
import org.apache.tez.engine.newapi.impl.EventMetaData.EventProducerConsumerType;
import org.apache.tez.engine.newapi.impl.InputSpec;
import org.apache.tez.engine.newapi.impl.OutputSpec;
import org.apache.tez.engine.newapi.impl.TaskSpec;
import org.apache.tez.engine.newapi.impl.TezEvent;
import org.apache.tez.engine.newapi.impl.TezInputContextImpl;
import org.apache.tez.engine.newapi.impl.TezOutputContextImpl;
import org.apache.tez.engine.newapi.impl.TezProcessorContextImpl;
import org.apache.tez.engine.newapi.impl.TezUmbilical;
import org.apache.tez.engine.shuffle.common.ShuffleUtils;

import com.google.common.base.Preconditions;

@Private
public class LogicalIOProcessorRuntimeTask extends RuntimeTask {

  private static final Log LOG = LogFactory
      .getLog(LogicalIOProcessorRuntimeTask.class);

  private final List<InputSpec> inputSpecs;
  private final List<LogicalInput> inputs;

  private final List<OutputSpec> outputSpecs;
  private final List<LogicalOutput> outputs;

  private final ProcessorDescriptor processorDescriptor;
  private final LogicalIOProcessor processor;

  private final Map<String, ByteBuffer> serviceConsumerMetadata;

  private Map<String, LogicalInput> inputMap;
  private Map<String, LogicalOutput> outputMap;

  private LinkedBlockingQueue<TezEvent> eventsToBeProcessed;
  private Thread eventRouterThread = null;

  private final int appAttemptNumber;

  public LogicalIOProcessorRuntimeTask(TaskSpec taskSpec, int appAttemptNumber,
      Configuration tezConf, TezUmbilical tezUmbilical,
      Token<JobTokenIdentifier> jobToken) throws IOException {
    // TODO Remove jobToken from here post TEZ-421
    super(taskSpec, tezConf, tezUmbilical);
    LOG.info("Initializing LogicalIOProcessorRuntimeTask with TaskSpec: "
        + taskSpec);
    this.inputSpecs = taskSpec.getInputs();
    this.inputs = createInputs(inputSpecs);
    this.outputSpecs = taskSpec.getOutputs();
    this.outputs = createOutputs(outputSpecs);
    this.processorDescriptor = taskSpec.getProcessorDescriptor();
    this.processor = createProcessor(processorDescriptor);
    this.serviceConsumerMetadata = new HashMap<String, ByteBuffer>();
    this.serviceConsumerMetadata.put(ShuffleUtils.SHUFFLE_HANDLER_SERVICE_ID,
        ShuffleUtils.convertJobTokenToBytes(jobToken));
    this.eventsToBeProcessed = new LinkedBlockingQueue<TezEvent>();
    this.state = State.NEW;
    this.appAttemptNumber = appAttemptNumber;
  }

  public void initialize() throws Exception {
    LOG.info("Initializing LogicalProcessorIORuntimeTask");
    Preconditions.checkState(this.state == State.NEW, "Already initialized");
    this.state = State.INITED;
    inputMap = new LinkedHashMap<String, LogicalInput>(inputs.size());
    outputMap = new LinkedHashMap<String, LogicalOutput>(outputs.size());

    // TODO Maybe close initialized inputs / outputs in case of failure to
    // initialize.
    // Initialize all inputs. TODO: Multi-threaded at some point.
    for (int i = 0; i < inputs.size(); i++) {
      String srcVertexName = inputSpecs.get(i).getSourceVertexName();
      initializeInput(inputs.get(i),
          inputSpecs.get(i));
      inputMap.put(srcVertexName, inputs.get(i));
    }

    // Initialize all outputs. TODO: Multi-threaded at some point.
    for (int i = 0; i < outputs.size(); i++) {
      String destVertexName = outputSpecs.get(i).getDestinationVertexName();
      initializeOutput(outputs.get(i), outputSpecs.get(i));
      outputMap.put(destVertexName, outputs.get(i));
    }

    // Initialize processor.
    initializeLogicalIOProcessor();
    startRouterThread();
  }

  public void run() throws Exception {
    synchronized (this.state) {
      Preconditions.checkState(this.state == State.INITED,
          "Can only run while in INITED state. Current: " + this.state);
      this.state = State.RUNNING;
    }
    LogicalIOProcessor lioProcessor = (LogicalIOProcessor) processor;
    lioProcessor.run(inputMap, outputMap);
  }

  public void close() throws Exception {
    try {
      Preconditions.checkState(this.state == State.RUNNING,
          "Can only run while in RUNNING state. Current: " + this.state);
      this.state = State.CLOSED;

      // Close the Inputs.
      for (int i = 0; i < inputs.size(); i++) {
        String srcVertexName = inputSpecs.get(i).getSourceVertexName();
        List<Event> closeInputEvents = inputs.get(i).close();
        sendTaskGeneratedEvents(closeInputEvents,
            EventProducerConsumerType.INPUT, taskSpec.getVertexName(),
            srcVertexName, taskSpec.getTaskAttemptID());
      }

      // Close the Processor.
      processor.close();

      // Close the Outputs.
      for (int i = 0; i < outputs.size(); i++) {
        String destVertexName = outputSpecs.get(i).getDestinationVertexName();
        List<Event> closeOutputEvents = outputs.get(i).close();
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

  private void initializeInput(Input input, InputSpec inputSpec)
      throws Exception {
    TezInputContext tezInputContext = createInputContext(inputSpec);
    if (input instanceof LogicalInput) {
      ((LogicalInput) input).setNumPhysicalInputs(inputSpec
          .getPhysicalEdgeCount());
    }
    LOG.info("Initializing Input using InputSpec: " + inputSpec);
    List<Event> events = input.initialize(tezInputContext);
    sendTaskGeneratedEvents(events, EventProducerConsumerType.INPUT,
        tezInputContext.getTaskVertexName(),
        tezInputContext.getSourceVertexName(), taskSpec.getTaskAttemptID());
  }

  private void initializeOutput(Output output, OutputSpec outputSpec)
      throws Exception {
    TezOutputContext tezOutputContext = createOutputContext(outputSpec);
    if (output instanceof LogicalOutput) {
      ((LogicalOutput) output).setNumPhysicalOutputs(outputSpec
          .getPhysicalEdgeCount());
    }
    LOG.info("Initializing Output using OutputSpec: " + outputSpec);
    List<Event> events = output.initialize(tezOutputContext);
    sendTaskGeneratedEvents(events, EventProducerConsumerType.OUTPUT,
        tezOutputContext.getTaskVertexName(),
        tezOutputContext.getDestinationVertexName(),
        taskSpec.getTaskAttemptID());
  }

  private void initializeLogicalIOProcessor() throws Exception {
    LOG.info("Initializing processor"
        + ", processorClassName=" + processorDescriptor.getClassName());
    TezProcessorContext processorContext = createProcessorContext();
    processor.initialize(processorContext);
  }

  private TezInputContext createInputContext(InputSpec inputSpec) {
    TezInputContext inputContext = new TezInputContextImpl(tezConf,
        appAttemptNumber, tezUmbilical, taskSpec.getVertexName(),
        inputSpec.getSourceVertexName(), taskSpec.getTaskAttemptID(),
        tezCounters,
        inputSpec.getInputDescriptor().getUserPayload() == null ? taskSpec
            .getProcessorDescriptor().getUserPayload() : inputSpec
            .getInputDescriptor().getUserPayload(), this,
        serviceConsumerMetadata);
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
        serviceConsumerMetadata);
    return outputContext;
  }

  private TezProcessorContext createProcessorContext() {
    TezProcessorContext processorContext = new TezProcessorContextImpl(tezConf,
        appAttemptNumber, tezUmbilical, taskSpec.getVertexName(), taskSpec.getTaskAttemptID(),
        tezCounters, processorDescriptor.getUserPayload(), this,
        serviceConsumerMetadata);
    return processorContext;
  }

  private List<LogicalInput> createInputs(List<InputSpec> inputSpecs) {
    List<LogicalInput> inputs = new ArrayList<LogicalInput>(inputSpecs.size());
    for (InputSpec inputSpec : inputSpecs) {
      LOG.info("Creating Input from InputSpec: "
          + inputSpec);
      Input input = RuntimeUtils.createClazzInstance(inputSpec
          .getInputDescriptor().getClassName());

      if (input instanceof LogicalInput) {
        inputs.add((LogicalInput) input);
      } else {
        throw new TezUncheckedException(input.getClass().getName()
            + " is not a sub-type of LogicalInput."
            + " Only LogicalInput sub-types supported by LogicalIOProcessor.");
      }
    }
    return inputs;
  }

  private List<LogicalOutput> createOutputs(List<OutputSpec> outputSpecs) {
    List<LogicalOutput> outputs = new ArrayList<LogicalOutput>(
        outputSpecs.size());
    for (OutputSpec outputSpec : outputSpecs) {
      LOG.info("Creating Output from OutputSpec"
          + outputSpec);
      Output output = RuntimeUtils.createClazzInstance(outputSpec
          .getOutputDescriptor().getClassName());
      if (output instanceof LogicalOutput) {
        outputs.add((LogicalOutput) output);
      } else {
        throw new TezUncheckedException(output.getClass().getName()
            + " is not a sub-type of LogicalOutput."
            + " Only LogicalOutput sub-types supported by LogicalIOProcessor.");
      }
    }
    return outputs;
  }

  private LogicalIOProcessor createProcessor(
      ProcessorDescriptor processorDescriptor) {
    Processor processor = RuntimeUtils.createClazzInstance(processorDescriptor
        .getClassName());
    if (!(processor instanceof LogicalIOProcessor)) {
      throw new TezUncheckedException(processor.getClass().getName()
          + " is not a sub-type of LogicalIOProcessor."
          + " Only LogicalOutput sub-types supported by LogicalIOProcessor.");
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
        LogicalInput input = inputMap.get(
            e.getDestinationInfo().getEdgeVertexName());
        if (input != null) {
          input.handleEvents(Collections.singletonList(e.getEvent()));
        } else {
          throw new TezUncheckedException("Unhandled event for invalid target: "
              + e);
        }
        break;
      case OUTPUT:
        LogicalOutput output = outputMap.get(
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

}
