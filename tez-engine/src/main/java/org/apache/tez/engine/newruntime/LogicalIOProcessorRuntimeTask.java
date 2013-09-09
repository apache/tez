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
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezUncheckedException;
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
import org.apache.tez.engine.newapi.impl.InputSpec;
import org.apache.tez.engine.newapi.impl.OutputSpec;
import org.apache.tez.engine.newapi.impl.TaskSpec;
import org.apache.tez.engine.newapi.impl.TezInputContextImpl;
import org.apache.tez.engine.newapi.impl.TezOutputContextImpl;
import org.apache.tez.engine.newapi.impl.TezProcessorContextImpl;

import com.google.common.base.Preconditions;

@Private
public class LogicalIOProcessorRuntimeTask {

  private enum State {
    NEW, INITED, RUNNING, CLOSED
  }

  private static final Log LOG = LogFactory
      .getLog(LogicalIOProcessorRuntimeTask.class);

  private final TaskSpec taskSpec;
  private final Configuration tezConf;

  private final List<InputSpec> inputSpecs;
  private final List<LogicalInput> inputs;

  private final List<OutputSpec> outputSpecs;
  private final List<LogicalOutput> outputs;

  private final ProcessorDescriptor processorDescriptor;
  private final LogicalIOProcessor processor;

  private final TezCounters tezCounters;

  private State state;

  private Map<String, LogicalInput> inputMap;
  private Map<String, LogicalOutput> outputMap;

  private Map<String, List<Event>> initInputEventMap;
  private Map<String, List<Event>> initOutputEventMap;

  private Map<String, List<Event>> closeInputEventMap;
  private Map<String, List<Event>> closeOutputEventMap;

  public LogicalIOProcessorRuntimeTask(TaskSpec taskSpec, Configuration tezConf) {
    LOG.info("Initializing LogicalIOProcessorRuntimeTask with TaskSpec: "
        + taskSpec);
    this.taskSpec = taskSpec;
    this.tezConf = tezConf;
    this.inputSpecs = taskSpec.getInputs();
    this.inputs = createInputs(inputSpecs);
    this.outputSpecs = taskSpec.getOutputs();
    this.outputs = createOutputs(outputSpecs);
    this.processorDescriptor = taskSpec.getProcessorDescriptor();
    this.processor = createProcessor(processorDescriptor);
    this.tezCounters = new TezCounters();
    this.state = State.NEW;
  }

  public void initialize() throws IOException {
    Preconditions.checkState(this.state == State.NEW, "Already initialized");
    this.state = State.INITED;
    inputMap = new LinkedHashMap<String, LogicalInput>(inputs.size());
    outputMap = new LinkedHashMap<String, LogicalOutput>(outputs.size());

    initInputEventMap = new LinkedHashMap<String, List<Event>>(inputs.size());
    initOutputEventMap = new LinkedHashMap<String, List<Event>>(outputs.size());

    // TODO Maybe close initialized inputs / outputs in case of failure to
    // initialize.
    // Initialize all inputs. TODO: Multi-threaded at some point.
    for (int i = 0; i < inputs.size(); i++) {
      String srcVertexName = inputSpecs.get(i).getSourceVertexName();
      List<Event> initInputEvents = initializeInput(inputs.get(i),
          inputSpecs.get(i));
      // TODO Add null/event list checking here or in the actual executor.
      initInputEventMap.put(srcVertexName, initInputEvents);
      inputMap.put(srcVertexName, inputs.get(i));
    }

    // Initialize all outputs. TODO: Multi-threaded at some point.
    for (int i = 0; i < outputs.size(); i++) {
      String destVertexName = outputSpecs.get(i).getDestinationVertexName();
      List<Event> initOutputEvents = initializeOutput(outputs.get(i),
          outputSpecs.get(i));
      // TODO Add null/event list checking here or in the actual executor.
      initOutputEventMap.put(destVertexName, initOutputEvents);
      outputMap.put(destVertexName, outputs.get(i));
    }

    // Initialize processor.
    initializeLogicalIOProcessor();
  }

  public Map<String, List<Event>> getInputInitEvents() {
    Preconditions.checkState(this.state != State.NEW, "Not initialized yet");
    return initInputEventMap;
  }

  public Map<String, List<Event>> getOutputInitEvents() {
    Preconditions.checkState(this.state != State.NEW, "Not initialized yet");
    return initOutputEventMap;
  }

  public void run() throws IOException {
    Preconditions.checkState(this.state == State.INITED,
        "Can only run while in INITED state. Current: " + this.state);
    this.state = State.RUNNING;
    LogicalIOProcessor lioProcessor = (LogicalIOProcessor) processor;
    lioProcessor.run(inputMap, outputMap);
  }

  public void close() throws IOException {
    Preconditions.checkState(this.state == State.RUNNING,
        "Can only run while in RUNNING state. Current: " + this.state);
    this.state=State.CLOSED;
    closeInputEventMap = new LinkedHashMap<String, List<Event>>(inputs.size());
    closeOutputEventMap = new LinkedHashMap<String, List<Event>>(outputs.size());

    // Close the Inputs.
    for (int i = 0; i < inputs.size(); i++) {
      String srcVertexName = inputSpecs.get(i).getSourceVertexName();
      List<Event> closeInputEvents = inputs.get(i).close();
      closeInputEventMap.put(srcVertexName, closeInputEvents);
    }

    // Close the Processor.
    processor.close();

    // Close the Outputs.
    for (int i = 0; i < outputs.size(); i++) {
      String destVertexName = outputSpecs.get(i).getDestinationVertexName();
      List<Event> closeOutputEvents = outputs.get(i).close();
      closeOutputEventMap.put(destVertexName, closeOutputEvents);
    }
  }

  public Map<String, List<Event>> getInputCloseEvents() {
    Preconditions.checkState(this.state == State.CLOSED, "Not closed yet");
    return closeInputEventMap;
  }

  public Map<String, List<Event>> getOutputCloseEvents() {
    Preconditions.checkState(this.state == State.CLOSED, "Not closed yet");
    return closeOutputEventMap;
  }

  private List<Event> initializeInput(Input input, InputSpec inputSpec)
      throws IOException {
    TezInputContext tezInputContext = createInputContext(inputSpec);
    if (input instanceof LogicalInput) {
      ((LogicalInput) input).setNumPhysicalInputs(inputSpec
          .getPhysicalEdgeCount());
    }
    return input.initialize(tezInputContext);
  }

  private List<Event> initializeOutput(Output output, OutputSpec outputSpec)
      throws IOException {
    TezOutputContext tezOutputContext = createOutputContext(outputSpec);
    if (output instanceof LogicalOutput) {
      ((LogicalOutput) output).setNumPhysicalOutputs(outputSpec
          .getPhysicalEdgeCount());
    }
    return output.initialize(tezOutputContext);
  }

  private void initializeLogicalIOProcessor() throws IOException {
    TezProcessorContext processorContext = createProcessorContext();
    processor.initialize(processorContext);
  }

  private TezInputContext createInputContext(InputSpec inputSpec) {
    TezInputContext inputContext = new TezInputContextImpl(tezConf,
        taskSpec.getVertexName(), inputSpec.getSourceVertexName(),
        taskSpec.getTaskAttemptID(), tezCounters,
        inputSpec.getInputDescriptor().getUserPayload());
    return inputContext;
  }

  private TezOutputContext createOutputContext(OutputSpec outputSpec) {
    TezOutputContext outputContext = new TezOutputContextImpl(tezConf,
        taskSpec.getVertexName(), outputSpec.getDestinationVertexName(),
        taskSpec.getTaskAttemptID(), tezCounters,
        outputSpec.getOutputDescriptor().getUserPayload());
    return outputContext;
  }

  private TezProcessorContext createProcessorContext() {
    TezProcessorContext processorContext = new TezProcessorContextImpl(tezConf,
        taskSpec.getVertexName(), taskSpec.getTaskAttemptID(), tezCounters,
        processorDescriptor.getUserPayload());
    return processorContext;
  }

  private List<LogicalInput> createInputs(List<InputSpec> inputSpecs) {
    List<LogicalInput> inputs = new ArrayList<LogicalInput>(inputSpecs.size());
    for (InputSpec inputSpec : inputSpecs) {
      Input input = RuntimeUtils.createClazzInstance(inputSpec
          .getInputDescriptor().getClassName());

      if (input instanceof LogicalInput) {
        inputs.add((LogicalInput) input);
      } else {
        throw new TezUncheckedException(
            input.getClass().getName()
                + " is not a sub-type of LogicalInput. Only LogicalInput sub-types supported by a LogicalIOProcessor.");
      }

    }
    return inputs;
  }

  private List<LogicalOutput> createOutputs(List<OutputSpec> outputSpecs) {
    List<LogicalOutput> outputs = new ArrayList<LogicalOutput>(
        outputSpecs.size());
    for (OutputSpec outputSpec : outputSpecs) {
      Output output = RuntimeUtils.createClazzInstance(outputSpec
          .getOutputDescriptor().getClassName());
      if (output instanceof LogicalOutput) {
        outputs.add((LogicalOutput) output);
      } else {
        throw new TezUncheckedException(
            output.getClass().getName()
                + " is not a sub-type of LogicalOutput. Only LogicalOutput sub-types supported by a LogicalIOProcessor.");
      }
    }
    return outputs;
  }

  private LogicalIOProcessor createProcessor(
      ProcessorDescriptor processorDescriptor) {
    Processor processor = RuntimeUtils.createClazzInstance(processorDescriptor
        .getClassName());
    if (!(processor instanceof LogicalIOProcessor)) {
      throw new TezUncheckedException(
          processor.getClass().getName()
              + " is not a sub-type of LogicalIOProcessor. Only LogicalIOProcessor sub-types supported at the moment");
    }
    return (LogicalIOProcessor) processor;
  }
}
