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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;

import com.google.common.collect.Sets;

/**
 * LogicalIOProcessor used to write tests. Supports fault injection through
 * configuration. The configuration is post-fixed by the name of the vertex for
 * this processor. The fault injection executes in the run() method. The
 * processor first sleeps for a specified interval. Then checks if it needs to
 * fail. It fails and exits if configured to do so. If not, then it calls
 * doRead() on all inputs to let them fail.
 */
public class TestProcessor extends AbstractLogicalIOProcessor {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestProcessor.class);
  
  Configuration conf;
  
  boolean doFail = false;
  boolean doRandomFail = false;
  float randomFailProbability = 0.0f;
  long sleepMs;
  Set<Integer> failingTaskIndices = Sets.newHashSet();
  int failingTaskAttemptUpto = 0;
  Integer failAll = new Integer(-1);
  
  int verifyValue = -1;
  Set<Integer> verifyTaskIndices = Sets.newHashSet();
  
  /**
   * Enable failure for this processor
   */
  public static String TEZ_FAILING_PROCESSOR_DO_FAIL =
      "tez.failing-processor.do-fail";
  /**
   * Enable random failure for all processors.
   */
  public static String TEZ_FAILING_PROCESSOR_DO_RANDOM_FAIL = "tez.failing-processor.do-random-fail";
  /**
   * Probability to random fail a task attempt. Range is 0 to 1. The number is set per DAG.
   */
  public static String TEZ_FAILING_PROCESSOR_RANDOM_FAIL_PROBABILITY = "tez.failing-processor.random-fail-probability";
  
  /**
   * Time to sleep in the processor in milliseconds.
   */
  public static String TEZ_FAILING_PROCESSOR_SLEEP_MS =
      "tez.failing-processor.sleep-ms";
  /**
   * The indices of tasks in the vertex for which the processor will fail. This 
   * is a comma-separated list of +ve integeres. -1 means all fail.
   */
  public static String TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX =
      "tez.failing-processor.failing-task-index";
  /**
   * Up to which attempt of the tasks will fail. Specifying 0 means the first
   * attempt will fail. 1 means first and second attempt will fail. And so on.
   */
  public static String TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT =
      "tez.failing-processor.failing-upto-task-attempt";
  
  public static String TEZ_FAILING_PROCESSOR_VERIFY_VALUE = 
      "tez.failing-processor.verify-value";
  
  public static String TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX =
      "tez.failing-processor.verify-task-index";

  /**
   * Constructor an instance of the LogicalProcessor. Classes extending this one to create a
   * LogicalProcessor, must provide the same constructor so that Tez can create an instance of the
   * class at runtime.
   *
   * @param context the {@link org.apache.tez.runtime.api.ProcessorContext} which provides
   *                the Processor with context information within the running task.
   */
  public TestProcessor(ProcessorContext context) {
    super(context);
  }

  public static ProcessorDescriptor getProcDesc(UserPayload payload) {
    return ProcessorDescriptor.create(TestProcessor.class.getName()).setUserPayload(
        payload == null ? UserPayload.create(null) : payload);
  }

  void throwException(String msg) {
    RuntimeException e = new RuntimeException(msg);
    getContext().fatalError(e , msg);
    throw e;
  }

  public static String getVertexConfName(String confName, String vertexName) {
    return confName + "." + vertexName;
  }
  
  public static String getVertexConfName(String confName, String vertexName,
      int taskIndex) {
    return confName + "." + vertexName + "." + String.valueOf(taskIndex);
  }
  
  @Override
  public void initialize() throws Exception {
    if (getContext().getUserPayload() != null && getContext().getUserPayload().hasPayload()) {
      String vName = getContext().getTaskVertexName();
      conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
      verifyValue = conf.getInt(
          getVertexConfName(TEZ_FAILING_PROCESSOR_VERIFY_VALUE, vName,
              getContext().getTaskIndex()), -1);
      if (verifyValue != -1) {
        LOG.info("Verify value: " + verifyValue);
        for (String verifyIndex : conf
            .getTrimmedStringCollection(
                getVertexConfName(TEZ_FAILING_PROCESSOR_VERIFY_TASK_INDEX, vName))) {
          LOG.info("Adding verify task index: " + verifyIndex);
          verifyTaskIndices.add(Integer.valueOf(verifyIndex));
        }
      }
      doFail = conf.getBoolean(
          getVertexConfName(TEZ_FAILING_PROCESSOR_DO_FAIL, vName), false);
      sleepMs = conf.getLong(
          getVertexConfName(TEZ_FAILING_PROCESSOR_SLEEP_MS, vName), 0);
      LOG.info("doFail: " + doFail);
      if (doFail) {
        for (String failingIndex : conf
            .getTrimmedStringCollection(
                getVertexConfName(TEZ_FAILING_PROCESSOR_FAILING_TASK_INDEX, vName))) {
          LOG.info("Adding failing task index: " + failingIndex);
          failingTaskIndices.add(Integer.valueOf(failingIndex));
        }
        failingTaskAttemptUpto = conf.getInt(
            getVertexConfName(TEZ_FAILING_PROCESSOR_FAILING_UPTO_TASK_ATTEMPT, vName), 0);
        LOG.info("Adding failing attempt : " + failingTaskAttemptUpto + 
            " dag: " + getContext().getDAGName());
      }
      
      doRandomFail = conf
          .getBoolean(TEZ_FAILING_PROCESSOR_DO_RANDOM_FAIL, false);
      randomFailProbability = conf.getFloat(TEZ_FAILING_PROCESSOR_RANDOM_FAIL_PROBABILITY, 0.0f);
      LOG.info("doRandomFail: " + doRandomFail);
      LOG.info("randomFailProbability: " + randomFailProbability);
    }
  }

  @Override
  public void handleEvents(List<Event> processorEvents) {
  }

  @Override
  public void close() throws Exception {
  }

  @Override
  public void run(Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs) throws Exception {
    LOG.info("Sleeping ms: " + sleepMs);

    for (LogicalInput input : inputs.values()) {
      input.start();
    }
    for (LogicalOutput output : outputs.values()) {
      output.start();
    }

    Thread.sleep(sleepMs);
    
    if (!doRandomFail) {
      // not random fail
      if (doFail) {
        if (
            (failingTaskIndices.contains(failAll) ||
            failingTaskIndices.contains(getContext().getTaskIndex())) &&
            (failingTaskAttemptUpto == failAll.intValue() || 
             failingTaskAttemptUpto >= getContext().getTaskAttemptNumber())) {
          String msg = "FailingProcessor: " + getContext().getUniqueIdentifier() + 
              " dag: " + getContext().getDAGName() +
              " taskIndex: " + getContext().getTaskIndex() +
              " taskAttempt: " + getContext().getTaskAttemptNumber();
          LOG.info(msg);
          throwException(msg);
        }
      }
    } else {
      // random fail
      // If task attempt number is below limit, try to randomly fail the attempt.
      int taskAttemptNumber = getContext().getTaskAttemptNumber();
      int maxFailedAttempt = conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 
                                     TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
      if (taskAttemptNumber < maxFailedAttempt - 1) {
        float rollNumber = (float) Math.random();
        String msg = "FailingProcessor random fail turned on." + 
            " Do a roll: " + getContext().getUniqueIdentifier() + 
            " dag: " + getContext().getDAGName() +
            " taskIndex: " + getContext().getTaskIndex() +
            " taskAttempt: " + taskAttemptNumber +
            " maxFailedAttempt: " + maxFailedAttempt +
            " rollNumber: " + rollNumber + 
            " randomFailProbability " + randomFailProbability;
        LOG.info(msg);
        if (rollNumber < randomFailProbability) {
          // fail the attempt
          msg = "FailingProcessor: rollNumber < randomFailProbability. Do fail.";
          LOG.info(msg);
          throwException(msg);
        }
      }
    }
      
    if (inputs.entrySet().size() > 0) {
        String msg = "Reading input of current FailingProcessor: " + getContext().getUniqueIdentifier() + 
            " dag: " + getContext().getDAGName() +
            " vertex: " + getContext().getTaskVertexName() +
            " taskIndex: " + getContext().getTaskIndex() +
            " taskAttempt: " + getContext().getTaskAttemptNumber();
        LOG.info(msg);
    }
    //initialize sum to attempt number + 1
    int sum = getContext().getTaskAttemptNumber() + 1;
    LOG.info("initializing vertex= " + getContext().getTaskVertexName() +
             " taskIndex: " + getContext().getTaskIndex() +
             " taskAttempt: " + getContext().getTaskAttemptNumber() +
             " sum= " + sum);
    //sum = summation of input values
    for (Map.Entry<String, LogicalInput> entry : inputs.entrySet()) {
      if (!(entry.getValue() instanceof TestInput)) {
        LOG.info("Ignoring non TestInput: " + entry.getKey()
            + " inputClass= " + entry.getValue().getClass().getSimpleName());
        continue;
      }
      TestInput input = (TestInput) entry.getValue();
      int inputValue = input.doRead();
      LOG.info("Reading input: " + entry.getKey() + " inputValue= " + inputValue);
      sum += inputValue;
    }
    
    if (outputs.entrySet().size() > 0) {
        String msg = "Writing output of current FailingProcessor: " + getContext().getUniqueIdentifier() + 
            " dag: " + getContext().getDAGName() +
            " vertex: " + getContext().getTaskVertexName() +
            " taskIndex: " + getContext().getTaskIndex() +
            " taskAttempt: " + getContext().getTaskAttemptNumber();
        LOG.info(msg);
    }
    for (Map.Entry<String, LogicalOutput> entry : outputs.entrySet()) {
      if (!(entry.getValue() instanceof TestOutput)) {
        LOG.info("Ignoring non TestOutput: " + entry.getKey()
            + " outputClass= " + entry.getValue().getClass().getSimpleName());
        continue;
      }
      LOG.info("Writing output: " + entry.getKey() + " sum= " + sum);
      TestOutput output = (TestOutput) entry.getValue();
      output.write(sum);
    }
    
    LOG.info("Output for DAG: " + getContext().getDAGName() 
        + " vertex: " + getContext().getTaskVertexName()
        + " task: " + getContext().getTaskIndex()
        + " attempt: " + getContext().getTaskAttemptNumber()
        + " is: " + sum);
    if (verifyTaskIndices
        .contains(new Integer(getContext().getTaskIndex()))) {
      if (verifyValue != -1 && verifyValue != sum) {
        // expected output value set and not equal to observed value
        String msg = "Expected output mismatch of current FailingProcessor: " 
                     + getContext().getUniqueIdentifier() + 
                     " dag: " + getContext().getDAGName() +
                     " vertex: " + getContext().getTaskVertexName() +
                     " taskIndex: " + getContext().getTaskIndex() +
                     " taskAttempt: " + getContext().getTaskAttemptNumber();
        msg += "\n" + "Expected output: " + verifyValue + " got: " + sum;
        throwException(msg);
      } else {
        LOG.info("Verified output for DAG: " + getContext().getDAGName()
            + " vertex: " + getContext().getTaskVertexName() + " task: "
            + getContext().getTaskIndex() + " attempt: "
            + getContext().getTaskAttemptNumber() + " is: " + sum);
      }
    }
  }

}
