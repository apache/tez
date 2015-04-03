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

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtils;
import org.apache.tez.dag.api.InputDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.UserPayload;
import org.apache.tez.runtime.api.AbstractLogicalInput;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.Reader;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputFailedEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * LogicalInput used to writing tests for Tez. Supports fault
 * injection based on configuration. All the failure injection happens when the
 * processor calls doRead() on the input. Thus doRead() blocks until all the
 * failure injection is completed. The input waits for all the incoming data to
 * be ready by waiting for DataMovement events. Then it checks if any of those
 * need to be failed. It fails them by sending InputReadError events. Then it
 * either exits or waits for the data to be re-generated. When the data is
 * re-generated, it goes through the above cycle again. When no more failures
 * are to be injected, then it completes. All configuration items are post-fixed
 * by the name of the vertex that executes this input.
 */
public class TestInput extends AbstractLogicalInput {
  private static final Logger LOG = LoggerFactory
      .getLogger(TestInput.class);
  
  public static final String COUNTER_NAME = "TestInput";

  Configuration conf;
  int numCompletedInputs = 0;
  int[] completedInputVersion;
  AtomicInteger inputReady = new AtomicInteger(-1);
  int lastInputReadyValue = -1;
  int failingInputUpto = 0;
  
  boolean doFail = false;
  boolean doRandomFail = false;
  float randomFailProbability = 0.0f;
  boolean doFailAndExit = false;
  Set<Integer> failingTaskIndices = Sets.newHashSet();
  Set<Integer> failingTaskAttempts = Sets.newHashSet();
  Set<Integer> failingInputIndices = Sets.newHashSet();
  Integer failAll = new Integer(-1);
  int[] inputValues;
  
  /**
   * Enable failure for this logical input
   */
  public static String TEZ_FAILING_INPUT_DO_FAIL =
      "tez.failing-input.do-fail";
  /**
   * Enable failure for this logical input. The config is set per DAG.
   */
  public static String TEZ_FAILING_INPUT_DO_RANDOM_FAIL =
      "tez.failing-input.do-random-fail";
  /**
   * Probability to random fail an input. Range is 0 to 1. The number is set per DAG.
   */
  public static String TEZ_FAILING_INPUT_RANDOM_FAIL_PROBABILITY =
      "tez.failing-input.random-fail-probability";
  /**
   * Logical input will exit (and cause task failure) after reporting failure to 
   * read.
   */
  public static String TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT =
      "tez.failing-input.do-fail-and-exit";
  /**
   * Which physical inputs to fail. This is a comma separated list of +ve integers.
   * -1 means fail all.
   */
  public static String TEZ_FAILING_INPUT_FAILING_INPUT_INDEX =
      "tez.failing-input.failing-input-index";
  /**
   * Up to which version of the above physical inputs to fail. 0 will fail the 
   * first version. 1 will fail the first and second versions. And so on.
   */
  public static String TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT =
      "tez.failing-input.failing-upto-input-attempt";
  /**
   * Indices of the tasks in the first for which this input will fail. Comma 
   * separated list of +ve integers. -1 means all tasks. E.g. 0 means the first
   * task in the vertex will have failing inputs.
   */
  public static String TEZ_FAILING_INPUT_FAILING_TASK_INDEX =
      "tez.failing-input.failing-task-index";
  /**
   * Which task attempts will fail the input. This is a comma separated list of
   * +ve integers. -1 means all will fail. E.g. specifying 1 means the first
   * attempt will not fail the input but a re-run (the second attempt) will
   * trigger input failure. So this can be used to simulate cascading failures.
   */
  public static String TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT =
      "tez.failing-input.failing-task-attempt";

  public TestInput(InputContext inputContext, int numPhysicalInputs) {
    super(inputContext, numPhysicalInputs);
    this.completedInputVersion = new int[numPhysicalInputs];
    this.inputValues = new int[numPhysicalInputs];
    for (int i=0; i<numPhysicalInputs; ++i) {
      this.completedInputVersion[i] = -1;
      this.inputValues[i] = -1;
    }
  }

  public static InputDescriptor getInputDesc(UserPayload payload) {
    InputDescriptor desc = InputDescriptor.create(TestInput.class.getName());
    if (payload != null) {
      desc.setUserPayload(payload);
    }
    return desc;
  }

  public int doRead() {
    boolean done = true;
    do {
      done = true;
      synchronized (inputReady) {
        while (inputReady.get() <= lastInputReadyValue) {
          try {
            LOG.info("Waiting for inputReady: " + inputReady.get() + 
                " last: " + lastInputReadyValue);
            inputReady.wait();
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
        lastInputReadyValue = inputReady.get();
        LOG.info("Done for inputReady: " + lastInputReadyValue);
      }
      if (!doRandomFail) {
        // not random fail
        if (doFail) {
          if (
              (failingTaskIndices.contains(failAll) ||
              failingTaskIndices.contains(getContext().getTaskIndex())) &&
              (failingTaskAttempts.contains(failAll) || 
               failingTaskAttempts.contains(getContext().getTaskAttemptNumber())) &&
               (lastInputReadyValue <= failingInputUpto)) {
            List<Event> events = Lists.newLinkedList();
            if (failingInputIndices.contains(failAll)) {
              for (int i=0; i<getNumPhysicalInputs(); ++i) {
                String msg = ("FailingInput: " + getContext().getUniqueIdentifier() + 
                    " index: " + i + " version: " + lastInputReadyValue);
                events.add(InputReadErrorEvent.create(msg, i, lastInputReadyValue));
                LOG.info("Failing input: " + msg);
              }
            } else {
              for (Integer index : failingInputIndices) {
                if (index.intValue() >= getNumPhysicalInputs()) {
                  throwException("InputIndex: " + index.intValue() + 
                      " should be less than numInputs: " + getNumPhysicalInputs());
                }
                if (completedInputVersion[index.intValue()] < lastInputReadyValue) {
                  continue; // dont fail a previous version now.
                }
                String msg = ("FailingInput: " + getContext().getUniqueIdentifier() + 
                    " index: " + index.intValue() + " version: " + lastInputReadyValue);
                events.add(InputReadErrorEvent.create(msg, index.intValue(), lastInputReadyValue));
                LOG.info("Failing input: " + msg);
              }
            }
            getContext().sendEvents(events);
            if (doFailAndExit) {
              String msg = "FailingInput exiting: " + getContext().getUniqueIdentifier();
              LOG.info(msg);
              throwException(msg);
            } else {
              done = false;
            }
          } else if ((failingTaskIndices.contains(failAll) ||
              failingTaskIndices.contains(getContext().getTaskIndex()))){
            boolean previousAttemptReadFailed = false;
            if (failingTaskAttempts.contains(failAll)) {
              previousAttemptReadFailed = true;
            } else {
              for (int i=0 ; i<getContext().getTaskAttemptNumber(); ++i) {
                if (failingTaskAttempts.contains(new Integer(i))) {
                  previousAttemptReadFailed = true;
                  break;
                }
              }
            }
            if (previousAttemptReadFailed && 
                (lastInputReadyValue <= failingInputUpto)) {
              // if any previous attempt has failed then dont be done when we see
              // a previously failed input
              LOG.info("Previous task attempt failed and input version less than failing upto version");
              done = false;
            }
          }
          
        }
      } else {
        // random fail
        List<Event> events = Lists.newLinkedList();
        for (int index=0; index<getNumPhysicalInputs(); ++index) {
          // completedInputVersion[index] has DataMovementEvent.getVersion() value.
          int sourceInputVersion = completedInputVersion[index];
          int maxFailedAttempt = conf.getInt(TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS, 
              TezConfiguration.TEZ_AM_TASK_MAX_FAILED_ATTEMPTS_DEFAULT);
          if (sourceInputVersion < maxFailedAttempt - 1) {
            float rollNumber = (float) Math.random();
            String msg = "FailingInput random fail turned on." +
                "Do a roll:" + getContext().getUniqueIdentifier() + 
                " index: " + index + " version: " + sourceInputVersion +
                " rollNumber: " + rollNumber + 
                " randomFailProbability " + randomFailProbability;
            LOG.info(msg);
            if (rollNumber < randomFailProbability) {
              // fail the source input
              msg = "FailingInput: rollNumber < randomFailProbability. Do fail." + 
                            getContext().getUniqueIdentifier() + 
                            " index: " + index + " version: " + sourceInputVersion;
              LOG.info(msg);
              events.add(InputReadErrorEvent.create(msg, index, sourceInputVersion));
            }
          }
        }
        getContext().sendEvents(events);
      }
    } while (!done);
    
    // sum input value given by upstream tasks
    int sum = 0;
    for (int i=0; i<getNumPhysicalInputs(); ++i) {
      if (inputValues[i] == -1) {
        throwException("Invalid input value : " + i);
      }
      sum += inputValues[i];
    }
    // return sum value
    return sum;
  }
  
  void throwException(String msg) {
    RuntimeException e = new RuntimeException(msg);
    getContext().fatalError(e , msg);
    throw e;
  }
  
  public static String getVertexConfName(String confName, String vertexName) {
    return confName + "." + vertexName;
  }

  @Override
  public List<Event> initialize() throws Exception {
    getContext().requestInitialMemory(0l, null); //Mandatory call.
    getContext().inputIsReady();
    if (getContext().getUserPayload() != null && getContext().getUserPayload().hasPayload()) {
      String vName = getContext().getTaskVertexName();
      conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
      doFail = conf.getBoolean(getVertexConfName(TEZ_FAILING_INPUT_DO_FAIL, vName), false);
      doFailAndExit = conf.getBoolean(
          getVertexConfName(TEZ_FAILING_INPUT_DO_FAIL_AND_EXIT, vName), false);
      LOG.info("doFail: " + doFail + " doFailAndExit: " + doFailAndExit);
      if (doFail) {
        for (String failingIndex : 
          conf.getTrimmedStringCollection(
              getVertexConfName(TEZ_FAILING_INPUT_FAILING_TASK_INDEX, vName))) {
          LOG.info("Adding failing task index: " + failingIndex);
          failingTaskIndices.add(Integer.valueOf(failingIndex));
        }
        for (String failingIndex : 
          conf.getTrimmedStringCollection(
              getVertexConfName(TEZ_FAILING_INPUT_FAILING_TASK_ATTEMPT, vName))) {
          LOG.info("Adding failing task attempt: " + failingIndex);
          failingTaskAttempts.add(Integer.valueOf(failingIndex));
        }
        failingInputUpto = conf.getInt(
            getVertexConfName(TEZ_FAILING_INPUT_FAILING_UPTO_INPUT_ATTEMPT, vName), 0);
        LOG.info("Adding failing input upto: " + failingInputUpto);
        for (String failingIndex : 
          conf.getTrimmedStringCollection(
              getVertexConfName(TEZ_FAILING_INPUT_FAILING_INPUT_INDEX, vName))) {
          LOG.info("Adding failing input index: " + failingIndex);
          failingInputIndices.add(Integer.valueOf(failingIndex));
        }
      }
      doRandomFail = conf
          .getBoolean(TEZ_FAILING_INPUT_DO_RANDOM_FAIL, false);
      randomFailProbability = conf.getFloat(TEZ_FAILING_INPUT_RANDOM_FAIL_PROBABILITY, 0.0f);
      LOG.info("doRandomFail: " + doRandomFail);
      LOG.info("randomFailProbability: " + randomFailProbability);
    }
    return Collections.emptyList();
  }

  @Override
  public void start() {
  }

  @Override
  public Reader getReader() throws Exception {
    return null;
  }

  @Override
  public void handleEvents(List<Event> inputEvents) throws Exception {
    for (Event event : inputEvents) {
      if (event instanceof DataMovementEvent) {
        DataMovementEvent dmEvent = (DataMovementEvent) event;
        numCompletedInputs++;
        LOG.info(getContext().getSourceVertexName() + " Received DataMovement event sourceId : " + dmEvent.getSourceIndex() + 
            " targetId: " + dmEvent.getTargetIndex() +
            " version: " + dmEvent.getVersion() +
            " numInputs: " + getNumPhysicalInputs() +
            " numCompletedInputs: " + numCompletedInputs);
        this.completedInputVersion[dmEvent.getTargetIndex()] = dmEvent.getVersion();
        this.inputValues[dmEvent.getTargetIndex()] = 
            dmEvent.getUserPayload().getInt();
      } else if (event instanceof InputFailedEvent) {
        InputFailedEvent ifEvent = (InputFailedEvent) event;
        numCompletedInputs--;
        LOG.info("Received InputFailed event targetId: " + ifEvent.getTargetIndex() +
            " version: " + ifEvent.getVersion() +
            " numInputs: " + getNumPhysicalInputs() +
            " numCompletedInputs: " + numCompletedInputs);
      }
    }
    if (numCompletedInputs == getNumPhysicalInputs()) {
      int maxInputVersionSeen = -1;  
      for (int i=0; i<getNumPhysicalInputs(); ++i) {
        if (completedInputVersion[i] < 0) {
          LOG.info("Not received completion for input " + i);
          return;
        } else if (maxInputVersionSeen < completedInputVersion[i]) {
          maxInputVersionSeen = completedInputVersion[i];
        }
      }
      LOG.info("Received all inputs");
      synchronized (inputReady) {
        inputReady.set(maxInputVersionSeen);
        LOG.info("Notifying done with " + maxInputVersionSeen);
        inputReady.notifyAll();
      }
    }
  }

  @Override
  public List<Event> close() throws Exception {
    getContext().getCounters().findCounter(COUNTER_NAME, COUNTER_NAME).increment(1);;
    return null;
  }

}
