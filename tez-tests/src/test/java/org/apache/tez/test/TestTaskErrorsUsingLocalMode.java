/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
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

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.tez.client.TezClient;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.ProcessorDescriptor;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezException;
import org.apache.tez.dag.api.Vertex;
import org.apache.tez.dag.api.client.DAGClient;
import org.apache.tez.dag.api.client.DAGStatus;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.TaskFailureType;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestTaskErrorsUsingLocalMode {

  private static final Logger LOG = LoggerFactory.getLogger(TestTaskErrorsUsingLocalMode.class);

  private static final String VERTEX_NAME = "vertex1";


  @Test(timeout = 20000)
  public void testFatalErrorReported() throws IOException, TezException, InterruptedException {

    TezClient tezClient = getTezClient("testFatalErrorReported");
    DAGClient dagClient = null;

    try {
      FailingProcessor.configureForFatalFail();
      DAG dag = DAG.create("testFatalErrorReportedDag").addVertex(
          Vertex
              .create(VERTEX_NAME, ProcessorDescriptor.create(FailingProcessor.class.getName()), 1));

      dagClient = tezClient.submitDAG(dag);
      dagClient.waitForCompletion();
      assertEquals(DAGStatus.State.FAILED, dagClient.getDAGStatus(null).getState());
      assertEquals(1, dagClient.getVertexStatus(VERTEX_NAME, null).getProgress().getFailedTaskAttemptCount());
    } finally {
      if (dagClient != null) {
        dagClient.close();
      }
      tezClient.stop();
    }
  }

  @Test(timeout = 20000)
  public void testNonFatalErrorReported() throws IOException, TezException, InterruptedException {

    TezClient tezClient = getTezClient("testNonFatalErrorReported");
    DAGClient dagClient = null;

    try {
      FailingProcessor.configureForNonFatalFail();
      DAG dag = DAG.create("testNonFatalErrorReported").addVertex(
          Vertex
              .create(VERTEX_NAME, ProcessorDescriptor.create(FailingProcessor.class.getName()), 1));

      dagClient = tezClient.submitDAG(dag);
      dagClient.waitForCompletion();
      assertEquals(DAGStatus.State.FAILED, dagClient.getDAGStatus(null).getState());
      assertEquals(4, dagClient.getVertexStatus(VERTEX_NAME, null).getProgress().getFailedTaskAttemptCount());
    } finally {
      if (dagClient != null) {
        dagClient.close();
      }
      tezClient.stop();
    }
  }

  @Test(timeout = 20000)
  public void testSelfKillReported() throws IOException, TezException, InterruptedException {

    TezClient tezClient = getTezClient("testSelfKillReported");
    DAGClient dagClient = null;

    try {
      FailingProcessor.configureForKilled(10);
      DAG dag = DAG.create("testSelfKillReported").addVertex(
          Vertex
              .create(VERTEX_NAME, ProcessorDescriptor.create(FailingProcessor.class.getName()), 1));

      dagClient = tezClient.submitDAG(dag);
      dagClient.waitForCompletion();
      assertEquals(DAGStatus.State.SUCCEEDED, dagClient.getDAGStatus(null).getState());
      assertEquals(10, dagClient.getVertexStatus(VERTEX_NAME, null).getProgress().getKilledTaskAttemptCount());
    } finally {
      if (dagClient != null) {
        dagClient.close();
      }
      tezClient.stop();
    }
  }


  private TezClient getTezClient(String name) throws IOException, TezException {
    TezConfiguration tezConf1 = new TezConfiguration();
    tezConf1.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    tezConf1.set("fs.defaultFS", "file:///");
    tezConf1.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    tezConf1.setLong(TezConfiguration.TEZ_AM_SLEEP_TIME_BEFORE_EXIT_MILLIS, 500);
    TezClient tezClient1 = TezClient.create(name, tezConf1, true);
    tezClient1.start();
    return tezClient1;
  }


  public static class FailingProcessor extends AbstractLogicalIOProcessor {

    private static final String FAIL_STRING_NON_FATAL = "non-fatal-fail";
    private static final String FAIL_STRING_FATAL = "fatal-fail";
    private static final String KILL_STRING = "kill-self";

    private static volatile boolean shouldFail;
    private static volatile boolean fatalError;

    private static volatile boolean shouldKill;
    private static volatile int killModeAttemptNumberToSucceed;


    static {
      reset();
    }

    static void reset() {
      shouldFail = false;
      fatalError = false;

      shouldKill = false;
      killModeAttemptNumberToSucceed = -1;
    }

    static void configureForNonFatalFail() {
      reset();
      shouldFail = true;
    }

    static void configureForFatalFail() {
      reset();
      shouldFail = true;
      fatalError = true;
    }

    static void configureForKilled(int attemptNumber) {
      reset();
      shouldKill = true;
      killModeAttemptNumberToSucceed = attemptNumber;
    }

    public FailingProcessor(ProcessorContext context) {
      super(context);
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void handleEvents(List<Event> processorEvents) {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws
        Exception {
      LOG.info("Running Failing processor");
      if (shouldFail) {
        if (fatalError) {
          LOG.info("Reporting fatal error");
          getContext().reportFailure(TaskFailureType.FATAL, null, FAIL_STRING_FATAL);
        } else {
          LOG.info("Reporting non-fatal error");
          getContext().reportFailure(TaskFailureType.NON_FATAL, null, FAIL_STRING_NON_FATAL);
        }
      } else if (shouldKill) {
        if (getContext().getTaskAttemptNumber() != killModeAttemptNumberToSucceed) {
          LOG.info("Reporting self-kill for attempt=" + getContext().getTaskAttemptNumber());
          getContext().killSelf(null, KILL_STRING);
        }
      }
    }
  }

}
