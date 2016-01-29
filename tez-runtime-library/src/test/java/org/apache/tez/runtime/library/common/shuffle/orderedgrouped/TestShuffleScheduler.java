/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.TezCommonUtils;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.impl.ExecutionContextImpl;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestShuffleScheduler {


  @Test (timeout = 10000)
  public void testNumParallelScheduledFetchers() throws IOException, InterruptedException {
    InputContext inputContext = createTezInputContext();
    Configuration conf = new TezConfiguration();
    // Allow 10 parallel copies at once.
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_PARALLEL_COPIES, 10);
    int numInputs = 50;
    Shuffle shuffle = mock(Shuffle.class);
    MergeManager mergeManager = mock(MergeManager.class);

    final ShuffleSchedulerForTest scheduler =
        new ShuffleSchedulerForTest(inputContext, conf, numInputs, shuffle, mergeManager,
            mergeManager,
            System.currentTimeMillis(), null, false, 0, "srcName", true);

    Future<Void> executorFuture = null;
    ExecutorService executor = Executors.newFixedThreadPool(1);
    try {
      executorFuture = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          scheduler.start();
          return null;
        }
      });

      InputAttemptIdentifier[] identifiers = new InputAttemptIdentifier[numInputs];

      // Schedule all copies.
      for (int i = 0; i < numInputs; i++) {
        InputAttemptIdentifier inputAttemptIdentifier =
            new InputAttemptIdentifier(i, 0, "attempt_");
        scheduler.addKnownMapOutput("host" + i, 10000, 1, "hostUrl", inputAttemptIdentifier);
        identifiers[i] = inputAttemptIdentifier;
      }

      // Sleep for a bit to allow the copies to be scheduled.
      Thread.sleep(2000l);
      assertEquals(10, scheduler.numFetchersCreated.get());

    } finally {
      scheduler.close();
      if (executorFuture != null) {
        executorFuture.cancel(true);
      }
      executor.shutdownNow();
    }
  }

  @Test(timeout = 5000)
  public void testSimpleFlow() throws Exception {
    InputContext inputContext = createTezInputContext();
    Configuration conf = new TezConfiguration();
    int numInputs = 10;
    Shuffle shuffle = mock(Shuffle.class);
    MergeManager mergeManager = mock(MergeManager.class);

    final ShuffleSchedulerForTest scheduler =
        new ShuffleSchedulerForTest(inputContext, conf, numInputs, shuffle, mergeManager,
            mergeManager,
            System.currentTimeMillis(), null, false, 0, "srcName");

    ExecutorService executor = Executors.newFixedThreadPool(1);
    try {
      Future<Void> executorFuture = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          scheduler.start();
          return null;
        }
      });

      InputAttemptIdentifier[] identifiers = new InputAttemptIdentifier[numInputs];

      for (int i = 0; i < numInputs; i++) {
        InputAttemptIdentifier inputAttemptIdentifier =
            new InputAttemptIdentifier(i, 0, "attempt_");
        scheduler.addKnownMapOutput("host" + i, 10000, 1, "hostUrl", inputAttemptIdentifier);
        identifiers[i] = inputAttemptIdentifier;
      }

      MapHost[] mapHosts = new MapHost[numInputs];
      int count = 0;
      for (MapHost mh : scheduler.mapLocations.values()) {
        mapHosts[count++] = mh;
      }

      for (int i = 0; i < numInputs; i++) {
        MapOutput mapOutput = MapOutput
            .createMemoryMapOutput(identifiers[i], mock(FetchedInputAllocatorOrderedGrouped.class),
                100, false);
        scheduler.copySucceeded(identifiers[i], mapHosts[i], 20, 25, 100, mapOutput, false);
        scheduler.freeHost(mapHosts[i]);
      }
      
      verify(inputContext, atLeast(numInputs)).notifyProgress();

      // Ensure the executor exits, and without an error.
      executorFuture.get();
    } finally {
      scheduler.close();
      executor.shutdownNow();
    }
  }

  @Test(timeout = 60000)
  /**
   * Scenario
   *    - reducer has not progressed enough
   *    - reducer becomes unhealthy after some failures
   *    - no of attempts failing exceeds maxFailedUniqueFetches (5)
   * Expected result
   *    - fail the reducer
   */
  public void testReducerHealth_1() throws IOException {
    Configuration conf = new TezConfiguration();
    _testReducerHealth_1(conf);
    conf.setInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST, 4000);
    _testReducerHealth_1(conf);
  }

  public void _testReducerHealth_1(Configuration conf) throws IOException {
    long startTime = System.currentTimeMillis() - 500000;
    Shuffle shuffle = mock(Shuffle.class);
    final ShuffleSchedulerForTest scheduler = createScheduler(startTime, 320,
        shuffle, conf);

    int totalProducerNodes = 20;

    //Generate 320 events
    for (int i = 0; i < 320; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.addKnownMapOutput("host" + (i % totalProducerNodes),
          10000, i, "hostUrl", inputAttemptIdentifier);
    }

    //100 succeeds
    for (int i = 0; i < 100; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      MapOutput mapOutput = MapOutput
          .createMemoryMapOutput(inputAttemptIdentifier, mock(FetchedInputAllocatorOrderedGrouped.class),
              100, false);
      scheduler.copySucceeded(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), 100, 200, startTime + (i * 100), mapOutput, false);
    }

    //99 fails
    for (int i = 100; i < 199; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
    }


    InputAttemptIdentifier inputAttemptIdentifier =
        new InputAttemptIdentifier(200, 0, "attempt_");

    //Should fail here and report exception as reducer is not healthy
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(200, "host" + (200 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);

    int minFailurePerHost = conf.getInt(
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_MIN_FAILURES_PER_HOST_DEFAULT);

    if (minFailurePerHost <= 4) {
      //As per test threshold. Should fail & retrigger shuffle
      verify(shuffle, atLeast(0)).reportException(any(Throwable.class));
    } else if (minFailurePerHost > 100) {
      //host failure is so high that this would not retrigger shuffle re-execution
      verify(shuffle, atLeast(1)).reportException(any(Throwable.class));
    }
  }

  @Test(timeout = 60000)
  /**
   * Scenario
   *    - reducer has progressed enough
   *    - failures start happening after that
   *    - no of attempts failing exceeds maxFailedUniqueFetches (5)
   *    - Has not stalled
   * Expected result
   *    - Since reducer is not stalled, it should continue without error
   *
   * When reducer stalls, wait until enough retries are done and throw exception
   *
   */
  public void testReducerHealth_2() throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis() - 500000;
    Shuffle shuffle = mock(Shuffle.class);
    final ShuffleSchedulerForTest scheduler = createScheduler(startTime, 320, shuffle);

    int totalProducerNodes = 20;

    //Generate 0-200 events
    for (int i = 0; i < 200; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.addKnownMapOutput("host" + (i % totalProducerNodes),
          10000, i, "hostUrl", inputAttemptIdentifier);
    }
    assertEquals(320, scheduler.remainingMaps.get());

    //Generate 200-320 events with empty partitions
    for (int i = 200; i < 320; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.copySucceeded(inputAttemptIdentifier, null, 0, 0, 0, null, true);
    }
    //120 are successful. so remaining is 200
    assertEquals(200, scheduler.remainingMaps.get());


    //200 pending to be downloaded. Download 190.
    for (int i = 0; i < 190; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      MapOutput mapOutput = MapOutput
          .createMemoryMapOutput(inputAttemptIdentifier, mock(FetchedInputAllocatorOrderedGrouped.class),
              100, false);
      scheduler.copySucceeded(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), 100, 200, startTime + (i * 100), mapOutput, false);
    }

    assertEquals(10, scheduler.remainingMaps.get());

    //10 fails
    for (int i = 190; i < 200; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
    }

    //Shuffle has not stalled. so no issues.
    verify(scheduler.reporter, times(0)).reportException(any(Throwable.class));

    //stall shuffle
    scheduler.lastProgressTime = System.currentTimeMillis() - 250000;

    InputAttemptIdentifier inputAttemptIdentifier =
        new InputAttemptIdentifier(190, 0, "attempt_");
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(190, "host" +
        (190 % totalProducerNodes)
        + ":" + 10000, ""), false, true, false);

    //Even when it is stalled, need (320 - 300 = 20) * 3 = 60 failures
    verify(scheduler.reporter, times(0)).reportException(any(Throwable.class));

    assertEquals(11, scheduler.failedShufflesSinceLastCompletion);

    //fail to download 50 more times across attempts
    for (int i = 190; i < 200; i++) {
      inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
    }

    assertEquals(61, scheduler.failedShufflesSinceLastCompletion);
    assertEquals(10, scheduler.remainingMaps.get());

    verify(shuffle, atLeast(0)).reportException(any(Throwable.class));

    //fail another 30
    for (int i = 110; i < 120; i++) {
      inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
    }

    // Should fail now due to fetcherHealthy. (stall has already happened and
    // these are the only pending tasks)
    verify(shuffle, atLeast(1)).reportException(any(Throwable.class));
  }



  @Test(timeout = 60000)
  /**
   * Scenario
   *    - reducer has progressed enough
   *    - failures start happening after that in last fetch
   *    - no of attempts failing does not exceed maxFailedUniqueFetches (5)
   *    - Stalled
   * Expected result
   *    - Since reducer is stalled and if failures haven't happened across nodes,
   *    it should be fine to proceed. AM would restart source task eventually.
   *
   */
  public void testReducerHealth_3() throws IOException {
    long startTime = System.currentTimeMillis() - 500000;
    Shuffle shuffle = mock(Shuffle.class);
    final ShuffleSchedulerForTest scheduler = createScheduler(startTime, 320, shuffle);

    int totalProducerNodes = 20;

    //Generate 320 events
    for (int i = 0; i < 320; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.addKnownMapOutput("host" + (i % totalProducerNodes),
          10000, i, "hostUrl", inputAttemptIdentifier);
    }

    //319 succeeds
    for (int i = 0; i < 319; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      MapOutput mapOutput = MapOutput
          .createMemoryMapOutput(inputAttemptIdentifier, mock(FetchedInputAllocatorOrderedGrouped.class),
              100, false);
      scheduler.copySucceeded(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), 100, 200, startTime + (i * 100), mapOutput, false);
    }

    //1 fails (last fetch)
    InputAttemptIdentifier inputAttemptIdentifier =
        new InputAttemptIdentifier(319, 0, "attempt_");
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(319, "host" + (319 % totalProducerNodes)
        + ":" + 10000, ""), false, true, false);

    //stall the shuffle
    scheduler.lastProgressTime = System.currentTimeMillis() - 1000000;

    assertEquals(scheduler.remainingMaps.get(), 1);

    //Retry for 3 more times
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(319, "host" + (319 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(319, "host" + (319 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(319, "host" + (319 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);

    // failedShufflesSinceLastCompletion has crossed the limits. Throw error
    verify(shuffle, times(0)).reportException(any(Throwable.class));
  }

  @Test(timeout = 60000)
  /**
   * Scenario
   *    - reducer has progressed enough
   *    - failures have happened randomly in nodes, but tasks are completed
   *    - failures start happening after that in last fetch
   *    - no of attempts failing does not exceed maxFailedUniqueFetches (5)
   *    - Stalled
   * Expected result
   *    - reducer is stalled. But since errors are not seen across multiple
   *    nodes, it is left to the AM to retart producer. Do not kill consumer.
   *
   */
  public void testReducerHealth_4() throws IOException {
    long startTime = System.currentTimeMillis() - 500000;
    Shuffle shuffle = mock(Shuffle.class);
    final ShuffleSchedulerForTest scheduler = createScheduler(startTime, 320, shuffle);

    int totalProducerNodes = 20;

    //Generate 320 events
    for (int i = 0; i < 320; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.addKnownMapOutput("host" + (i % totalProducerNodes),
          10000, i, "hostUrl", inputAttemptIdentifier);
    }

    //Tasks fail in 20% of nodes 3 times, but are able to proceed further
    for (int i = 0; i < 64; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");

      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i %
          totalProducerNodes) + ":" + 10000, ""), false, true, false);

      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i %
          totalProducerNodes) + ":" + 10000, ""), false, true, false);

      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i %
          totalProducerNodes) + ":" + 10000, ""), false, true, false);

      MapOutput mapOutput = MapOutput
          .createMemoryMapOutput(inputAttemptIdentifier, mock(FetchedInputAllocatorOrderedGrouped.class),
              100, false);
      scheduler.copySucceeded(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), 100, 200, startTime + (i * 100), mapOutput, false);
    }

      //319 succeeds
    for (int i = 64; i < 319; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      MapOutput mapOutput = MapOutput
          .createMemoryMapOutput(inputAttemptIdentifier, mock(FetchedInputAllocatorOrderedGrouped.class),
              100, false);
      scheduler.copySucceeded(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), 100, 200, startTime + (i * 100), mapOutput, false);
    }

    //1 fails (last fetch)
    InputAttemptIdentifier inputAttemptIdentifier =
        new InputAttemptIdentifier(319, 0, "attempt_");
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(319, "host" + (319 % totalProducerNodes)
        + ":" + 10000, ""), false, true, false);

    //stall the shuffle (but within limits)
    scheduler.lastProgressTime = System.currentTimeMillis() - 100000;

    assertEquals(scheduler.remainingMaps.get(), 1);

    //Retry for 3 more times
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(319, "host" + (319 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(319, "host" + (319 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(319, "host" + (319 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);

    // failedShufflesSinceLastCompletion has crossed the limits. 20% of other nodes had failures as
    // well. However, it has failed only in one host. So this should proceed
    // until AM decides to restart the producer.
    verify(shuffle, times(0)).reportException(any(Throwable.class));

    //stall the shuffle (but within limits)
    scheduler.lastProgressTime = System.currentTimeMillis() - 300000;
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(319, "host" + (319 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);
    verify(shuffle, times(1)).reportException(any(Throwable.class));

  }

  @Test(timeout = 60000)
  /**
   * Scenario
   *    - Shuffle has progressed enough
   *    - Last event is yet to arrive
   *    - Failures start happening after Shuffle has progressed enough
   *    - no of attempts failing does not exceed maxFailedUniqueFetches (5)
   *    - Stalled
   * Expected result
   *    - Do not throw errors, as Shuffle is yet to receive inputs
   *
   */
  public void testReducerHealth_5() throws IOException {
    long startTime = System.currentTimeMillis() - 500000;
    Shuffle shuffle = mock(Shuffle.class);
    final ShuffleSchedulerForTest scheduler = createScheduler(startTime, 320, shuffle);

    int totalProducerNodes = 20;

    //Generate 319 events (last event has not arrived)
    for (int i = 0; i < 319; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.addKnownMapOutput("host" + (i % totalProducerNodes),
          10000, i, "hostUrl", inputAttemptIdentifier);
    }

    //318 succeeds
    for (int i = 0; i < 319; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      MapOutput mapOutput = MapOutput
          .createMemoryMapOutput(inputAttemptIdentifier, mock(FetchedInputAllocatorOrderedGrouped.class),
              100, false);
      scheduler.copySucceeded(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), 100, 200, startTime + (i * 100), mapOutput, false);
    }

    //1 fails (last fetch)
    InputAttemptIdentifier inputAttemptIdentifier =
        new InputAttemptIdentifier(318, 0, "attempt_");
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(318, "host" + (318 % totalProducerNodes)
        + ":" + 10000, ""), false, true, false);

    //stall the shuffle
    scheduler.lastProgressTime = System.currentTimeMillis() - 1000000;

    assertEquals(scheduler.remainingMaps.get(), 1);

    //Retry for 3 more times
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(318, "host" + (318 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(318, "host" + (318 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);
    scheduler.copyFailed(inputAttemptIdentifier, new MapHost(318, "host" + (318 %
        totalProducerNodes)
        + ":" + 10000, ""), false, true, false);

    //Shuffle has not received the events completely. So do not bail out yet.
    verify(shuffle, times(0)).reportException(any(Throwable.class));
  }


  @Test(timeout = 60000)
  /**
   * Scenario
   *    - Shuffle has NOT progressed enough
   *    - Failures start happening
   *    - no of attempts failing exceed maxFailedUniqueFetches (5)
   *    - Not stalled
   * Expected result
   *    - Bail out
   *
   */
  public void testReducerHealth_6() throws IOException {
    Configuration conf = new TezConfiguration();
    conf.setBoolean
        (TezRuntimeConfiguration
                .TEZ_RUNTIME_SHUFFLE_FAILED_CHECK_SINCE_LAST_COMPLETION, true);
    _testReducerHealth_6(conf);

    conf.setBoolean
        (TezRuntimeConfiguration
            .TEZ_RUNTIME_SHUFFLE_FAILED_CHECK_SINCE_LAST_COMPLETION, false);
    _testReducerHealth_6(conf);

  }

  public void _testReducerHealth_6(Configuration conf) throws IOException {
    long startTime = System.currentTimeMillis() - 500000;
    Shuffle shuffle = mock(Shuffle.class);
    final ShuffleSchedulerForTest scheduler = createScheduler(startTime, 320,
        shuffle, conf);

    int totalProducerNodes = 20;

    //Generate 320 events (last event has not arrived)
    for (int i = 0; i < 320; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.addKnownMapOutput("host" + (i % totalProducerNodes),
          10000, i, "hostUrl", inputAttemptIdentifier);
    }

    //10 succeeds
    for (int i = 0; i < 10; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      MapOutput mapOutput = MapOutput
          .createMemoryMapOutput(inputAttemptIdentifier, mock(FetchedInputAllocatorOrderedGrouped.class),
              100, false);
      scheduler.copySucceeded(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), 100, 200, startTime + (i * 100), mapOutput, false);
    }

    //5 fetches fail once
    for (int i = 10; i < 15; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
    }

    assertTrue(scheduler.failureCounts.size() >= 5);
    assertEquals(scheduler.remainingMaps.get(), 310);

    //Do not bail out (number of failures is just 5)
    verify(scheduler.reporter, times(0)).reportException(any(Throwable.class));

    //5 fetches fail repeatedly
    for (int i = 10; i < 15; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier, new MapHost(i, "host" + (i % totalProducerNodes)
          + ":" + 10000, ""), false, true, false);
    }

    boolean checkFailedFetchSinceLastCompletion = conf.getBoolean
        (TezRuntimeConfiguration
                .TEZ_RUNTIME_SHUFFLE_FAILED_CHECK_SINCE_LAST_COMPLETION,
            TezRuntimeConfiguration
                .TEZ_RUNTIME_SHUFFLE_FAILED_CHECK_SINCE_LAST_COMPLETION_DEFAULT);
    if (checkFailedFetchSinceLastCompletion) {
      // Now bail out, as Shuffle has crossed the
      // failedShufflesSinceLastCompletion limits. (even
      // though reducerHeathly is
      verify(shuffle, atLeast(1)).reportException(any(Throwable.class));
    } else {
      //Do not bail out yet.
      verify(shuffle, atLeast(0)).reportException(any(Throwable.class));
    }

  }

  @Test(timeout = 60000)
  /**
   * Scenario
   *    - reducer has not progressed enough
   *    - fetch fails >
   *    TEZ_RUNTIME_SHUFFLE_ACCEPTABLE_HOST_FETCH_FAILURE_FRACTION
   * Expected result
   *    - fail the reducer
   */
  public void testReducerHealth_7() throws IOException {
    long startTime = System.currentTimeMillis() - 500000;
    Shuffle shuffle = mock(Shuffle.class);
    final ShuffleSchedulerForTest scheduler = createScheduler(startTime, 320, shuffle);

    int totalProducerNodes = 20;

    //Generate 320 events
    for (int i = 0; i < 320; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.addKnownMapOutput("host" + (i % totalProducerNodes), 10000, i,
          "hostUrl", inputAttemptIdentifier);
    }

    //100 succeeds
    for (int i = 0; i < 100; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      MapOutput mapOutput = MapOutput
          .createMemoryMapOutput(inputAttemptIdentifier, mock(FetchedInputAllocatorOrderedGrouped.class),
              100, false);
      scheduler.copySucceeded(inputAttemptIdentifier,
          new MapHost(i, "host" + (i % totalProducerNodes) + ":" + 10000, ""),
          100, 200, startTime + (i * 100), mapOutput, false);
    }

    //99 fails
    for (int i = 100; i < 199; i++) {
      InputAttemptIdentifier inputAttemptIdentifier =
          new InputAttemptIdentifier(i, 0, "attempt_");
      scheduler.copyFailed(inputAttemptIdentifier,
          new MapHost(i, "host" + (i % totalProducerNodes) + ":" + 10000, ""),
          false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier,
          new MapHost(i, "host" + (i % totalProducerNodes) + ":" + 10000, ""),
          false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier,
          new MapHost(i, "host" + (i % totalProducerNodes) + ":" + 10000, ""),
          false, true, false);
      scheduler.copyFailed(inputAttemptIdentifier,
          new MapHost(i, "host" + (i % totalProducerNodes) + ":" + 10000, ""),
          false, true, false);
    }

    verify(shuffle, atLeast(1)).reportException(any(Throwable.class));
  }

  private ShuffleSchedulerForTest createScheduler(long startTime, int
      numInputs, Shuffle shuffle, Configuration conf)
      throws IOException {
    InputContext inputContext = createTezInputContext();
    MergeManager mergeManager = mock(MergeManager.class);

    final ShuffleSchedulerForTest scheduler =
        new ShuffleSchedulerForTest(inputContext, conf, numInputs, shuffle, mergeManager,
            mergeManager,startTime, null, false, 0, "srcName");
    return scheduler;
  }

  private ShuffleSchedulerForTest createScheduler(long startTime, int numInputs, Shuffle shuffle)
      throws IOException {
    return createScheduler(startTime, numInputs, shuffle, new
        TezConfiguration());
  }

  @Test(timeout = 60000)
  public void testPenalty() throws IOException, InterruptedException {
    long startTime = System.currentTimeMillis();
    Shuffle shuffle = mock(Shuffle.class);
    final ShuffleSchedulerForTest scheduler = createScheduler(startTime, 1, shuffle);

    InputAttemptIdentifier inputAttemptIdentifier =
        new InputAttemptIdentifier(0, 0, "attempt_");
    scheduler.addKnownMapOutput("host0", 10000, 0, "hostUrl", inputAttemptIdentifier);

    assertTrue(scheduler.pendingHosts.size() == 1);
    assertTrue(scheduler.pendingHosts.iterator().next().getState() == MapHost.State.PENDING);
    MapHost mapHost = scheduler.pendingHosts.iterator().next();

    //Fails to pull from host0. host0 should be added to penalties
    scheduler.copyFailed(inputAttemptIdentifier, mapHost, false, true, false);

    //Should not get host, as it is added to penalty loop
    MapHost host = scheduler.getHost();
    assertFalse(host.getIdentifier(), host.getIdentifier().equalsIgnoreCase("host0:10000"));

    //Refree thread would release it after INITIAL_PENALTY timeout
    Thread.sleep(ShuffleScheduler.INITIAL_PENALTY + 1000);
    host = scheduler.getHost();
    assertFalse(host.getIdentifier(), host.getIdentifier().equalsIgnoreCase("host0:10000"));
  }

  @Test(timeout = 5000)
  public void testShutdown() throws Exception {
    InputContext inputContext = createTezInputContext();
    Configuration conf = new TezConfiguration();
    int numInputs = 10;
    Shuffle shuffle = mock(Shuffle.class);
    MergeManager mergeManager = mock(MergeManager.class);

    final ShuffleSchedulerForTest scheduler =
        new ShuffleSchedulerForTest(inputContext, conf, numInputs, shuffle, mergeManager,
            mergeManager,
            System.currentTimeMillis(), null, false, 0, "srcName");

    ExecutorService executor = Executors.newFixedThreadPool(1);
    try {
      Future<Void> executorFuture = executor.submit(new Callable<Void>() {
        @Override
        public Void call() throws Exception {
          scheduler.start();
          return null;
        }
      });

      InputAttemptIdentifier[] identifiers = new InputAttemptIdentifier[numInputs];

      for (int i = 0; i < numInputs; i++) {
        InputAttemptIdentifier inputAttemptIdentifier =
            new InputAttemptIdentifier(i, 0, "attempt_");
        scheduler.addKnownMapOutput("host" + i, 10000, 1, "hostUrl", inputAttemptIdentifier);
        identifiers[i] = inputAttemptIdentifier;
      }

      MapHost[] mapHosts = new MapHost[numInputs];
      int count = 0;
      for (MapHost mh : scheduler.mapLocations.values()) {
        mapHosts[count++] = mh;
      }

      // Copy succeeded for 1 less host
      for (int i = 0; i < numInputs - 1; i++) {
        MapOutput mapOutput = MapOutput
            .createMemoryMapOutput(identifiers[i], mock(FetchedInputAllocatorOrderedGrouped.class),
                100, false);
        scheduler.copySucceeded(identifiers[i], mapHosts[i], 20, 25, 100, mapOutput, false);
        scheduler.freeHost(mapHosts[i]);
      }

      scheduler.close();
      // Ensure the executor exits, and without an error.
      executorFuture.get();
    } finally {
      scheduler.close();
      executor.shutdownNow();
    }
  }


  private InputContext createTezInputContext() throws IOException {
    ApplicationId applicationId = ApplicationId.newInstance(1, 1);
    InputContext inputContext = mock(InputContext.class);
    doReturn(applicationId).when(inputContext).getApplicationId();
    doReturn("sourceVertex").when(inputContext).getSourceVertexName();
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    ExecutionContext executionContext = new ExecutionContextImpl("localhost");
    doReturn(executionContext).when(inputContext).getExecutionContext();
    ByteBuffer shuffleBuffer = ByteBuffer.allocate(4).putInt(0, 4);
    doReturn(shuffleBuffer).when(inputContext).getServiceProviderMetaData(anyString());
    Token<JobTokenIdentifier>
        sessionToken = new Token<JobTokenIdentifier>(new JobTokenIdentifier(new Text("text")),
        new JobTokenSecretManager());
    ByteBuffer tokenBuffer = TezCommonUtils.serializeServiceData(sessionToken);
    doReturn(tokenBuffer).when(inputContext).getServiceConsumerMetaData(anyString());
    return inputContext;
  }

  private static class ShuffleSchedulerForTest extends ShuffleScheduler {

    private final AtomicInteger numFetchersCreated = new AtomicInteger(0);
    private final boolean fetcherShouldWait;
    private final ExceptionReporter reporter;

    public ShuffleSchedulerForTest(InputContext inputContext, Configuration conf,
                                   int numberOfInputs,
                                   Shuffle shuffle,
                                   MergeManager mergeManager,
                                   FetchedInputAllocatorOrderedGrouped allocator, long startTime,
                                   CompressionCodec codec,
                                   boolean ifileReadAhead, int ifileReadAheadLength,
                                   String srcNameTrimmed) throws IOException {
      this(inputContext, conf, numberOfInputs, shuffle, mergeManager, allocator, startTime, codec,
          ifileReadAhead, ifileReadAheadLength, srcNameTrimmed, false);
    }

    public ShuffleSchedulerForTest(InputContext inputContext, Configuration conf,
                                   int numberOfInputs,
                                   Shuffle shuffle,
                                   MergeManager mergeManager,
                                   FetchedInputAllocatorOrderedGrouped allocator, long startTime,
                                   CompressionCodec codec,
                                   boolean ifileReadAhead, int ifileReadAheadLength,
                                   String srcNameTrimmed, boolean fetcherShouldWait) throws IOException {
      super(inputContext, conf, numberOfInputs, shuffle, mergeManager, allocator, startTime, codec,
          ifileReadAhead, ifileReadAheadLength, srcNameTrimmed);
      this.fetcherShouldWait = fetcherShouldWait;
      this.reporter = shuffle;
    }

    @Override
    FetcherOrderedGrouped constructFetcherForHost(MapHost mapHost) {
      numFetchersCreated.incrementAndGet();
      FetcherOrderedGrouped mockFetcher = mock(FetcherOrderedGrouped.class);
      doAnswer(new Answer() {
        @Override
        public Object answer(InvocationOnMock invocation) throws Throwable {
          if (fetcherShouldWait) {
            Thread.sleep(100000l);
          }
          return null;
        }
      }).when(mockFetcher).callInternal();
      return mockFetcher;
    }
  }
}
