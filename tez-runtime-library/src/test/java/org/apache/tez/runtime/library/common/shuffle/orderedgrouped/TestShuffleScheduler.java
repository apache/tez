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

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
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
import org.apache.tez.runtime.library.common.InputIdentifier;
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
            new InputAttemptIdentifier(new InputIdentifier(i), 0, "attempt_");
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
            new InputAttemptIdentifier(new InputIdentifier(i), 0, "attempt_");
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
        scheduler.copySucceeded(identifiers[i], mapHosts[i], 20, 25, 100, mapOutput);
        scheduler.freeHost(mapHosts[i]);
      }

      // Ensure the executor exits, and without an error.
      executorFuture.get();
    } finally {
      scheduler.close();
      executor.shutdownNow();
    }
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
            new InputAttemptIdentifier(new InputIdentifier(i), 0, "attempt_");
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
        scheduler.copySucceeded(identifiers[i], mapHosts[i], 20, 25, 100, mapOutput);
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
