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

package org.apache.tez.runtime.library.common.shuffle.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.security.token.Token;
import org.apache.tez.common.TezExecutors;
import org.apache.tez.common.TezRuntimeFrameworkConfigs;
import org.apache.tez.common.TezSharedExecutor;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.api.events.DataMovementEvent;
import org.apache.tez.runtime.api.events.InputReadErrorEvent;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput;
import org.apache.tez.runtime.library.common.shuffle.FetchedInputAllocator;
import org.apache.tez.runtime.library.common.shuffle.Fetcher;
import org.apache.tez.runtime.library.common.shuffle.FetchResult;
import org.apache.tez.runtime.library.common.shuffle.InputHost;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.shuffle.impl.ShuffleUserPayloads.DataMovementEventPayloadProto;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


public class TestShuffleManager {

  private static final String FETCHER_HOST = "localhost";
  private static final int PORT = 8080;
  private static final String PATH_COMPONENT = "attempttmp";
  private final Configuration conf = new Configuration();
  private TezExecutors sharedExecutor;

  @Before
  public void setup() {
    sharedExecutor = new TezSharedExecutor(conf);
  }

  @After
  public void cleanup() {
    sharedExecutor.shutdownNow();
  }

  /**
   * One reducer fetches multiple partitions from each mapper.
   * For a given mapper, the reducer sends DataMovementEvents for several
   * partitions, wait for some time and then send DataMovementEvents for the
   * rest of the partitions. Then do the same thing for the next mapper.
   * Verify ShuffleManager is able to get all the events.
  */
  @Test(timeout = 50000)
  public void testMultiplePartitions() throws Exception {
    final int numOfMappers = 3;
    final int numOfPartitions = 5;
    final int firstPart = 2;
    InputContext inputContext = createInputContext();
    ShuffleManagerForTest shuffleManager = createShuffleManager(inputContext,
        numOfMappers * numOfPartitions);
    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);

    ShuffleInputEventHandlerImpl handler = new ShuffleInputEventHandlerImpl(
        inputContext, shuffleManager, inputAllocator, null, false, 0, false);
    shuffleManager.run();

    List<Event> eventList = new LinkedList<Event>();

    int targetIndex = 0; // The physical input index within the reduce task

    for (int i = 0; i < numOfMappers; i++) {
      String mapperHost = "host" + i;
      int srcIndex = 20; // The physical output index within the map task
      // Send the first batch of DataMovementEvents
      eventList.clear();
      for (int j = 0; j < firstPart; j++) {
        Event dme = createDataMovementEvent(mapperHost, srcIndex++,
            targetIndex++);
        eventList.add(dme);
      }
      handler.handleEvents(eventList);

      Thread.sleep(500);


      // Send the second batch of DataMovementEvents
      eventList.clear();
      for (int j = 0; j < numOfPartitions - firstPart; j++) {
        Event dme = createDataMovementEvent(mapperHost, srcIndex++,
            targetIndex++);
        eventList.add(dme);
      }
      handler.handleEvents(eventList);
    }

    int waitCount = 100;
    while (waitCount-- > 0 &&
        !(shuffleManager.isFetcherExecutorShutdown() &&
            numOfMappers * numOfPartitions ==
                shuffleManager.getNumOfCompletedInputs())) {
      Thread.sleep(100);
    }
    assertTrue(shuffleManager.isFetcherExecutorShutdown());
    assertEquals(numOfMappers * numOfPartitions,
        shuffleManager.getNumOfCompletedInputs());
  }

  private InputContext createInputContext() throws IOException {
    DataOutputBuffer port_dob = new DataOutputBuffer();
    port_dob.writeInt(PORT);
    final ByteBuffer shuffleMetaData = ByteBuffer.wrap(port_dob.getData(), 0,
        port_dob.getLength());
    port_dob.close();

    ExecutionContext executionContext = mock(ExecutionContext.class);
    doReturn(FETCHER_HOST).when(executionContext).getHostName();

    InputContext inputContext = mock(InputContext.class);
    doReturn(new TezCounters()).when(inputContext).getCounters();
    doReturn("sourceVertex").when(inputContext).getSourceVertexName();
    doReturn(shuffleMetaData).when(inputContext)
        .getServiceProviderMetaData(conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
            TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT));
    doReturn(executionContext).when(inputContext).getExecutionContext();
    when(inputContext.createTezFrameworkExecutorService(anyInt(), anyString())).thenAnswer(
        new Answer<ExecutorService>() {
          @Override
          public ExecutorService answer(InvocationOnMock invocation) throws Throwable {
            return sharedExecutor.createExecutorService(
                invocation.getArgumentAt(0, Integer.class),
                invocation.getArgumentAt(1, String.class));
          }
        });
    return inputContext;
  }

  @Test(timeout=5000)
  public void testUseSharedExecutor() throws Exception {
    InputContext inputContext = createInputContext();
    createShuffleManager(inputContext, 2);
    verify(inputContext, times(0)).createTezFrameworkExecutorService(anyInt(), anyString());

    inputContext = createInputContext();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCHER_USE_SHARED_POOL, true);
    createShuffleManager(inputContext, 2);
    verify(inputContext).createTezFrameworkExecutorService(anyInt(), anyString());
  }

  @Test (timeout = 20000)
  public void testProgressWithEmptyPendingHosts() throws Exception {
    InputContext inputContext = createInputContext();
    final ShuffleManager shuffleManager = spy(createShuffleManager(inputContext, 1));
    Thread schedulerGetHostThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          shuffleManager.run();
          } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    schedulerGetHostThread.start();
    Thread.currentThread().sleep(1000 * 3 + 1000);
    schedulerGetHostThread.interrupt();
    verify(inputContext, atLeast(3)).notifyProgress();
  }

  @Test (timeout = 200000)
  public void testFetchFailed() throws Exception {
    InputContext inputContext = createInputContext();
    final ShuffleManager shuffleManager = spy(createShuffleManager(inputContext, 1));
    Thread schedulerGetHostThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          shuffleManager.run();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    });
    InputAttemptIdentifier inputAttemptIdentifier
        = new InputAttemptIdentifier(1, 1);

    schedulerGetHostThread.start();
    Thread.sleep(1000);
    shuffleManager.fetchFailed("host1", inputAttemptIdentifier, false);
    Thread.sleep(1000);

    ArgumentCaptor<List> captor = ArgumentCaptor.forClass(List.class);
    verify(inputContext, times(1))
        .sendEvents(captor.capture());
    Assert.assertEquals("Size was: " + captor.getAllValues().size(),
        captor.getAllValues().size(), 1);
    List<Event> capturedList = captor.getAllValues().get(0);
    Assert.assertEquals("Size was: " + capturedList.size(),
        capturedList.size(), 1);
    InputReadErrorEvent inputEvent = (InputReadErrorEvent)capturedList.get(0);
    Assert.assertEquals("Number of failures was: " + inputEvent.getNumFailures(),
        inputEvent.getNumFailures(), 1);

    shuffleManager.fetchFailed("host1", inputAttemptIdentifier, false);
    shuffleManager.fetchFailed("host1", inputAttemptIdentifier, false);

    Thread.sleep(1000);
    verify(inputContext, times(1)).sendEvents(any());

    // Wait more than five seconds for the batch to go out
    Thread.sleep(5000);
    captor = ArgumentCaptor.forClass(List.class);
    verify(inputContext, times(2))
        .sendEvents(captor.capture());
    Assert.assertEquals("Size was: " + captor.getAllValues().size(),
        captor.getAllValues().size(), 2);
    capturedList = captor.getAllValues().get(1);
    Assert.assertEquals("Size was: " + capturedList.size(),
        capturedList.size(), 1);
    inputEvent = (InputReadErrorEvent)capturedList.get(0);
    Assert.assertEquals("Number of failures was: " + inputEvent.getNumFailures(),
        inputEvent.getNumFailures(), 2);


    schedulerGetHostThread.interrupt();
  }

  private ShuffleManagerForTest createShuffleManager(
      InputContext inputContext, int expectedNumOfPhysicalInputs)
          throws IOException {
    Path outDirBase = new Path(".", "outDir");
    String[] outDirs = new String[] { outDirBase.toString() };
    doReturn(outDirs).when(inputContext).getWorkDirs();
    conf.setStrings(TezRuntimeFrameworkConfigs.LOCAL_DIRS,
        inputContext.getWorkDirs());
    // 5 seconds
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_BATCH_WAIT, 5000);

    DataOutputBuffer out = new DataOutputBuffer();
    Token<JobTokenIdentifier> token = new Token<JobTokenIdentifier>(new JobTokenIdentifier(),
        new JobTokenSecretManager(null));
    token.write(out);
    doReturn(ByteBuffer.wrap(out.getData())).when(inputContext).
        getServiceConsumerMetaData(
            conf.get(TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID,
                TezConfiguration.TEZ_AM_SHUFFLE_AUXILIARY_SERVICE_ID_DEFAULT));

    FetchedInputAllocator inputAllocator = mock(FetchedInputAllocator.class);
    return new ShuffleManagerForTest(inputContext, conf,
        expectedNumOfPhysicalInputs, 1024, false, -1, null, inputAllocator);
  }

  private Event createDataMovementEvent(String host, int srcIndex, int targetIndex) {
    DataMovementEventPayloadProto.Builder builder =
        DataMovementEventPayloadProto.newBuilder();
    builder.setHost(host);
    builder.setPort(PORT);
    builder.setPathComponent(PATH_COMPONENT);
    Event dme = DataMovementEvent
        .create(srcIndex, targetIndex, 0,
            builder.build().toByteString().asReadOnlyByteBuffer());
    return dme;
  }

  private static class ShuffleManagerForTest extends ShuffleManager {
    public ShuffleManagerForTest(InputContext inputContext, Configuration conf,
        int numInputs, int bufferSize, boolean ifileReadAheadEnabled,
        int ifileReadAheadLength, CompressionCodec codec,
        FetchedInputAllocator inputAllocator) throws IOException {
      super(inputContext, conf, numInputs, bufferSize, ifileReadAheadEnabled,
          ifileReadAheadLength, codec, inputAllocator);
    }

    @Override
    Fetcher constructFetcherForHost(InputHost inputHost, Configuration conf) {
      final Fetcher fetcher = spy(super.constructFetcherForHost(inputHost,
          conf));
      final FetchResult mockFetcherResult = mock(FetchResult.class);
      try {
        doAnswer(new Answer<FetchResult>() {
          @Override
          public FetchResult answer(InvocationOnMock invocation) throws Throwable {
            for(InputAttemptIdentifier input : fetcher.getSrcAttempts()) {
              ShuffleManagerForTest.this.fetchSucceeded(
                  fetcher.getHost(), input, new TestFetchedInput(input), 0, 0,
                      0);
            }
            return mockFetcherResult;
          }
        }).when(fetcher).callInternal();
      } catch (Exception e) {
        //ignore
      }
      return fetcher;
    }

    public int getNumOfCompletedInputs() {
      return completedInputSet.cardinality();
    }

    boolean isFetcherExecutorShutdown() {
      return fetcherExecutor.isShutdown();
    }
  }

  /**
   * Fake input that is added to the completed input list in case an input does not have any data.
   *
   */
  @VisibleForTesting
  static class TestFetchedInput extends FetchedInput {

    public TestFetchedInput(InputAttemptIdentifier inputAttemptIdentifier) {
      super(inputAttemptIdentifier, null);
    }

    @Override
    public long getSize() {
      return -1;
    }

    @Override
    public Type getType() {
      return Type.MEMORY;
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
      return null;
    }

    @Override
    public InputStream getInputStream() throws IOException {
      return null;
    }

    @Override
    public void commit() throws IOException {
    }

    @Override
    public void abort() throws IOException {
    }

    @Override
    public void free() {
    }
  }
}
