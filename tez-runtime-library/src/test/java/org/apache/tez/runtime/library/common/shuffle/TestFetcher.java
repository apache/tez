/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.runtime.library.common.shuffle;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.shuffle.FetchedInput.Type;
import org.apache.tez.runtime.library.common.shuffle.Fetcher.FetcherBuilder;
import org.apache.tez.runtime.library.common.shuffle.Fetcher.PathPartition;
import org.apache.tez.runtime.library.common.shuffle.api.ShuffleHandlerError;
import org.apache.tez.runtime.library.common.shuffle.impl.ShuffleManager;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.testutils.RuntimeTestUtils;

import com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

public class TestFetcher {
  private static final String SHUFFLE_INPUT_FILE_PREFIX = "shuffle_input_file_";
  private static String HOST = "localhost";
  private static int PORT = 41;

  @Test
  @Timeout(value = 3000, unit = TimeUnit.MILLISECONDS)
  public void testLocalFetchModeSetting() throws Exception {
    TezConfiguration conf = new TezConfiguration();
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, true);
    InputAttemptIdentifier[] srcAttempts = {
        new InputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1")
    };
    FetcherCallback fetcherCallback = mock(FetcherCallback.class);
    final boolean ENABLE_LOCAL_FETCH = true;
    final boolean DISABLE_LOCAL_FETCH = false;

    Fetcher.FetcherBuilder builder = new Fetcher.FetcherBuilder(fetcherCallback, null, null,
        createMockInputContext(), null, conf, ENABLE_LOCAL_FETCH, HOST,
        PORT, false, true, false);
    builder.assignWork(HOST, PORT, 0, 1, Arrays.asList(srcAttempts));
    Fetcher fetcher = spy(builder.build());

    FetchResult fr = new FetchResult(HOST, PORT, 0, 1, Arrays.asList(srcAttempts));
    Fetcher.HostFetchResult hfr =
        new Fetcher.HostFetchResult(fr, InputAttemptFetchFailure.fromAttempts(srcAttempts), false);
    doReturn(hfr).when(fetcher).setupLocalDiskFetch();
    doReturn(null).when(fetcher).doHttpFetch();
    doNothing().when(fetcher).shutdown();

    fetcher.call();

    verify(fetcher).setupLocalDiskFetch();
    verify(fetcher, never()).doHttpFetch();

    // when enabled and hostname does not match use http fetch.
    builder = new Fetcher.FetcherBuilder(fetcherCallback, null, null,
        createMockInputContext(), null, conf, ENABLE_LOCAL_FETCH, HOST,
        PORT, false, true, false);
    builder.assignWork(HOST + "_OTHER", PORT, 0, 1, Arrays.asList(srcAttempts));
    fetcher = spy(builder.build());

    doReturn(null).when(fetcher).setupLocalDiskFetch();
    doReturn(hfr).when(fetcher).doHttpFetch();
    doNothing().when(fetcher).shutdown();

    fetcher.call();

    verify(fetcher, never()).setupLocalDiskFetch();
    verify(fetcher).doHttpFetch();

    // when enabled and port does not match use http fetch.
    builder = new Fetcher.FetcherBuilder(fetcherCallback, null, null,
        createMockInputContext(), null, conf, ENABLE_LOCAL_FETCH, HOST,
        PORT, false, true, false);
    builder.assignWork(HOST, PORT + 1, 0, 1, Arrays.asList(srcAttempts));
    fetcher = spy(builder.build());

    doReturn(null).when(fetcher).setupLocalDiskFetch();
    doReturn(hfr).when(fetcher).doHttpFetch();
    doNothing().when(fetcher).shutdown();

    fetcher.call();

    verify(fetcher, never()).setupLocalDiskFetch();
    verify(fetcher).doHttpFetch();

    // When disabled use http fetch
    conf.setBoolean(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, false);
    builder = new Fetcher.FetcherBuilder(fetcherCallback, null, null,
        createMockInputContext(), null, conf, DISABLE_LOCAL_FETCH, HOST,
        PORT, false, true, false);
    builder.assignWork(HOST, PORT, 0, 1, Arrays.asList(srcAttempts));
    fetcher = spy(builder.build());

    doReturn(null).when(fetcher).setupLocalDiskFetch();
    doReturn(hfr).when(fetcher).doHttpFetch();
    doNothing().when(fetcher).shutdown();

    fetcher.call();

    verify(fetcher, never()).setupLocalDiskFetch();
    verify(fetcher).doHttpFetch();
  }

  @Test
  @Timeout(value = 3000, unit = TimeUnit.MILLISECONDS)
  public void testSetupLocalDiskFetch() throws Exception {

    CompositeInputAttemptIdentifier[] srcAttempts = {
        new CompositeInputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0", 1),
        new CompositeInputAttemptIdentifier(1, 2, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1", 1),
        new CompositeInputAttemptIdentifier(2, 3, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_2", 1),
        new CompositeInputAttemptIdentifier(3, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3", 1),
        new CompositeInputAttemptIdentifier(4, 5, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_4", 1)
    };
    final int FIRST_FAILED_ATTEMPT_IDX = 2;
    final int SECOND_FAILED_ATTEMPT_IDX = 4;
    final int[] successfulAttempts = {0, 1, 3};

    TezConfiguration conf = new TezConfiguration();
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, "true");
    int partition = 42;
    FetcherCallback callback = mock(FetcherCallback.class);
    FetcherBuilder builder = new FetcherBuilder(callback, null, null,
        createMockInputContext(), null, conf, true, HOST, PORT,
        false, true, true);
    ArrayList<InputAttemptIdentifier> inputAttemptIdentifiers = new ArrayList<>();
    for(CompositeInputAttemptIdentifier compositeInputAttemptIdentifier : srcAttempts) {
      for(int i=0;i<compositeInputAttemptIdentifier.getInputIdentifierCount();i++) {
        inputAttemptIdentifiers.add(compositeInputAttemptIdentifier.expand(i));
      }
    }
    ArrayList<InputAttemptIdentifier> list = new ArrayList<InputAttemptIdentifier>();
    list.addAll(Arrays.asList(srcAttempts));
    builder.assignWork(HOST, PORT, partition, 1, list);
    Fetcher fetcher = spy(builder.build());
    for(CompositeInputAttemptIdentifier compositeInputAttemptIdentifier : srcAttempts) {
      for(int i=0;i<compositeInputAttemptIdentifier.getInputIdentifierCount();i++) {
        inputAttemptIdentifiers.add(compositeInputAttemptIdentifier.expand(i));
        PathPartition pathPartition =
            new PathPartition(compositeInputAttemptIdentifier.getPathComponent(),partition + i);
        fetcher.getPathToAttemptMap().put(pathPartition, compositeInputAttemptIdentifier.expand(i));
      }
    }
    doAnswer(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        return new Path(SHUFFLE_INPUT_FILE_PREFIX + args[0]);
      }
    }).when(fetcher).getShuffleInputFileName(anyString(), any());

    doAnswer(new Answer<TezIndexRecord>() {
      @Override
      public TezIndexRecord answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        InputAttemptIdentifier srcAttemptId = (InputAttemptIdentifier) args[0];
        String pathComponent = srcAttemptId.getPathComponent();
        int len = pathComponent.length();
        long p = Long.valueOf(pathComponent.substring(len - 1, len));
        // Fail the 3rd one and 5th one.
        if (p == FIRST_FAILED_ATTEMPT_IDX || p == SECOND_FAILED_ATTEMPT_IDX) {
          throw new IOException("failing on 3/5th input to simulate failure case");
        }
        // match with params for copySucceeded below.
        return new TezIndexRecord(p * 10, p * 1000, p * 100);
      }
    }).when(fetcher).getTezIndexRecord(any(InputAttemptIdentifier.class), anyInt());

    doNothing().when(fetcher).shutdown();
    doNothing().when(callback).fetchSucceeded(anyString(), any(InputAttemptIdentifier.class),
        any(FetchedInput.class), anyLong(), anyLong(), anyLong());
    doNothing().when(callback).fetchFailed(anyString(), any(InputAttemptFetchFailure.class), eq(false));

    FetchResult fetchResult = fetcher.call();

    verify(fetcher).setupLocalDiskFetch();

    // expect 3 successes and 2 failures
    for (int i : successfulAttempts) {
      verifyFetchSucceeded(callback, srcAttempts[i], conf);
    }
    verify(callback).fetchFailed(eq(HOST),
        eq(InputAttemptFetchFailure
            .fromCompositeAttemptLocalFetchFailure(srcAttempts[FIRST_FAILED_ATTEMPT_IDX])),
        eq(false));
    verify(callback).fetchFailed(eq(HOST),
        eq(InputAttemptFetchFailure
            .fromCompositeAttemptLocalFetchFailure(srcAttempts[SECOND_FAILED_ATTEMPT_IDX])),
        eq(false));

    assertEquals(fetchResult.getHost(), HOST, "fetchResult host");
    assertEquals(fetchResult.getPartition(), partition, "fetchResult partition");
    assertEquals(fetchResult.getPort(), PORT, "fetchResult port");

    // 3nd and 5th attempt failed
    List<InputAttemptIdentifier> pendingInputs = Lists.newArrayList(fetchResult.getPendingInputs());
    assertEquals(pendingInputs.size(), 2, "fetchResult pendingInput size");
    assertEquals(pendingInputs.get(0), srcAttempts[FIRST_FAILED_ATTEMPT_IDX], "fetchResult failed attempt");
    assertEquals(pendingInputs.get(1), srcAttempts[SECOND_FAILED_ATTEMPT_IDX], "fetchResult failed attempt");
  }

  protected void verifyFetchSucceeded(FetcherCallback callback, CompositeInputAttemptIdentifier srcAttempId, Configuration conf) throws IOException {
    String pathComponent = srcAttempId.getPathComponent();
    int len = pathComponent.length();
    long p = Long.valueOf(pathComponent.substring(len - 1, len));
    ArgumentCaptor<LocalDiskFetchedInput> capturedFetchedInput =
        ArgumentCaptor.forClass(LocalDiskFetchedInput.class);
    verify(callback)
        .fetchSucceeded(eq(HOST), eq(srcAttempId.expand(0)), capturedFetchedInput.capture(), eq(p * 100),
            eq(p * 1000), anyLong());
    LocalDiskFetchedInput f = capturedFetchedInput.getValue();
    assertEquals(f.getInputFile().toString(), SHUFFLE_INPUT_FILE_PREFIX + pathComponent, "success callback filename");
    assertInstanceOf(RawLocalFileSystem.class, f.getLocalFS(), "success callback fs");
    assertEquals(f.getStartOffset(), p * 10, "success callback filesystem");
    assertEquals(f.getSize(), p * 100, "success callback compressed size");
    assertEquals(f.getInputAttemptIdentifier(), srcAttempId.expand(0), "success callback input id");
    assertEquals(f.getType(), Type.DISK_DIRECT, "success callback type");
  }

  @Test
  @Timeout(value = 5000, unit = TimeUnit.MILLISECONDS)
  public void testInputAttemptIdentifierMap() {
    InputAttemptIdentifier[] srcAttempts = {
        new InputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0",
            false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0),
            //duplicate entry
        new InputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0",
            false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0),
        // pipeline shuffle based identifiers, with multiple attempts
        new InputAttemptIdentifier(1, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1",
            false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0),
        new InputAttemptIdentifier(1, 2, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1",
            false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0),
        new InputAttemptIdentifier(1, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_2",
            false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 1),
        new InputAttemptIdentifier(1, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3",
            false, InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE, 2),
        new InputAttemptIdentifier(2, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3",
            false, InputAttemptIdentifier.SPILL_INFO.FINAL_MERGE_ENABLED, 0)
    };
    InputAttemptIdentifier[] expectedSrcAttempts = {
        new InputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0",
            false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0),
        // pipeline shuffle based identifiers
        new InputAttemptIdentifier(1, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1",
            false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0),
        new InputAttemptIdentifier(1, 2, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1",
            false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 0),
        new InputAttemptIdentifier(1, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_2",
            false, InputAttemptIdentifier.SPILL_INFO.INCREMENTAL_UPDATE, 1),
        new InputAttemptIdentifier(1, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3",
            false, InputAttemptIdentifier.SPILL_INFO.FINAL_UPDATE, 2),
        new InputAttemptIdentifier(2, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3",
            false, InputAttemptIdentifier.SPILL_INFO.FINAL_MERGE_ENABLED, 0)
    };
    TezConfiguration conf = new TezConfiguration();
    conf.set(TezRuntimeConfiguration.TEZ_RUNTIME_OPTIMIZE_LOCAL_FETCH, "true");
    int partition = 42;
    FetcherCallback callback = mock(FetcherCallback.class);
    Fetcher.FetcherBuilder builder = new Fetcher.FetcherBuilder(callback, null, null,
        createMockInputContext(), null, conf, true, HOST, PORT,
        false, true, false);
    builder.assignWork(HOST, PORT, partition, 1, Arrays.asList(srcAttempts));
    Fetcher fetcher = spy(builder.build());
    fetcher.populateRemainingMap(new LinkedList<InputAttemptIdentifier>(Arrays.asList(srcAttempts)));
    assertEquals(expectedSrcAttempts.length, fetcher.srcAttemptsRemaining.size());
    Iterator<Entry<String, InputAttemptIdentifier>> iterator = fetcher.srcAttemptsRemaining.entrySet().iterator();
    int count = 0;
    while(iterator.hasNext()) {
      String key = iterator.next().getKey();
      assertEquals(0, expectedSrcAttempts[count++].toString().compareTo(key));
    }
  }

  @Test
  public void testShuffleHandlerDiskErrorUnordered()
      throws Exception {
    Configuration conf = new Configuration();

    InputContext inputContext = mock(InputContext.class);
    doReturn(new TezCounters()).when(inputContext).getCounters();
    doReturn("vertex").when(inputContext).getSourceVertexName();

    Fetcher.FetcherBuilder builder = new Fetcher.FetcherBuilder(mock(ShuffleManager.class), null,
        null, createMockInputContext(), null, conf, true, HOST, PORT,
        false, true, false);
    builder.assignWork(HOST, PORT, 0, 1, Arrays.asList(new InputAttemptIdentifier(0, 0)));

    Fetcher fetcher = builder.build();
    ShuffleHeader header =
        new ShuffleHeader(ShuffleHandlerError.DISK_ERROR_EXCEPTION.toString(), -1, -1, -1);
    DataInputStream input = RuntimeTestUtils.shuffleHeaderToDataInput(header);

    InputAttemptFetchFailure[] failures =
        fetcher.fetchInputs(input, null, new InputAttemptIdentifier(0, 0));
    assertEquals(1, failures.length);
    assertTrue(failures[0].isDiskErrorAtSource());
    assertFalse(failures[0].isLocalFetch());
  }

  private InputContext createMockInputContext() {
    InputContext inputContext = mock(InputContext.class);

    doReturn(ApplicationId.newInstance(0, 1)).when(inputContext).getApplicationId();
    doReturn(1).when(inputContext).getDagIdentifier();
    doReturn("sourceVertex").when(inputContext).getSourceVertexName();
    doReturn("taskVertex").when(inputContext).getTaskVertexName();

    return inputContext;
  }
}
