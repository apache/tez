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

package org.apache.tez.runtime.library.common.shuffle.orderedgrouped;

import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Lists;

import org.apache.tez.http.HttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.library.common.CompositeInputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.exceptions.FetcherReadTimeoutException;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;


public class TestFetcher {

  public static final String SHUFFLE_INPUT_FILE_PREFIX = "shuffle_input_file_";
  public static final String HOST = "localhost";
  public static final int PORT = 65;
  public static final int DAG_ID = 1;
  public static final String APP_ID = "application_1234_1";

  private TezCounters tezCounters = new TezCounters();
  private TezCounter ioErrsCounter = tezCounters.findCounter(ShuffleScheduler.SHUFFLE_ERR_GRP_NAME,
      ShuffleScheduler.ShuffleErrors.IO_ERROR.toString());
  private TezCounter wrongLengthErrsCounter =
      tezCounters.findCounter(ShuffleScheduler.SHUFFLE_ERR_GRP_NAME,
          ShuffleScheduler.ShuffleErrors.WRONG_LENGTH.toString());
  private TezCounter badIdErrsCounter =
      tezCounters.findCounter(ShuffleScheduler.SHUFFLE_ERR_GRP_NAME,
          ShuffleScheduler.ShuffleErrors.BAD_ID.toString());
  private TezCounter wrongMapErrsCounter =
      tezCounters.findCounter(ShuffleScheduler.SHUFFLE_ERR_GRP_NAME,
          ShuffleScheduler.ShuffleErrors.WRONG_MAP.toString());
  private TezCounter connectionErrsCounter =
      tezCounters.findCounter(ShuffleScheduler.SHUFFLE_ERR_GRP_NAME,
          ShuffleScheduler.ShuffleErrors.CONNECTION.toString());
  private TezCounter wrongReduceErrsCounter =
      tezCounters.findCounter(ShuffleScheduler.SHUFFLE_ERR_GRP_NAME,
          ShuffleScheduler.ShuffleErrors.WRONG_REDUCE.toString());

  static final Logger LOG = LoggerFactory.getLogger(TestFetcher.class);

  @Test(timeout = 5000)
  public void testInputsReturnedOnConnectionException() throws Exception {
    Configuration conf = new TezConfiguration();
    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);

    Shuffle shuffle = mock(Shuffle.class);

    InputContext inputContext = mock(InputContext.class);
    doReturn(new TezCounters()).when(inputContext).getCounters();
    doReturn("src vertex").when(inputContext).getSourceVertexName();

    MapHost mapHost = new MapHost(HOST, PORT, 0, 1);
    InputAttemptIdentifier inputAttemptIdentifier = new InputAttemptIdentifier(0, 0, "attempt");
    mapHost.addKnownMap(inputAttemptIdentifier);
    List<InputAttemptIdentifier> mapsForHost = Lists.newArrayList(inputAttemptIdentifier);
    doReturn(mapsForHost).when(scheduler).getMapsForHost(mapHost);

    FetcherOrderedGrouped fetcher =
        new FetcherOrderedGrouped(null, scheduler, merger, shuffle, null, false, 0,
            null, conf, false, HOST, PORT, "src vertex", mapHost, ioErrsCounter,
            wrongLengthErrsCounter, badIdErrsCounter,
            wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
            false, false, true, false);

    fetcher.call();
    verify(scheduler).getMapsForHost(mapHost);
    verify(scheduler).freeHost(mapHost);
    verify(scheduler).putBackKnownMapOutput(mapHost, inputAttemptIdentifier);
  }


  @Test(timeout = 5000)
  public void testLocalFetchModeSetting1() throws Exception {
    Configuration conf = new TezConfiguration();
    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);
    Shuffle shuffle = mock(Shuffle.class);

    InputContext inputContext = mock(InputContext.class);
    doReturn(new TezCounters()).when(inputContext).getCounters();
    doReturn("src vertex").when(inputContext).getSourceVertexName();

    final boolean ENABLE_LOCAL_FETCH = true;
    final boolean DISABLE_LOCAL_FETCH = false;
    MapHost mapHost = new MapHost(HOST, PORT, 0, 1);
    FetcherOrderedGrouped fetcher =
        new FetcherOrderedGrouped(null, scheduler, merger, shuffle, null, false, 0,
            null, conf, ENABLE_LOCAL_FETCH, HOST, PORT, "src vertex", mapHost, ioErrsCounter,
            wrongLengthErrsCounter, badIdErrsCounter,
            wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
            false, false, true, false);

    // when local mode is enabled and host and port matches use local fetch
    FetcherOrderedGrouped spyFetcher = spy(fetcher);
    doNothing().when(spyFetcher).setupLocalDiskFetch(mapHost);

    spyFetcher.fetchNext();

    verify(spyFetcher, times(1)).setupLocalDiskFetch(mapHost);
    verify(spyFetcher, never()).copyFromHost(any(MapHost.class));

    // if hostname does not match use http
    mapHost = new MapHost(HOST + "_OTHER", PORT, 0, 1);
    fetcher =
        new FetcherOrderedGrouped(null, scheduler, merger, shuffle, null, false, 0,
            null, conf, ENABLE_LOCAL_FETCH, HOST, PORT, "src vertex", mapHost, ioErrsCounter,
            wrongLengthErrsCounter, badIdErrsCounter,
            wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
            false, false, true, false);
    spyFetcher = spy(fetcher);
    doNothing().when(spyFetcher).setupLocalDiskFetch(mapHost);

    spyFetcher.fetchNext();

    verify(spyFetcher, never()).setupLocalDiskFetch(any(MapHost.class));
    verify(spyFetcher, times(1)).copyFromHost(mapHost);

    // if port does not match use http
    mapHost = new MapHost(HOST, PORT + 1, 0, 1);
    fetcher =
        new FetcherOrderedGrouped(null, scheduler, merger, shuffle, null, false, 0,
            null, conf, ENABLE_LOCAL_FETCH, HOST, PORT, "src vertex", mapHost, ioErrsCounter,
            wrongLengthErrsCounter, badIdErrsCounter,
            wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
            false, false, true, false);
    spyFetcher = spy(fetcher);
    doNothing().when(spyFetcher).setupLocalDiskFetch(mapHost);

    spyFetcher.fetchNext();

    verify(spyFetcher, never()).setupLocalDiskFetch(any(MapHost.class));
    verify(spyFetcher, times(1)).copyFromHost(mapHost);

    //if local fetch is not enabled
    mapHost = new MapHost(HOST, PORT, 0, 1);
    fetcher = new FetcherOrderedGrouped(null, scheduler, merger, shuffle, null, false, 0,
        null, conf, DISABLE_LOCAL_FETCH, HOST, PORT, "src vertex", mapHost, ioErrsCounter,
        wrongLengthErrsCounter, badIdErrsCounter,
        wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
        false, false, true, false);
    spyFetcher = spy(fetcher);
    doNothing().when(spyFetcher).setupLocalDiskFetch(mapHost);

    spyFetcher.fetchNext();

    verify(spyFetcher, never()).setupLocalDiskFetch(any(MapHost.class));
    verify(spyFetcher, times(1)).copyFromHost(mapHost);
  }

  @Test(timeout = 5000)
  public void testSetupLocalDiskFetch() throws Exception {
    Configuration conf = new TezConfiguration();
    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);
    Shuffle shuffle = mock(Shuffle.class);
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    when(inputContext.getSourceVertexName()).thenReturn("");

    MapHost host = new MapHost(HOST, PORT, 1, 1);
    FetcherOrderedGrouped fetcher = new FetcherOrderedGrouped(null, scheduler, merger, shuffle, null, false, 0,
        null, conf, true, HOST, PORT, "src vertex", host, ioErrsCounter, wrongLengthErrsCounter, badIdErrsCounter,
        wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
        false, false, true, false);
    FetcherOrderedGrouped spyFetcher = spy(fetcher);


    final List<CompositeInputAttemptIdentifier> srcAttempts = Arrays.asList(
        new CompositeInputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0", 1),
        new CompositeInputAttemptIdentifier(1, 2, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1", 1),
        new CompositeInputAttemptIdentifier(2, 3, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_2", 1),
        new CompositeInputAttemptIdentifier(3, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3", 1),
        new CompositeInputAttemptIdentifier(4, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_4", 1)
    );
    final int FIRST_FAILED_ATTEMPT_IDX = 2;
    final int SECOND_FAILED_ATTEMPT_IDX = 4;
    final int[] sucessfulAttemptsIndexes = { 0, 1, 3 };

    doReturn(srcAttempts).when(scheduler).getMapsForHost(host);

    final ConcurrentMap<ShuffleScheduler.PathPartition, InputAttemptIdentifier> pathToIdentifierMap = new ConcurrentHashMap<ShuffleScheduler.PathPartition, InputAttemptIdentifier>();
    for (CompositeInputAttemptIdentifier srcAttempt : srcAttempts) {
      for (int i = 0; i < srcAttempt.getInputIdentifierCount(); i++) {
        ShuffleScheduler.PathPartition pathPartition = new ShuffleScheduler.PathPartition(srcAttempt.getPathComponent(), host.getPartitionId() + i);
        pathToIdentifierMap.put(pathPartition, srcAttempt.expand(i));
        }
      }
    doAnswer(new Answer<InputAttemptIdentifier>() {
      @Override
      public InputAttemptIdentifier answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        String path = (String) args[0];
        int reduceId = (int) args[1];
        return pathToIdentifierMap.get(new ShuffleScheduler.PathPartition(path, reduceId));
      }
    }).when(scheduler)
        .getIdentifierForFetchedOutput(any(String.class), any(int.class));

    doAnswer(new Answer<MapOutput>() {
      @Override
      public MapOutput answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        MapOutput mapOutput = mock(MapOutput.class);
        doReturn(MapOutput.Type.DISK_DIRECT).when(mapOutput).getType();
        doReturn(args[0]).when(mapOutput).getAttemptIdentifier();
        return mapOutput;
      }
    }).when(spyFetcher)
        .getMapOutputForDirectDiskFetch(any(InputAttemptIdentifier.class), any(Path.class),
            any(TezIndexRecord.class));

    doAnswer(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        return new Path(SHUFFLE_INPUT_FILE_PREFIX + args[0]);
      }
    }).when(spyFetcher).getShuffleInputFileName(anyString(), anyString());

    for (int i = 0; i < host.getPartitionCount(); i++) {
      doAnswer(new Answer<TezIndexRecord>() {
        @Override
        public TezIndexRecord answer(InvocationOnMock invocation) throws Throwable {
          Object[] args = invocation.getArguments();
          String pathComponent = (String) args[0];
          int len = pathComponent.length();
          long p = Long.valueOf(pathComponent.substring(len - 1, len));
          if (p == FIRST_FAILED_ATTEMPT_IDX || p == SECOND_FAILED_ATTEMPT_IDX) {
            throw new IOException("failing to simulate failure case");
          }
          // match with params for copySucceeded below.
          return new TezIndexRecord(p * 10, p * 1000, p * 100);
        }
      }).when(spyFetcher).getIndexRecord(anyString(), eq(host.getPartitionId() + i));
    }

    doNothing().when(scheduler).copySucceeded(any(InputAttemptIdentifier.class), any(MapHost.class),
        anyLong(), anyLong(), anyLong(), any(MapOutput.class), anyBoolean());
    doNothing().when(scheduler).putBackKnownMapOutput(host,
        srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX));
    doNothing().when(scheduler).putBackKnownMapOutput(host,
        srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX));

    spyFetcher.setupLocalDiskFetch(host);

    // should have exactly 3 success and 1 failure.
    for (int i : sucessfulAttemptsIndexes) {
      for (int j = 0; j < host.getPartitionCount(); j++) {
        verifyCopySucceeded(scheduler, host, srcAttempts, i, j);
      }
    }
    verify(scheduler).copyFailed(srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX).expand(0), host, true, false, true);
    verify(scheduler).copyFailed(srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX).expand(0), host, true, false, true);

    verify(spyFetcher).putBackRemainingMapOutputs(host);
    verify(scheduler).putBackKnownMapOutput(host, srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX));
    verify(scheduler).putBackKnownMapOutput(host, srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX));
  }

  @Test(timeout = 5000)
  public void testSetupLocalDiskFetchAutoReduce() throws Exception {
    Configuration conf = new TezConfiguration();
    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);
    Shuffle shuffle = mock(Shuffle.class);
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    when(inputContext.getSourceVertexName()).thenReturn("");

    MapHost host = new MapHost(HOST, PORT, 1, 2);
    FetcherOrderedGrouped fetcher = new FetcherOrderedGrouped(null, scheduler, merger, shuffle, null, false, 0,
        null, conf, true, HOST, PORT, "src vertex", host, ioErrsCounter, wrongLengthErrsCounter, badIdErrsCounter,
        wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
        false, false, true, false);
    FetcherOrderedGrouped spyFetcher = spy(fetcher);


    final List<CompositeInputAttemptIdentifier> srcAttempts = Arrays.asList(
        new CompositeInputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0", host.getPartitionCount()),
        new CompositeInputAttemptIdentifier(1, 2, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1", host.getPartitionCount()),
        new CompositeInputAttemptIdentifier(2, 3, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_2", host.getPartitionCount()),
        new CompositeInputAttemptIdentifier(3, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3", host.getPartitionCount()),
        new CompositeInputAttemptIdentifier(4, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_4", host.getPartitionCount())
    );
    final int FIRST_FAILED_ATTEMPT_IDX = 2;
    final int SECOND_FAILED_ATTEMPT_IDX = 4;
    final int[] sucessfulAttemptsIndexes = { 0, 1, 3 };

    doReturn(srcAttempts).when(scheduler).getMapsForHost(host);
    final ConcurrentMap<ShuffleScheduler.PathPartition, InputAttemptIdentifier> pathToIdentifierMap
        = new ConcurrentHashMap<ShuffleScheduler.PathPartition, InputAttemptIdentifier>();
    for (CompositeInputAttemptIdentifier srcAttempt : srcAttempts) {
      for (int i = 0; i < srcAttempt.getInputIdentifierCount(); i++) {
        ShuffleScheduler.PathPartition pathPartition = new ShuffleScheduler.PathPartition(srcAttempt.getPathComponent(), host.getPartitionId() + i);
        pathToIdentifierMap.put(pathPartition, srcAttempt.expand(i));
      }
    }

    doAnswer(new Answer<InputAttemptIdentifier>() {
        @Override
        public InputAttemptIdentifier answer(InvocationOnMock invocation) throws Throwable {
          Object[] args = invocation.getArguments();
          String path = (String) args[0];
          int reduceId = (int) args[1];
          return pathToIdentifierMap.get(new ShuffleScheduler.PathPartition(path, reduceId));
        }
      }).when(scheduler)
          .getIdentifierForFetchedOutput(any(String.class), any(int.class));

    doAnswer(new Answer<MapOutput>() {
      @Override
      public MapOutput answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        MapOutput mapOutput = mock(MapOutput.class);
        doReturn(MapOutput.Type.DISK_DIRECT).when(mapOutput).getType();
        doReturn(args[0]).when(mapOutput).getAttemptIdentifier();
        return mapOutput;
      }
    }).when(spyFetcher)
        .getMapOutputForDirectDiskFetch(any(InputAttemptIdentifier.class), any(Path.class),
            any(TezIndexRecord.class));

    doAnswer(new Answer<Path>() {
      @Override
      public Path answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        return new Path(SHUFFLE_INPUT_FILE_PREFIX + args[0]);
      }
    }).when(spyFetcher).getShuffleInputFileName(anyString(), anyString());

    for (int i = 0; i < host.getPartitionCount(); i++) {
      doAnswer(new Answer<TezIndexRecord>() {
        @Override
        public TezIndexRecord answer(InvocationOnMock invocation) throws Throwable {
          Object[] args = invocation.getArguments();
          String pathComponent = (String) args[0];
          int len = pathComponent.length();
          long p = Long.valueOf(pathComponent.substring(len - 1, len));

          if (pathComponent.equals(srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX).getPathComponent()) ||
              pathComponent.equals(srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX).getPathComponent())) {
            throw new IOException("Thowing exception to simulate failure case");
          }
          // match with params for copySucceeded below.
          return new TezIndexRecord(p * 10, p * 1000, p * 100);
        }
      }).when(spyFetcher).getIndexRecord(anyString(), eq(host.getPartitionId() + i));
    }

    doNothing().when(scheduler).copySucceeded(any(InputAttemptIdentifier.class), any(MapHost.class),
        anyLong(), anyLong(), anyLong(), any(MapOutput.class), anyBoolean());
    doNothing().when(scheduler).putBackKnownMapOutput(host,
        srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX).expand(0));
    doNothing().when(scheduler).putBackKnownMapOutput(host,
        srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX).expand(1));
    doNothing().when(scheduler).putBackKnownMapOutput(host,
        srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX).expand(0));
    doNothing().when(scheduler).putBackKnownMapOutput(host,
        srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX).expand(1));

    spyFetcher.setupLocalDiskFetch(host);

    // should have exactly 3 success and 1 failure.
    for (int i : sucessfulAttemptsIndexes) {
      for (int j = 0; j < host.getPartitionCount(); j++) {
        verifyCopySucceeded(scheduler, host, srcAttempts, i, j);
      }
    }
    verify(scheduler).copyFailed(srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX).expand(0), host, true, false, true);
    verify(scheduler).copyFailed(srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX).expand(1), host, true, false, true);
    verify(scheduler).copyFailed(srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX).expand(0), host, true, false, true);
    verify(scheduler).copyFailed(srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX).expand(1), host, true, false, true);

    verify(spyFetcher).putBackRemainingMapOutputs(host);
    verify(scheduler).putBackKnownMapOutput(host, srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX));
    verify(scheduler).putBackKnownMapOutput(host, srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX));
    verify(scheduler).putBackKnownMapOutput(host, srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX));
    verify(scheduler).putBackKnownMapOutput(host, srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX));
  }

  private void verifyCopySucceeded(ShuffleScheduler scheduler, MapHost host,
      List<CompositeInputAttemptIdentifier> srcAttempts, long p, int j) throws
      IOException {
    // need to verify filename, offsets, sizes wherever they are used.
    InputAttemptIdentifier srcAttemptToMatch = srcAttempts.get((int) p).expand(j);
    String filenameToMatch = SHUFFLE_INPUT_FILE_PREFIX + srcAttemptToMatch.getPathComponent();
    ArgumentCaptor<MapOutput> captureMapOutput = ArgumentCaptor.forClass(MapOutput.class);
    verify(scheduler).copySucceeded(eq(srcAttemptToMatch), eq(host), eq(p * 100),
        eq(p * 1000), anyLong(), captureMapOutput.capture(), anyBoolean());

    // cannot use the equals of MapOutput as it compares id which is private. so doing it manually
    MapOutput m = captureMapOutput.getAllValues().get(0);
    Assert.assertTrue(m.getType().equals(MapOutput.Type.DISK_DIRECT) &&
        m.getAttemptIdentifier().equals(srcAttemptToMatch));
  }

  static class FakeHttpConnection extends HttpConnection {

    public FakeHttpConnection(URL url,
        HttpConnectionParams connParams, String logIdentifier, JobTokenSecretManager jobTokenSecretMgr)
        throws IOException {
      super(url, connParams, logIdentifier, jobTokenSecretMgr);
      this.connection = mock(HttpURLConnection.class);
      when(connection.getResponseCode()).thenReturn(200);
      when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
          .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      when(connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))
          .thenReturn(ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      when(connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH)).thenReturn("");
    }

    public DataInputStream getInputStream() throws IOException {
      byte[] b = new byte[1024];
      ByteArrayInputStream bin = new ByteArrayInputStream(b);
      return new DataInputStream(bin);
    }

    public void validate() throws IOException {
      //empty
    }

    public void cleanup(boolean disconnect) throws IOException {
      LOG.info("HttpConnection cleanup called with disconnect=" + disconnect);
      //ignore
    }
  }

  @Test(timeout = 5000)
  @SuppressWarnings("unchecked")
  public void testWithRetry() throws Exception {
    Configuration conf = new TezConfiguration();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT, 3000);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT, 3000);
    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);

    Shuffle shuffle = mock(Shuffle.class);
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    when(inputContext.getSourceVertexName()).thenReturn("");
    when(inputContext.getApplicationId()).thenReturn(ApplicationId.newInstance(0, 1));

    HttpConnectionParams httpConnectionParams = ShuffleUtils.getHttpConnectionParams(conf);
    final MapHost host = new MapHost(HOST, PORT, 1, 1);
    FetcherOrderedGrouped mockFetcher = new FetcherOrderedGrouped(null, scheduler, merger, shuffle, null, false, 0,
        null, conf, false, HOST, PORT, "src vertex", host, ioErrsCounter, wrongLengthErrsCounter, badIdErrsCounter,
        wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
        false, false, true, false);
    final FetcherOrderedGrouped fetcher = spy(mockFetcher);


    final List<InputAttemptIdentifier> srcAttempts = Arrays.asList(
        new InputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0"),
        new InputAttemptIdentifier(1, 2, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1"),
        new InputAttemptIdentifier(3, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3")
    );
    doReturn(srcAttempts).when(scheduler).getMapsForHost(host);
    doReturn(true).when(fetcher).setupConnection(any(MapHost.class), any(Collection.class));

    URL url = ShuffleUtils.constructInputURL("http://" + HOST + ":" + PORT + "/mapOutput?job=job_123&&reduce=1&map=", srcAttempts, false);
    fetcher.httpConnection = new FakeHttpConnection(url, null, "", null);

    doAnswer(new Answer<MapOutput>() {
      @Override
      public MapOutput answer(InvocationOnMock invocation) throws Throwable {
        Object[] args = invocation.getArguments();
        MapOutput mapOutput = mock(MapOutput.class);
        doReturn(MapOutput.Type.MEMORY).when(mapOutput).getType();
        doReturn(args[0]).when(mapOutput).getAttemptIdentifier();
        return mapOutput;
      }
    }).when(merger).reserve(any(InputAttemptIdentifier.class), anyInt(), anyInt(), anyInt());

    //Create read timeout when reading data
    doAnswer(new Answer<Void>() {
      @Override public Void answer(InvocationOnMock invocation) throws Throwable {
        // Emulate host down for 4 seconds.
        Thread.sleep(4000);
        doReturn(false).when(fetcher).setupConnection(any(MapHost.class), any(Collection.class));
        // Throw IOException when fetcher tries to connect again to the same node
        throw new FetcherReadTimeoutException("creating fetcher socket read timeout exception");
      }
    }).when(fetcher).copyMapOutput(any(MapHost.class), any(DataInputStream.class), any(InputAttemptIdentifier.class));

    try {
      fetcher.copyFromHost(host);
    } catch(IOException e) {
      //ignore
    }
    //setup connection should be called twice (1 for connect and another for retry)
    verify(fetcher, times(2)).setupConnection(any(MapHost.class), any(Collection.class));
    //since copyMapOutput consistently fails, it should call copyFailed once
    verify(scheduler, times(1)).copyFailed(any(InputAttemptIdentifier.class), any(MapHost.class),
          anyBoolean(), anyBoolean(), anyBoolean());

    verify(fetcher, times(1)).putBackRemainingMapOutputs(any(MapHost.class));
    verify(scheduler, times(3)).putBackKnownMapOutput(any(MapHost.class),
        any(InputAttemptIdentifier.class));


    //Verify by stopping the fetcher abruptly
    try {
      fetcher.stopped = false; // flag to indicate fetcher stopped
      fetcher.copyFromHost(host);
      verify(fetcher, times(2)).putBackRemainingMapOutputs(any(MapHost.class));
    } catch(IOException e) {
      //ignore
    }

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAsyncWithException() throws Exception {
    Configuration conf = new TezConfiguration();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT, 3000);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT, 3000);

    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);
    Shuffle shuffle = mock(Shuffle.class);

    TezCounters counters = new TezCounters();
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getCounters()).thenReturn(counters);
    when(inputContext.getSourceVertexName()).thenReturn("");

    JobTokenSecretManager jobMgr = mock(JobTokenSecretManager.class);
    doReturn(new byte[10]).when(jobMgr).computeHash(any(byte[].class));

    HttpConnectionParams httpConnectionParams = ShuffleUtils.getHttpConnectionParams(conf);
    final MapHost host = new MapHost(HOST, PORT, 1, 1);
    FetcherOrderedGrouped mockFetcher =
        new FetcherOrderedGrouped(httpConnectionParams, scheduler, merger, shuffle, jobMgr,
            false, 0,
            null, conf, false, HOST, PORT, "src vertex", host, ioErrsCounter,
            wrongLengthErrsCounter, badIdErrsCounter,
            wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
            true, false, true, false);
    final FetcherOrderedGrouped fetcher = spy(mockFetcher);
    fetcher.remaining = new LinkedHashMap<String, InputAttemptIdentifier>();
    final List<InputAttemptIdentifier> srcAttempts = Arrays.asList(
        new InputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0"),
        new InputAttemptIdentifier(1, 2, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1"),
        new InputAttemptIdentifier(3, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3")
    );
    doReturn(srcAttempts).when(scheduler).getMapsForHost(host);

    try {
      long currentIOErrors = ioErrsCounter.getValue();
      boolean connected = fetcher.setupConnection(host, srcAttempts);
      Assert.assertTrue(connected == false);
      //Ensure that counters are incremented (i.e it followed the exception codepath)
      Assert.assertTrue(ioErrsCounter.getValue() > currentIOErrors);
    } catch (IOException e) {
      fail();
    }
  }

  @Test(timeout = 5000)
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

    Configuration conf = new TezConfiguration();
    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);
    Shuffle shuffle = mock(Shuffle.class);
    MapHost mapHost = new MapHost(HOST, PORT, 0, 1);
    FetcherOrderedGrouped fetcher =
        new FetcherOrderedGrouped(null, scheduler, merger, shuffle, null, false, 0,
            null, conf, false, HOST, PORT, "src vertex", mapHost, ioErrsCounter,
            wrongLengthErrsCounter, badIdErrsCounter,
            wrongMapErrsCounter, connectionErrsCounter, wrongReduceErrsCounter, APP_ID, DAG_ID,
            false, false, true, false);
    fetcher.populateRemainingMap(new LinkedList<InputAttemptIdentifier>(Arrays.asList(srcAttempts)));
    Assert.assertEquals(expectedSrcAttempts.length, fetcher.remaining.size());
    Iterator<Entry<String, InputAttemptIdentifier>> iterator = fetcher.remaining.entrySet().iterator();
    int count = 0;
    while(iterator.hasNext()) {
      String key = iterator.next().getKey();
      Assert.assertTrue(expectedSrcAttempts[count++].toString().compareTo(key) == 0);
    }
  }
}
