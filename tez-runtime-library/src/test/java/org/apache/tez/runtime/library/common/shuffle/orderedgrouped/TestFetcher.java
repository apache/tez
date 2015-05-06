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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.io.DataOutputBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.runtime.api.InputContext;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.exceptions.FetcherReadTimeoutException;
import org.apache.tez.runtime.library.common.shuffle.HttpConnection;
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

  static final Logger LOG = LoggerFactory.getLogger(TestFetcher.class);

  @Test(timeout = 5000)
  public void testLocalFetchModeSetting1() throws Exception {
    Configuration conf = new TezConfiguration();
    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);
    ShuffleClientMetrics metrics = mock(ShuffleClientMetrics.class);
    Shuffle shuffle = mock(Shuffle.class);

    InputContext inputContext = mock(InputContext.class);
    doReturn(new TezCounters()).when(inputContext).getCounters();
    doReturn("src vertex").when(inputContext).getSourceVertexName();

    final boolean ENABLE_LOCAL_FETCH = true;
    final boolean DISABLE_LOCAL_FETCH = false;
    MapHost mapHost = new MapHost(0, HOST + ":" + PORT, "baseurl");
    FetcherOrderedGrouped
        fetcher = new FetcherOrderedGrouped(null, scheduler, merger, metrics, shuffle, null,
        false, 0, null, inputContext, conf, ENABLE_LOCAL_FETCH, HOST, PORT);

    // when local mode is enabled and host and port matches use local fetch
    FetcherOrderedGrouped spyFetcher = spy(fetcher);
    doNothing().when(spyFetcher).setupLocalDiskFetch(mapHost);
    doReturn(mapHost).when(scheduler).getHost();

    spyFetcher.fetchNext();

    verify(spyFetcher, times(1)).setupLocalDiskFetch(mapHost);
    verify(spyFetcher, never()).copyFromHost(any(MapHost.class));

    // if hostname does not match use http
    spyFetcher = spy(fetcher);
    mapHost = new MapHost(0, HOST + "_OTHER" + ":" + PORT, "baseurl");
    doNothing().when(spyFetcher).setupLocalDiskFetch(mapHost);
    doReturn(mapHost).when(scheduler).getHost();

    spyFetcher.fetchNext();

    verify(spyFetcher, never()).setupLocalDiskFetch(any(MapHost.class));
    verify(spyFetcher, times(1)).copyFromHost(mapHost);

    // if port does not match use http
    spyFetcher = spy(fetcher);
    mapHost = new MapHost(0, HOST + ":" + (PORT + 1), "baseurl");
    doNothing().when(spyFetcher).setupLocalDiskFetch(mapHost);
    doReturn(mapHost).when(scheduler).getHost();

    spyFetcher.fetchNext();

    verify(spyFetcher, never()).setupLocalDiskFetch(any(MapHost.class));
    verify(spyFetcher, times(1)).copyFromHost(mapHost);

    //if local fetch is not enabled
    mapHost = new MapHost(0, HOST + ":" + PORT, "baseurl");
    fetcher = new FetcherOrderedGrouped(null, scheduler, merger, metrics, shuffle, null,
        false, 0, null, inputContext, conf, DISABLE_LOCAL_FETCH, HOST, PORT);
    spyFetcher = spy(fetcher);
    doNothing().when(spyFetcher).setupLocalDiskFetch(mapHost);
    doReturn(mapHost).when(scheduler).getHost();

    spyFetcher.fetchNext();

    verify(spyFetcher, never()).setupLocalDiskFetch(any(MapHost.class));
    verify(spyFetcher, times(1)).copyFromHost(mapHost);
  }

  @Test(timeout = 5000)
  public void testSetupLocalDiskFetch() throws Exception {
    Configuration conf = new TezConfiguration();
    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);
    ShuffleClientMetrics metrics = mock(ShuffleClientMetrics.class);
    Shuffle shuffle = mock(Shuffle.class);
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    when(inputContext.getSourceVertexName()).thenReturn("");

    FetcherOrderedGrouped
        fetcher = new FetcherOrderedGrouped(null, scheduler, merger, metrics, shuffle, null,
        false, 0, null, inputContext, conf, true, HOST, PORT);
    FetcherOrderedGrouped spyFetcher = spy(fetcher);

    MapHost host = new MapHost(1, HOST + ":" + PORT,
        "http://" + HOST + ":" + PORT + "/mapOutput?job=job_123&&reduce=1&map=");
    List<InputAttemptIdentifier> srcAttempts = Arrays.asList(
        new InputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0"),
        new InputAttemptIdentifier(1, 2, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1"),
        new InputAttemptIdentifier(2, 3, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_2"),
        new InputAttemptIdentifier(3, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3"),
        new InputAttemptIdentifier(4, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_4")
    );
    final int FIRST_FAILED_ATTEMPT_IDX = 2;
    final int SECOND_FAILED_ATTEMPT_IDX = 4;
    final int[] sucessfulAttemptsIndexes = { 0, 1, 3 };

    doReturn(srcAttempts).when(scheduler).getMapsForHost(host);

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
    }).when(spyFetcher).getIndexRecord(anyString(), eq(host.getPartitionId()));

    doNothing().when(scheduler).copySucceeded(any(InputAttemptIdentifier.class), any(MapHost.class),
        anyLong(), anyLong(), anyLong(), any(MapOutput.class));
    doNothing().when(scheduler).putBackKnownMapOutput(host,
        srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX));
    doNothing().when(scheduler).putBackKnownMapOutput(host,
        srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX));

    spyFetcher.setupLocalDiskFetch(host);

    // should have exactly 3 success and 1 failure.
    for (int i : sucessfulAttemptsIndexes) {
      verifyCopySucceeded(scheduler, host, srcAttempts, i);
    }
    verify(scheduler).copyFailed(srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX), host, true, false);
    verify(scheduler).copyFailed(srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX), host, true, false);

    verify(metrics, times(3)).successFetch();
    verify(metrics, times(2)).failedFetch();

    verify(spyFetcher).putBackRemainingMapOutputs(host);
    verify(scheduler).putBackKnownMapOutput(host, srcAttempts.get(FIRST_FAILED_ATTEMPT_IDX));
    verify(scheduler).putBackKnownMapOutput(host, srcAttempts.get(SECOND_FAILED_ATTEMPT_IDX));
  }

  private void verifyCopySucceeded(ShuffleScheduler scheduler, MapHost host,
      List<InputAttemptIdentifier> srcAttempts, long p) throws
      IOException {
    // need to verify filename, offsets, sizes wherever they are used.
    InputAttemptIdentifier srcAttemptToMatch = srcAttempts.get((int) p);
    String filenameToMatch = SHUFFLE_INPUT_FILE_PREFIX + srcAttemptToMatch.getPathComponent();
    ArgumentCaptor<MapOutput> captureMapOutput = ArgumentCaptor.forClass(MapOutput.class);
    verify(scheduler).copySucceeded(eq(srcAttemptToMatch), eq(host), eq(p * 100),
        eq(p * 1000), anyLong(), captureMapOutput.capture());

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
  public void testWithRetry() throws Exception {
    Configuration conf = new TezConfiguration();
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT, 3000);
    conf.setInt(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT, 3000);
    ShuffleScheduler scheduler = mock(ShuffleScheduler.class);
    MergeManager merger = mock(MergeManager.class);

    ShuffleClientMetrics metrics = mock(ShuffleClientMetrics.class);
    Shuffle shuffle = mock(Shuffle.class);
    InputContext inputContext = mock(InputContext.class);
    when(inputContext.getCounters()).thenReturn(new TezCounters());
    when(inputContext.getSourceVertexName()).thenReturn("");

    HttpConnection.HttpConnectionParams httpConnectionParams =
        ShuffleUtils.constructHttpShuffleConnectionParams(conf);
    FetcherOrderedGrouped mockFetcher =
        new FetcherOrderedGrouped(httpConnectionParams, scheduler, merger, metrics, shuffle, null,
            false, 0, null, inputContext, conf, false, HOST, PORT);
    final FetcherOrderedGrouped fetcher = spy(mockFetcher);

    final MapHost host = new MapHost(1, HOST + ":" + PORT,
        "http://" + HOST + ":" + PORT + "/mapOutput?job=job_123&&reduce=1&map=");
    final List<InputAttemptIdentifier> srcAttempts = Arrays.asList(
        new InputAttemptIdentifier(0, 1, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_0"),
        new InputAttemptIdentifier(1, 2, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_1"),
        new InputAttemptIdentifier(3, 4, InputAttemptIdentifier.PATH_PREFIX + "pathComponent_3")
    );
    doReturn(srcAttempts).when(scheduler).getMapsForHost(host);
    doReturn(true).when(fetcher).setupConnection(host, srcAttempts);

    URL url = ShuffleUtils.constructInputURL(host.getBaseUrl(), srcAttempts, false);
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
        doReturn(false).when(fetcher).setupConnection(host, srcAttempts);

        // Throw IOException when fetcher tries to connect again to the same node
        throw new FetcherReadTimeoutException("creating fetcher socket read timeout exception");
      }
    }).when(fetcher).copyMapOutput(any(MapHost.class), any(DataInputStream.class));

    try {
      fetcher.copyFromHost(host);
    } catch(IOException e) {
      //ignore
    }
    //setup connection should be called twice (1 for connect and another for retry)
    verify(fetcher, times(2)).setupConnection(any(MapHost.class), anyList());
    //since copyMapOutput consistently fails, it should call copyFailed once
    verify(scheduler, times(1)).copyFailed(any(InputAttemptIdentifier.class), any(MapHost.class),
          anyBoolean(), anyBoolean());

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

}
