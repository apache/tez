/*
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
package org.apache.tez.auxservices;

//import static org.apache.hadoop.test.MetricsAsserts.assertCounter;
//import static org.apache.hadoop.test.MetricsAsserts.assertGauge;
//import static org.apache.hadoop.test.MetricsAsserts.getMetrics;
import org.apache.hadoop.util.DiskChecker.DiskErrorException;
import static org.junit.Assert.assertTrue;
import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.Checksum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.runtime.library.common.shuffle.ShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.api.ShuffleHandlerError;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.impl.MetricsSystemImpl;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.service.ServiceStateException;
import org.apache.hadoop.util.PureJavaCrc32;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.records.Version;
import org.apache.tez.runtime.library.common.sort.impl.TezIndexRecord;
import org.apache.tez.runtime.library.common.sort.impl.TezSpillRecord;

import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.AbstractChannel;
import io.netty.handler.codec.http.DefaultFullHttpRequest;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import io.netty.handler.codec.http.HttpMethod;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestShuffleHandler {
  static final long MiB = 1024 * 1024;
  private static final Logger LOG = LoggerFactory.getLogger(TestShuffleHandler.class);
  private static final File TEST_DIR = new File(System.getProperty("test.build.data"),
      TestShuffleHandler.class.getName()).getAbsoluteFile();
  private static final String HADOOP_TMP_DIR = "hadoop.tmp.dir";
  class MockShuffleHandler extends org.apache.tez.auxservices.ShuffleHandler {
    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      return new Shuffle(conf) {
        @Override
        protected void verifyRequest(String appid, ChannelHandlerContext ctx,
            HttpRequest request, HttpResponse response, URL requestUri)
            throws IOException {
        }
        @Override
        protected MapOutputInfo getMapOutputInfo(String dagId, String mapId,
                                                 Range reduceRange, String jobId,
                                                 String user)
            throws IOException {
          // Do nothing.
          return null;
        }
        @Override
        protected void populateHeaders(List<String> mapIds, String jobId,
                                       String dagId, String user, Range reduceRange,
                                       HttpResponse response,
                                       boolean keepAliveParam,
                                       Map<String, MapOutputInfo> infoMap) throws IOException {
          // Do nothing.
        }
        @Override
        protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
                                              Channel ch, String user, String mapId, Range reduceRange,
                                              MapOutputInfo info) throws IOException {

          ShuffleHeader header =
              new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
          DataOutputBuffer dob = new DataOutputBuffer();
          header.write(dob);
          ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          dob = new DataOutputBuffer();
          for (int i = 0; i < 100; ++i) {
            header.write(dob);
          }
          return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
        }
      };
    }
  }

  private static class MockShuffleHandler2 extends org.apache.tez.auxservices.ShuffleHandler {
    boolean socketKeepAlive = false;

    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      return new Shuffle(conf) {
        @Override
        protected void verifyRequest(String appid, ChannelHandlerContext ctx,
            HttpRequest request, HttpResponse response, URL requestUri)
            throws IOException {
          SocketChannel channel = (SocketChannel)(ctx.channel());
          socketKeepAlive = channel.config().isKeepAlive();
        }
      };
    }

    protected boolean isSocketKeepAlive() {
      return socketKeepAlive;
    }
  }

  class MockShuffleHandlerWithFatalDiskError extends org.apache.tez.auxservices.ShuffleHandler {
    public static final String MESSAGE =
        "Could not find application_1234/240/output/attempt_1234_0/file.out.index";

    private JobTokenSecretManager secretManager =
        new JobTokenSecretManager(JobTokenSecretManager.createSecretKey(getSecret().getBytes()));

    protected JobTokenSecretManager getSecretManager(){
      return secretManager;
    }

    @Override
    protected Shuffle getShuffle(final Configuration conf) {
      return new Shuffle(conf) {
        @Override
        protected void verifyRequest(String appid, ChannelHandlerContext ctx, HttpRequest request,
            HttpResponse response, URL requestUri) throws IOException {
          super.verifyRequest(appid, ctx, request, response, requestUri);
        }

        @Override
        protected MapOutputInfo getMapOutputInfo(String dagId, String mapId, Range reduceRange,
            String jobId, String user) {
          return null;
        }

        @Override
        protected void populateHeaders(List<String> mapIds, String jobId, String dagId, String user,
            Range reduceRange, HttpResponse response, boolean keepAliveParam,
            Map<String, MapOutputInfo> infoMap) throws IOException {
          throw new DiskErrorException(MESSAGE);
        }

        @Override
        protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx, Channel ch, String user,
            String mapId, Range reduceRange, MapOutputInfo info) throws IOException {
          return null;
        }
      };
    }

    public String getSecret() {
      return "secret";
    }
  }

  /**
   * Test the validation of ShuffleHandler's meta-data's serialization and
   * de-serialization.
   *
   * @throws Exception exception
   */
  @Test (timeout = 10000)
  public void testSerializeMeta()  throws Exception {
    assertEquals(1, ShuffleHandler.deserializeMetaData(
        ShuffleHandler.serializeMetaData(1)));
    assertEquals(-1, ShuffleHandler.deserializeMetaData(
        ShuffleHandler.serializeMetaData(-1)));
    assertEquals(8080, ShuffleHandler.deserializeMetaData(
        ShuffleHandler.serializeMetaData(8080)));
  }

  /**
   * Validate shuffle connection and input/output metrics.
   *
   * @throws Exception exception
   */
  @Test (timeout = 10000)
  public void testShuffleMetrics() throws Exception {
    MetricsSystem ms = new MetricsSystemImpl();
    ShuffleHandler sh = new ShuffleHandler(ms);
    ChannelFuture cf = mock(ChannelFuture.class);
    when(cf.isSuccess()).thenReturn(true, false);

    sh.metrics.shuffleConnections.incr();
    sh.metrics.shuffleOutputBytes.incr(1*MiB);
    sh.metrics.shuffleConnections.incr();
    sh.metrics.shuffleOutputBytes.incr(2*MiB);

    checkShuffleMetrics(ms, 3*MiB, 0 , 0, 2);

    sh.metrics.operationComplete(cf);
    sh.metrics.operationComplete(cf);

    checkShuffleMetrics(ms, 3*MiB, 1, 1, 0);
    sh.close();
  }

  static void checkShuffleMetrics(MetricsSystem ms, long bytes, int failed,
                                  int succeeded, int connections) {
    /* TODO
    MetricsSource source = ms.getSource("ShuffleMetrics");
    MetricsRecordBuilder rb = getMetrics(source);
    assertCounter("ShuffleOutputBytes", bytes, rb);
    assertCounter("ShuffleOutputsFailed", failed, rb);
    assertCounter("ShuffleOutputsOK", succeeded, rb);
    assertGauge("ShuffleConnections", connections, rb);
    */
  }

  /**
   * Verify client prematurely closing a connection.
   *
   * @throws Exception exception.
   */
  @Test (timeout = 10000)
  public void testClientClosesConnection() throws Exception {
    final AtomicBoolean failureEncountered = new AtomicBoolean(false);
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String dagId, String mapId,
                                                   Range reduceRange, String jobId,
                                                   String user)
              throws IOException {
            return null;
          }
          @Override
          protected void populateHeaders(List<String> mapIds, String jobId,
                                         String dagId, String user, Range reduceRange,
                                         HttpResponse response,
                                         boolean keepAliveParam,
                                         Map<String, MapOutputInfo> infoMap) throws IOException {
            // Only set response headers and skip everything else
            // send some dummy value for content-length
            super.setResponseHeaders(response, keepAliveParam, 100);
          }
          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri)
                  throws IOException {
          }
          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
                                                Channel ch, String user, String mapId, Range reduceRange,
                                                MapOutputInfo info)
                  throws IOException {
            // send a shuffle header and a lot of data down the channel
            // to trigger a broken pipe
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i = 0; i < 100000; ++i) {
              header.write(dob);
            }
            return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx,
              HttpResponseStatus status) {
            if (failureEncountered.compareAndSet(false, true)) {
              ctx.channel().close();
            }
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failureEncountered.compareAndSet(false, true)) {
              ctx.channel().close();
            }
          }
        };
      }
    };
    shuffleHandler.init(conf);
    shuffleHandler.start();

    // simulate a reducer that closes early by reading a single shuffle header
    // then closing the connection
    URL url = new URL("http://127.0.0.1:"
      + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
      + "/mapOutput?job=job_12345_1&dag=1&reduce=1&map=attempt_12345_1_m_1_0");
    HttpURLConnection conn = (HttpURLConnection)url.openConnection();
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    DataInputStream input = new DataInputStream(conn.getInputStream());
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    Assert.assertEquals("close", conn.getHeaderField(HttpHeaders.Names.CONNECTION));
    ShuffleHeader header = new ShuffleHeader();
    header.readFields(input);
    input.close();

    shuffleHandler.close();
    Assert.assertTrue("sendError called when client closed connection",
        !failureEncountered.get());
  }

  static class LastSocketAddress {
    SocketAddress lastAddress;
    void setAddress(SocketAddress lastAddress) {
      this.lastAddress = lastAddress;
    }
    SocketAddress getSocketAddress() {
      return lastAddress;
    }
  }

  @Test(timeout = 10000)
  public void testKeepAlive() throws Exception {
    final AtomicBoolean failureEncountered = new AtomicBoolean(false);
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    // try setting to -ve keep alive timeout.
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, -100);
    final LastSocketAddress lastSocketAddress = new LastSocketAddress();

    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(final Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String dagId, String mapId,
                                                   Range reduceRange, String jobId, String user)
              throws IOException {
            return null;
          }
          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri)
              throws IOException {
          }

          @Override
          protected void populateHeaders(List<String> mapIds, String jobId,
                                         String dagId, String user,
                                         Range reduceRange,
                                         HttpResponse response,
                                         boolean keepAliveParam,
                                         Map<String, MapOutputInfo> infoMap)
              throws IOException {
            // Send some dummy data (populate content length details)
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            dob = new DataOutputBuffer();
            for (int i = 0; i < 100000; ++i) {
              header.write(dob);
            }

            long contentLength = dob.getLength();
            // for testing purpose;
            // disable connectinKeepAliveEnabled if keepAliveParam is available
            if (keepAliveParam) {
              connectionKeepAliveEnabled = false;
            }

            super.setResponseHeaders(response, keepAliveParam, contentLength);
          }

          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
                                                Channel ch, String user, String mapId, Range reduceRange,
                                                MapOutputInfo info) throws IOException {
            lastSocketAddress.setAddress(ch.remoteAddress());

            // send a shuffle header and a lot of data down the channel
            // to trigger a broken pipe
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i = 0; i < 100000; ++i) {
              header.write(dob);
            }
            return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }

          @Override
          protected void sendError(ChannelHandlerContext ctx,
              HttpResponseStatus status) {
            if (failureEncountered.compareAndSet(false, true)) {
              ctx.channel().close();
            }
          }

          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failureEncountered.compareAndSet(false, true)) {
              ctx.channel().close();
            }
          }
        };
      }
    };
    shuffleHandler.init(conf);
    shuffleHandler.start();

    String shuffleBaseURL = "http://127.0.0.1:"
            + shuffleHandler.getConfig().get(
              ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
    URL url =
        new URL(shuffleBaseURL + "/mapOutput?job=job_12345_1&dag=1&reduce=1&"
            + "map=attempt_12345_1_m_1_0");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
      ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
      ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    DataInputStream input = new DataInputStream(conn.getInputStream());
    Assert.assertEquals(HttpHeaders.Values.KEEP_ALIVE,
      conn.getHeaderField(HttpHeaders.Names.CONNECTION));
    Assert.assertEquals("timeout=1",
      conn.getHeaderField(HttpHeaders.Values.KEEP_ALIVE));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    ShuffleHeader header = new ShuffleHeader();
    header.readFields(input);
    byte[] buffer = new byte[1024];
    while (input.read(buffer) != -1) {}
    SocketAddress firstAddress = lastSocketAddress.getSocketAddress();
    input.close();

    // For keepAlive via URL
    url =
        new URL(shuffleBaseURL + "/mapOutput?job=job_12345_1&dag=1&reduce=1&"
            + "map=attempt_12345_1_m_1_0&keepAlive=true");
    conn = (HttpURLConnection) url.openConnection();
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
      ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
      ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    input = new DataInputStream(conn.getInputStream());
    Assert.assertEquals(HttpHeaders.Values.KEEP_ALIVE,
      conn.getHeaderField(HttpHeaders.Names.CONNECTION));
    Assert.assertEquals("timeout=1",
      conn.getHeaderField(HttpHeaders.Values.KEEP_ALIVE));
    Assert.assertEquals(HttpURLConnection.HTTP_OK, conn.getResponseCode());
    header = new ShuffleHeader();
    header.readFields(input);
    input.close();
    SocketAddress secondAddress = lastSocketAddress.getSocketAddress();
    Assert.assertNotNull("Initial shuffle address should not be null", firstAddress);
    Assert.assertNotNull("Keep-Alive shuffle address should not be null", secondAddress);
    Assert.assertEquals("Initial shuffle address and keep-alive shuffle "
        + "address should be the same", firstAddress, secondAddress);
    shuffleHandler.close();
  }

  @Test
  public void testSocketKeepAlive() throws Exception {
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setBoolean(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_ENABLED, true);
    // try setting to -ve keep alive timeout.
    conf.setInt(ShuffleHandler.SHUFFLE_CONNECTION_KEEP_ALIVE_TIME_OUT, -100);
    HttpURLConnection conn = null;
    MockShuffleHandler2 shuffleHandler = new MockShuffleHandler2();
    try {
      shuffleHandler.init(conf);
      shuffleHandler.start();

      String shuffleBaseURL = "http://127.0.0.1:"
              + shuffleHandler.getConfig().get(
                ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
      URL url =
          new URL(shuffleBaseURL + "/mapOutput?job=job_12345_1&dag=1&reduce=1&"
              + "map=attempt_12345_1_m_1_0");
      conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      conn.connect();
      conn.getInputStream();
      Assert.assertTrue("socket should be set KEEP_ALIVE",
          shuffleHandler.isSocketKeepAlive());
    } finally {
      if (conn != null) {
        conn.disconnect();
      }
      shuffleHandler.close();
    }
  }

  /**
   * simulate a reducer that sends an invalid shuffle-header - sometimes a wrong
   * header_name and sometimes a wrong version
   *
   * @throws Exception exception
   */
  @Test (timeout = 10000)
  public void testIncompatibleShuffleVersion() throws Exception {
    final int failureNum = 3;
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    ShuffleHandler shuffleHandler = new ShuffleHandler();
    shuffleHandler.init(conf);
    shuffleHandler.start();

    // simulate a reducer that closes early by reading a single shuffle header
    // then closing the connection
    URL url = new URL("http://127.0.0.1:"
      + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
      + "/mapOutput?job=job_12345_1&&dag=1reduce=1&map=attempt_12345_1_m_1_0");
    for (int i = 0; i < failureNum; ++i) {
      HttpURLConnection conn = (HttpURLConnection)url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          i == 0 ? "mapreduce" : "other");
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          i == 1 ? "1.0.0" : "1.0.1");
      conn.connect();
      Assert.assertEquals(
          HttpURLConnection.HTTP_BAD_REQUEST, conn.getResponseCode());
    }

    shuffleHandler.close();
  }

  /**
   * Validate the limit on number of shuffle connections.
   *
   * @throws Exception exception
   */
  @Test (timeout = 10000)
  public void testMaxConnections() throws Exception {

    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected MapOutputInfo getMapOutputInfo(String dagId, String mapId,
                                                   Range reduceRange, String jobId,
                                                   String user)
              throws IOException {
            // Do nothing.
            return null;
          }
          @Override
          protected void populateHeaders(List<String> mapIds, String jobId,
                                         String dagId, String user, Range reduceRange,
                                         HttpResponse response,
                                         boolean keepAliveParam,
                                         Map<String, MapOutputInfo> infoMap) throws IOException {
            // Do nothing.
          }
          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri)
                  throws IOException {
            // Do nothing.
          }
          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
                                                Channel ch, String user, String mapId, Range reduceRange,
                                                MapOutputInfo info)
                  throws IOException {
            // send a shuffle header and a lot of data down the channel
            // to trigger a broken pipe
            ShuffleHeader header =
                new ShuffleHeader("dummy_header", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
            dob = new DataOutputBuffer();
            for (int i=0; i<100000; ++i) {
              header.write(dob);
            }
            return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }
        };
      }
    };
    shuffleHandler.init(conf);
    shuffleHandler.start();

    // setup connections
    int connAttempts = 3;
    HttpURLConnection conns[] = new HttpURLConnection[connAttempts];

    for (int i = 0; i < connAttempts; i++) {
      String URLstring = "http://127.0.0.1:"
           + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
           + "/mapOutput?job=job_12345_1&dag=1&reduce=1&map=attempt_12345_1_m_"
           + i + "_0";
      URL url = new URL(URLstring);
      conns[i] = (HttpURLConnection)url.openConnection();
      conns[i].setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conns[i].setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    }

    // Try to open numerous connections
    for (int i = 0; i < connAttempts; i++) {
      // connections should be made in a bit relaxed way, otherwise
      // non-synced channelActive method will mess them up
      Thread.sleep(200);

      conns[i].connect();
    }

    //Ensure first connections are okay
    conns[0].getInputStream();
    int rc = conns[0].getResponseCode();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

    conns[1].getInputStream();
    rc = conns[1].getResponseCode();
    Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

    // This connection should be closed because it to above the limit
    try {
      conns[2].getInputStream();
      rc = conns[2].getResponseCode();
      Assert.fail("Expected a SocketException");
    } catch (SocketException se) {
      LOG.info("Expected - connection should not be open");
    } catch (Exception e) {
      Assert.fail("Expected a SocketException");
    }

    shuffleHandler.close();
  }

  /**
   * Validate the ranged fetch works as expected
   */
  @Test(timeout = 10000)
  public void testRangedFetch() throws IOException {
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "simple");
    UserGroupInformation.setConfiguration(conf);
    File absLogDir = new File("target",
        TestShuffleHandler.class.getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absLogDir.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    LOG.info(appId.toString());
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    String reducerIdStart = "0";
    String reducerIdEnd = "1";
    List<File> fileMap = new ArrayList<>();
    createShuffleHandlerFiles(absLogDir, user, appId.toString(), appAttemptId,
        conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {

      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {

          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
                                       HttpRequest request, HttpResponse response, URL requestUri)
              throws IOException {
            // Do nothing.
          }

        };
      }
    };
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<JobTokenIdentifier>("identifier".getBytes(),
              "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler
          .initializeApplication(new ApplicationInitializationContext(user,
              appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
              outputBuffer.getLength())));
      URL url =
          new URL(
              "http://127.0.0.1:"
                  + shuffleHandler.getConfig().get(
                  ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                  + "/mapOutput?job=job_12345_0001&dag=1&reduce=" + reducerIdStart + "-" + reducerIdEnd
                  + "&map=attempt_12345_1_m_1_0");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      conn.connect();
      boolean succeeded = false;
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        int partitionCount = WritableUtils.readVInt(is);
        List<ShuffleHeader> headers = new ArrayList<>(2);
        for (int i = 0; i < partitionCount; i++) {
          ShuffleHeader header = new ShuffleHeader();
          header.readFields(is);
          Assert.assertEquals("Incorrect map id", "attempt_12345_1_m_1_0", header.getMapId());
          Assert.assertEquals("Incorrect reduce id", i, header.getPartition());
          headers.add(header);
        }
        for (ShuffleHeader header: headers) {
          byte[] bytes = new byte[(int)header.getCompressedLength()];
          is.read(bytes);
        }
        succeeded = true;
        // Read one more byte to force EOF
        is.readByte();
        Assert.fail("More fetch bytes that expected in stream");
      } catch (EOFException e) {
        Assert.assertTrue("Failed to copy ranged fetch", succeeded);
      }

    } finally {
      shuffleHandler.close();
      FileUtil.fullyDelete(absLogDir);
    }
  }

  /**
   * Validate the ownership of the map-output files being pulled in. The
   * local-file-system owner of the file should match the user component in the
   *
   * @throws Exception exception
   */
  @Test(timeout = 100000)
  public void testMapFileAccess() throws IOException {
    // This will run only in NativeIO is enabled as SecureIOUtils need it
    assumeTrue(NativeIO.isAvailable());
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "kerberos");
    UserGroupInformation.setConfiguration(conf);
    File absLogDir = new File("target",
        TestShuffleHandler.class.getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absLogDir.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    LOG.info(appId.toString());
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    String reducerId = "0";
    List<File> fileMap = new ArrayList<File>();
    createShuffleHandlerFiles(absLogDir, user, appId.toString(), appAttemptId,
        conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {

      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {

          @Override
          protected void verifyRequest(String appid, ChannelHandlerContext ctx,
              HttpRequest request, HttpResponse response, URL requestUri)
              throws IOException {
            // Do nothing.
          }

        };
      }
    };
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<JobTokenIdentifier>("identifier".getBytes(),
              "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler
        .initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
            outputBuffer.getLength())));
      URL url =
          new URL(
              "http://127.0.0.1:"
                  + shuffleHandler.getConfig().get(
                      ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                  + "/mapOutput?job=job_12345_0001&dag=1&reduce=" + reducerId
                  + "&map=attempt_12345_1_m_1_0");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      conn.connect();
      byte[] byteArr = new byte[10000];
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        is.readFully(byteArr);
      } catch (EOFException e) {
        // ignore
      }
      // Retrieve file owner name
      FileInputStream is = new FileInputStream(fileMap.get(0));
      String owner = NativeIO.POSIX.getFstat(is.getFD()).getOwner();
      is.close();

      String message =
          "Owner '" + owner + "' for path " + fileMap.get(0).getAbsolutePath()
              + " did not match expected owner '" + user + "'";
      Assert.assertTrue((new String(byteArr)).contains(message));
    } finally {
      shuffleHandler.close();
      FileUtil.fullyDelete(absLogDir);
    }
  }

  private static void createShuffleHandlerFiles(File logDir, String user,
      String appId, String appAttemptId, Configuration conf,
      List<File> fileMap) throws IOException {
    String attemptDir =
        StringUtils.join(Path.SEPARATOR,
            new String[] { logDir.getAbsolutePath(),
                ShuffleHandler.USERCACHE, user,
                ShuffleHandler.APPCACHE, appId,"dag_1/" + "output",
                appAttemptId });
    File appAttemptDir = new File(attemptDir);
    appAttemptDir.mkdirs();
    System.out.println(appAttemptDir.getAbsolutePath());
    File indexFile = new File(appAttemptDir, "file.out.index");
    fileMap.add(indexFile);
    createIndexFile(indexFile, conf);
    File mapOutputFile = new File(appAttemptDir, "file.out");
    fileMap.add(mapOutputFile);
    createMapOutputFile(mapOutputFile, conf);
  }

  private static void
    createMapOutputFile(File mapOutputFile, Configuration conf)
          throws IOException {
    FileOutputStream out = new FileOutputStream(mapOutputFile);
    out.write("Creating new dummy map output file. Used only for testing"
        .getBytes());
    out.flush();
    out.close();
  }

  private static void createIndexFile(File indexFile, Configuration conf)
      throws IOException {
    if (indexFile.exists()) {
      System.out.println("Deleting existing file");
      indexFile.delete();
    }
    Checksum crc = new PureJavaCrc32();
    TezSpillRecord tezSpillRecord = new TezSpillRecord(2);
    tezSpillRecord.putIndex(new TezIndexRecord(0, 10, 10), 0);
    tezSpillRecord.putIndex(new TezIndexRecord(10, 10, 10), 1);
    tezSpillRecord.writeToFile(new Path(indexFile.getAbsolutePath()), conf,
        FileSystem.getLocal(conf).getRaw(), crc);
  }

  @Test
  public void testRecovery() throws IOException {
    final String user = "someuser";
    final ApplicationId appId = ApplicationId.newInstance(12345, 1);
    final File tmpDir = new File(System.getProperty("test.build.data",
        System.getProperty("java.io.tmpdir")),
        TestShuffleHandler.class.getName());
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    ShuffleHandler shuffle = new ShuffleHandler();
    // emulate aux services startup with recovery enabled
    shuffle.setRecoveryPath(new Path(tmpDir.toString()));
    tmpDir.mkdirs();
    try {
      shuffle.init(conf);
      shuffle.start();

      // setup a shuffle token for an application
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>(
          "identifier".getBytes(), "password".getBytes(), new Text(user),
          new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffle.initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
            outputBuffer.getLength())));

      // verify we are authorized to shuffle
      int rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandler();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

      // shutdown app and verify access is lost
      shuffle.stopApplication(new ApplicationTerminationContext(appId));
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandler();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we still don't have access
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_UNAUTHORIZED, rc);
    } finally {
      if (shuffle != null) {
        shuffle.close();
      }
      FileUtil.fullyDelete(tmpDir);
    }
  }

  @Test
  public void testRecoveryFromOtherVersions() throws IOException {
    final String user = "someuser";
    final ApplicationId appId = ApplicationId.newInstance(12345, 1);
    final File tmpDir = new File(System.getProperty("test.build.data",
        System.getProperty("java.io.tmpdir")),
        TestShuffleHandler.class.getName());
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    ShuffleHandler shuffle = new ShuffleHandler();
    // emulate aux services startup with recovery enabled
    shuffle.setRecoveryPath(new Path(tmpDir.toString()));
    tmpDir.mkdirs();
    try {
      shuffle.init(conf);
      shuffle.start();

      // setup a shuffle token for an application
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt = new Token<JobTokenIdentifier>(
          "identifier".getBytes(), "password".getBytes(), new Text(user),
          new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffle.initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
              outputBuffer.getLength())));

      // verify we are authorized to shuffle
      int rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart
      shuffle.close();
      shuffle = new ShuffleHandler();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();

      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);
      Version version = Version.newInstance(1, 0);
      Assert.assertEquals(version, shuffle.getCurrentVersion());

      // emulate shuffle handler restart with compatible version
      Version version11 = Version.newInstance(1, 1);
      // update version info before close shuffle
      shuffle.storeVersion(version11);
      Assert.assertEquals(version11, shuffle.loadVersion());
      shuffle.close();
      shuffle = new ShuffleHandler();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
      shuffle.start();
      // shuffle version will be override by CURRENT_VERSION_INFO after restart
      // successfully.
      Assert.assertEquals(version, shuffle.loadVersion());
      // verify we are still authorized to shuffle to the old application
      rc = getShuffleResponseCode(shuffle, jt);
      Assert.assertEquals(HttpURLConnection.HTTP_OK, rc);

      // emulate shuffle handler restart with incompatible version
      Version version21 = Version.newInstance(2, 1);
      shuffle.storeVersion(version21);
      Assert.assertEquals(version21, shuffle.loadVersion());
      shuffle.close();
      shuffle = new ShuffleHandler();
      shuffle.setRecoveryPath(new Path(tmpDir.toString()));
      shuffle.init(conf);
    
      try {
        shuffle.start();
        Assert.fail("Incompatible version, should expect fail here.");
      } catch (ServiceStateException e) {
        Assert.assertTrue("Exception message mismatch",
        e.getMessage().contains("Incompatible version for state DB schema:"));
      }

    } finally {
      if (shuffle != null) {
        shuffle.close();
      }
      FileUtil.fullyDelete(tmpDir);
    }
  }

  private static int getShuffleResponseCode(ShuffleHandler shuffle,
      Token<JobTokenIdentifier> jt) throws IOException {
    URL url = new URL("http://127.0.0.1:"
        + shuffle.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
        + "/mapOutput?job=job_12345_0001&dag=1&reduce=0" +
        "&map=attempt_12345_1_m_1_0");
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    String encHash = SecureShuffleUtils.hashFromString(
        SecureShuffleUtils.buildMsgFrom(url),
        new JobTokenSecretManager(JobTokenSecretManager.createSecretKey(jt.getPassword())));
    conn.addRequestProperty(
        SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    conn.connect();
    int rc = conn.getResponseCode();
    conn.disconnect();
    return rc;
  }

  @Test(timeout = 100000)
  public void testGetMapOutputInfo() throws Exception {
    final AtomicBoolean failureEncountered = new AtomicBoolean(false);
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "simple");
    UserGroupInformation.setConfiguration(conf);
    File absLogDir = new File("target", TestShuffleHandler.class.
        getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absLogDir.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    String reducerId = "0";
    List<File> fileMap = new ArrayList<File>();
    createShuffleHandlerFiles(absLogDir, user, appId.toString(), appAttemptId,
        conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected void populateHeaders(List<String> mapIds,
                                         String outputBaseStr, String dagId, String user, Range reduceRange,
                                         HttpResponse response,
                                         boolean keepAliveParam, Map<String, MapOutputInfo> infoMap)
              throws IOException {
            // Only set response headers and skip everything else
            // send some dummy value for content-length
            super.setResponseHeaders(response, keepAliveParam, 100);
          }
          @Override
          protected void verifyRequest(String appid,
              ChannelHandlerContext ctx, HttpRequest request,
              HttpResponse response, URL requestUri) throws IOException {
            // Do nothing.
          }
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
              HttpResponseStatus status) {
            if (failureEncountered.compareAndSet(false, true)) {
              ctx.channel().close();
            }
          }
          @Override
          protected ChannelFuture sendMapOutput(ChannelHandlerContext ctx,
                                                Channel ch, String user, String mapId, Range reduceRange,
                                                MapOutputInfo info) throws IOException {
            // send a shuffle header
            ShuffleHeader header =
                new ShuffleHeader("attempt_12345_1_m_1_0", 5678, 5678, 1);
            DataOutputBuffer dob = new DataOutputBuffer();
            header.write(dob);
            return ch.writeAndFlush(wrappedBuffer(dob.getData(), 0, dob.getLength()));
          }
        };
      }
    };
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<JobTokenIdentifier>("identifier".getBytes(),
          "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler
          .initializeApplication(new ApplicationInitializationContext(user,
          appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
          outputBuffer.getLength())));
      URL url =
          new URL(
              "http://127.0.0.1:"
                  + shuffleHandler.getConfig().get(
                      ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                  + "/mapOutput?job=job_12345_0001&dag=1&reduce=" + reducerId
                  + "&map=attempt_12345_1_m_1_0");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      conn.connect();
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        ShuffleHeader header = new ShuffleHeader();
        header.readFields(is);
        is.close();
      } catch (EOFException e) {
        // ignore
      }
      Assert.assertEquals("sendError called due to shuffle error",
          false, failureEncountered.get());
    } finally {
      shuffleHandler.close();
      FileUtil.fullyDelete(absLogDir);
    }
  }

  @Test(timeout = 5000)
  public void testDagDelete() throws Exception {
    final AtomicBoolean failureEncountered = new AtomicBoolean(false);
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "simple");
    UserGroupInformation.setConfiguration(conf);
    File absLogDir = new File("target", TestShuffleHandler.class.
        getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absLogDir.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    List<File> fileMap = new ArrayList<File>();
    createShuffleHandlerFiles(absLogDir, user, appId.toString(), appAttemptId,
        conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
                                   HttpResponseStatus status) {
            if (failureEncountered.compareAndSet(false, true)) {
              ctx.channel().close();
            }
          }
        };
      }
    };
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<JobTokenIdentifier>("identifier".getBytes(),
              "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler
          .initializeApplication(new ApplicationInitializationContext(user,
              appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
              outputBuffer.getLength())));
      URL url =
          new URL(
              "http://127.0.0.1:"
                  + shuffleHandler.getConfig().get(
                  ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                  + "/mapOutput?dagAction=delete&job=job_12345_0001&dag=1");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      String dagDirStr =
          StringUtils.join(Path.SEPARATOR,
              new String[] { absLogDir.getAbsolutePath(),
                  ShuffleHandler.USERCACHE, user,
                  ShuffleHandler.APPCACHE, appId.toString(),"dag_1/"});
      File dagDir = new File(dagDirStr);
      Assert.assertTrue("Dag Directory does not exist!", dagDir.exists());
      conn.connect();
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        is.close();
        Assert.assertFalse("Dag Directory was not deleted!", dagDir.exists());
      } catch (EOFException e) {
        // ignore
      }
      Assert.assertEquals("sendError called due to shuffle error",
          false, failureEncountered.get());
    } finally {
      shuffleHandler.close();
      FileUtil.fullyDelete(absLogDir);
    }
  }

  @Test
  public void testVertexShuffleDelete() throws Exception {
    final ArrayList<Throwable> failures = new ArrayList<Throwable>(1);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
            "simple");
    UserGroupInformation.setConfiguration(conf);
    File absLogDir = new File("target", TestShuffleHandler.class.
            getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absLogDir.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345L, 1);
    String appAttemptId = "attempt_12345_0001_1_00_000000_0_10003_0";
    String user = "randomUser";
    List<File> fileMap = new ArrayList<File>();
    String vertexDirStr = StringUtils.join(Path.SEPARATOR, new String[] { absLogDir.getAbsolutePath(),
        ShuffleHandler.USERCACHE, user, ShuffleHandler.APPCACHE, appId.toString(), "dag_1/output/" + appAttemptId});
    File vertexDir = new File(vertexDirStr);
    Assert.assertFalse("vertex directory should not be present", vertexDir.exists());
    createShuffleHandlerFiles(absLogDir, user, appId.toString(), appAttemptId,
            conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
                                   HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error(message));
              ctx.channel().close();
            }
          }
        };
      }
    };
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
              new Token<JobTokenIdentifier>("identifier".getBytes(),
                      "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler
              .initializeApplication(new ApplicationInitializationContext(user,
                      appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
                      outputBuffer.getLength())));
      URL url =
              new URL(
                      "http://127.0.0.1:"
                              + shuffleHandler.getConfig().get(
                              ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                              + "/mapOutput?vertexAction=delete&job=job_12345_0001&dag=1&vertex=00");
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
              ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
              ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      Assert.assertTrue("Attempt Directory does not exist!", vertexDir.exists());
      conn.connect();
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        is.close();
        Assert.assertFalse("Vertex Directory was not deleted", vertexDir.exists());
      } catch (EOFException e) {
        fail("Encountered Exception!" + e.getMessage());
      }
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(absLogDir);
    }
  }

  @Test(timeout = 5000)
  public void testFailedTaskAttemptDelete() throws Exception {
    final ArrayList<Throwable> failures = new ArrayList<Throwable>(1);
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.MAX_SHUFFLE_CONNECTIONS, 3);
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    conf.set(CommonConfigurationKeysPublic.HADOOP_SECURITY_AUTHENTICATION,
        "simple");
    UserGroupInformation.setConfiguration(conf);
    File absLogDir = new File("target", TestShuffleHandler.class.
        getSimpleName() + "LocDir").getAbsoluteFile();
    conf.set(YarnConfiguration.NM_LOCAL_DIRS, absLogDir.getAbsolutePath());
    ApplicationId appId = ApplicationId.newInstance(12345, 1);
    String appAttemptId = "attempt_12345_1_m_1_0";
    String user = "randomUser";
    List<File> fileMap = new ArrayList<File>();
    String taskAttemptDirStr =
            StringUtils.join(Path.SEPARATOR,
                    new String[] {absLogDir.getAbsolutePath(),
                            ShuffleHandler.USERCACHE, user,
                            ShuffleHandler.APPCACHE, appId.toString(), "dag_1/output/", appAttemptId});
    File taskAttemptDir = new File(taskAttemptDirStr);
    Assert.assertFalse("Task Attempt Directory should not exist", taskAttemptDir.exists());
    createShuffleHandlerFiles(absLogDir, user, appId.toString(), appAttemptId,
        conf, fileMap);
    ShuffleHandler shuffleHandler = new ShuffleHandler() {
      @Override
      protected Shuffle getShuffle(Configuration conf) {
        // replace the shuffle handler with one stubbed for testing
        return new Shuffle(conf) {
          @Override
          protected void sendError(ChannelHandlerContext ctx, String message,
                                   HttpResponseStatus status) {
            if (failures.size() == 0) {
              failures.add(new Error(message));
              ctx.channel().close();
            }
          }
        };
      }
    };
    shuffleHandler.init(conf);
    try {
      shuffleHandler.start();
      DataOutputBuffer outputBuffer = new DataOutputBuffer();
      outputBuffer.reset();
      Token<JobTokenIdentifier> jt =
          new Token<JobTokenIdentifier>("identifier".getBytes(),
              "password".getBytes(), new Text(user), new Text("shuffleService"));
      jt.write(outputBuffer);
      shuffleHandler
          .initializeApplication(new ApplicationInitializationContext(user,
              appId, ByteBuffer.wrap(outputBuffer.getData(), 0,
              outputBuffer.getLength())));
      URL url =
          new URL(
              "http://127.0.0.1:"
                  + shuffleHandler.getConfig().get(
                  ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY)
                  + "/mapOutput?taskAttemptAction=delete&job=job_12345_0001&dag=1&map=" + appAttemptId);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      conn.setRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      Assert.assertTrue("Task Attempt Directory does not exist!", taskAttemptDir.exists());
      conn.connect();
      try {
        DataInputStream is = new DataInputStream(conn.getInputStream());
        is.close();
        Assert.assertFalse("Task Attempt file was not deleted!", taskAttemptDir.exists());
      } catch (EOFException e) {
        // ignore
      }
      Assert.assertEquals("sendError called due to shuffle error",
          0, failures.size());
    } finally {
      shuffleHandler.stop();
      FileUtil.fullyDelete(absLogDir);
    }
  }

  @Test(timeout = 4000)
  public void testSendMapCount() throws Exception {
    final List<ShuffleHandler.ReduceMapFileCount> listenerList =
        new ArrayList<ShuffleHandler.ReduceMapFileCount>();

    final ChannelHandlerContext mockCtx =
        mock(ChannelHandlerContext.class);
    final Channel mockCh = mock(AbstractChannel.class);
    final ChannelPipeline mockPipeline = mock(ChannelPipeline.class);

    // Mock HttpRequest and ChannelFuture
    final FullHttpRequest httpRequest = createHttpRequest();
    final ChannelFuture mockFuture = createMockChannelFuture(mockCh,
        listenerList);
    final ShuffleHandler.TimeoutHandler timerHandler =
        new ShuffleHandler.TimeoutHandler();

    // Mock Netty Channel Context and Channel behavior
    doReturn(mockCh).when(mockCtx).channel();
    when(mockCh.pipeline()).thenReturn(mockPipeline);
    when(mockPipeline.get(any(String.class))).thenReturn(timerHandler);
    when(mockCtx.channel()).thenReturn(mockCh);
    doReturn(mockFuture).when(mockCh).writeAndFlush(any());
    when(mockCh.writeAndFlush(Object.class)).thenReturn(mockFuture);

    final ShuffleHandler sh = new MockShuffleHandler();
    Configuration conf = new Configuration();
    conf.set(HADOOP_TMP_DIR, TEST_DIR.getAbsolutePath());
    // The Shuffle handler port associated with the service is bound to but not used.
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    sh.init(conf);
    sh.start();
    int maxOpenFiles =conf.getInt(ShuffleHandler.SHUFFLE_MAX_SESSION_OPEN_FILES,
        ShuffleHandler.DEFAULT_SHUFFLE_MAX_SESSION_OPEN_FILES);
    sh.getShuffle(conf).channelRead(mockCtx, httpRequest);
    assertTrue("Number of Open files should not exceed the configured " +
            "value!-Not Expected",
        listenerList.size() <= maxOpenFiles);
    while(!listenerList.isEmpty()) {
      listenerList.remove(0).operationComplete(mockFuture);
      assertTrue("Number of Open files should not exceed the configured " +
              "value!-Not Expected",
          listenerList.size() <= maxOpenFiles);
    }
    sh.close();
  }

  @Test
  public void testShuffleHandlerSendsDiskError() throws Exception {
    Configuration conf = new Configuration();
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);

    DataInputStream input = null;
    MockShuffleHandlerWithFatalDiskError shuffleHandler =
        new MockShuffleHandlerWithFatalDiskError();
    try {
      shuffleHandler.init(conf);
      shuffleHandler.start();

      String shuffleBaseURL = "http://127.0.0.1:"
          + shuffleHandler.getConfig().get(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY);
      URL url = new URL(
          shuffleBaseURL + "/mapOutput?job=job_12345_1&dag=1&reduce=1&map=attempt_12345_1_m_1_0");
      shuffleHandler.secretManager.addTokenForJob("job_12345_1",
          new Token<>("id".getBytes(), shuffleHandler.getSecret().getBytes(), null, null));

      HttpConnectionParams httpConnectionParams = ShuffleUtils.getHttpConnectionParams(conf);
      BaseHttpConnection httpConnection = ShuffleUtils.getHttpConnection(true, url,
          httpConnectionParams, "testFetcher", shuffleHandler.secretManager);

      boolean connectSucceeded = httpConnection.connect();
      Assert.assertTrue(connectSucceeded);

      input = httpConnection.getInputStream();
      httpConnection.validate();

      ShuffleHeader header = new ShuffleHeader();
      header.readFields(input);

      // message is encoded in the shuffle header, and can be checked by fetchers
      Assert.assertEquals(
          ShuffleHandlerError.DISK_ERROR_EXCEPTION + ": " + MockShuffleHandlerWithFatalDiskError.MESSAGE,
          header.getMapId());
      Assert.assertEquals(-1, header.getCompressedLength());
      Assert.assertEquals(-1, header.getUncompressedLength());
      Assert.assertEquals(-1, header.getPartition());
    } finally {
      if (input != null) {
        input.close();
      }
      shuffleHandler.close();
    }
  }

  public ChannelFuture createMockChannelFuture(Channel mockCh,
      final List<ShuffleHandler.ReduceMapFileCount> listenerList) {
    final ChannelFuture mockFuture = mock(ChannelFuture.class);
    when(mockFuture.channel()).thenReturn(mockCh);
    doReturn(true).when(mockFuture).isSuccess();
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        //Add ReduceMapFileCount listener to a list
        if (invocation.getArguments()[0].getClass() ==
            ShuffleHandler.ReduceMapFileCount.class)
          listenerList.add((ShuffleHandler.ReduceMapFileCount)
              invocation.getArguments()[0]);
        return null;
      }
    }).when(mockFuture).addListener(any(
        ShuffleHandler.ReduceMapFileCount.class));
    return mockFuture;
  }

  public FullHttpRequest createHttpRequest() {
    String uri = "/mapOutput?job=job_12345_1&dag=1&reduce=1";
    for (int i = 0; i < 100; i++) {
      uri = uri.concat("&map=attempt_12345_1_m_" + i + "_0");
    }
    return new DefaultFullHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.GET, uri);
  }

  @Test
  public void testConfigPortStatic() throws Exception {
    Random rand = new Random();
    int port = rand.nextInt(10) + 50000;
    Configuration conf = new Configuration();
    // provide a port for ShuffleHandler
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, port);
    MockShuffleHandler2 shuffleHandler = new MockShuffleHandler2();
    shuffleHandler.serviceInit(conf);
    try {
      shuffleHandler.serviceStart();
      Assert.assertEquals(port, shuffleHandler.getPort());
    } finally {
      shuffleHandler.stop();
    }
  }

  @Test
  public void testConfigPortDynamic() throws Exception {
    Configuration conf = new Configuration();
    // 0 as config, should be dynamically chosen by netty
    conf.setInt(ShuffleHandler.SHUFFLE_PORT_CONFIG_KEY, 0);
    MockShuffleHandler2 shuffleHandler = new MockShuffleHandler2();
    shuffleHandler.serviceInit(conf);
    try {
      shuffleHandler.serviceStart();
      Assert.assertTrue("ShuffleHandler should use a random chosen port", shuffleHandler.getPort() > 0);
    } finally {
      shuffleHandler.stop();
    }
  }
}
