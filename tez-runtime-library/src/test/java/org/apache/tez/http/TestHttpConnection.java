/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.http;

import com.google.common.base.Throwables;
import org.apache.tez.http.async.netty.AsyncHttpConnection;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.net.ConnectException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TestHttpConnection {

  private static int connTimeout = 5000;
  private static int readTimeout = 5000;

  /**
   * Ref: https://en.wikipedia.org/wiki/Reserved_IP_addresses
   * Using 240.0.0.1 as the intention is to connect to unhosted ip and interrupt in between.
   * With 10.255.255.255, it is possible to get permission denied exception in some
   * networks (ref: http://linux.die.net/man/2/connect).  192.0.2.x can be considered as well.
   */
  private static final String NOT_HOSTED_URL = "http://240.0.0.1:10221";

  private static ExecutorService executorService;
  private static URL url;
  private static JobTokenSecretManager tokenSecretManager;

  private Thread currentThread;

  @BeforeClass
  public static void setup() throws IOException, URISyntaxException {
    executorService = Executors.newFixedThreadPool(1,
        new ThreadFactory() {
          public Thread newThread(Runnable r) {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            return t;
          }
        });
    url = new URL(NOT_HOSTED_URL);
    tokenSecretManager = mock(JobTokenSecretManager.class);
  }

  @AfterClass
  public static void cleanup() throws Exception {
    executorService.shutdownNow();
  }

  public void baseTest(Callable<Void> worker, CountDownLatch latch, String message) throws
      InterruptedException {
    long startTime = System.currentTimeMillis();
    try {
      Future future = executorService.submit(worker);
      future.get();
    } catch (ExecutionException e) {
      assertTrue(e.getCause().getCause() instanceof IOException);
      assertTrue(e.getMessage(), e.getMessage().contains(message));
      long elapsedTime = System.currentTimeMillis() - startTime;
      assertTrue("elapasedTime=" + elapsedTime + " should be greater than " + connTimeout,
          elapsedTime > connTimeout);
    }
    assertTrue(latch.getCount() == 0);
  }

  @Test(timeout = 20000)
  public void testConnectionTimeout() throws IOException, InterruptedException {
    HttpConnectionParams params = getConnectionParams();

    //For http
    CountDownLatch latch = new CountDownLatch(1);
    HttpConnection httpConn = getHttpConnection(params);
    baseTest(new Worker(latch, httpConn, false), latch, "Failed to connect");

    //For async http
    latch = new CountDownLatch(1);
    AsyncHttpConnection asyncHttpConn = getAsyncHttpConnection(params);
    baseTest(new Worker(latch, asyncHttpConn, false), latch, "connection timed out");
  }

  @Test(timeout = 20000)
  @SuppressWarnings("unchecked")
  //Should be interruptible
  public void testAsyncHttpConnectionInterrupt()
      throws IOException, InterruptedException, ExecutionException {
    CountDownLatch latch = new CountDownLatch(1);
    HttpConnectionParams params = getConnectionParams();
    AsyncHttpConnection asyncHttpConn = getAsyncHttpConnection(params);
    Future future = executorService.submit(new Worker(latch, asyncHttpConn, true));

    while(currentThread == null) {
      synchronized (this) {
        wait(100);
      }
    }
    assertTrue("currentThread is still null", currentThread != null);
    Thread.sleep(1000); //To avoid race to interrupt the thread before connect()

    //Try interrupting the thread (exception verification happens in the worker itself)
    currentThread.interrupt();

    future.get();
    assertTrue(latch.getCount() == 0);
  }

  HttpConnectionParams getConnectionParams() {
    HttpConnectionParams params = mock(HttpConnectionParams.class);
    when(params.getBufferSize()).thenReturn(8192);
    when(params.getKeepAliveMaxConnections()).thenReturn(1);
    when(params.getConnectionTimeout()).thenReturn(connTimeout);
    when(params.getReadTimeout()).thenReturn(readTimeout);
    return params;
  }

  HttpConnection getHttpConnection(HttpConnectionParams params) throws IOException {
    HttpConnection realConn = new HttpConnection(url, params, "log", tokenSecretManager);
    HttpConnection connection = spy(realConn);

    doAnswer(new Answer() {
      public Void answer(InvocationOnMock invocation) {
        return null;
      }
    }).when(connection).computeEncHash();
    return connection;
  }

  AsyncHttpConnection getAsyncHttpConnection(HttpConnectionParams params) throws IOException {
    AsyncHttpConnection realConn = new AsyncHttpConnection(url, params, "log", tokenSecretManager);
    AsyncHttpConnection connection = spy(realConn);

    doAnswer(new Answer() {
      public Void answer(InvocationOnMock invocation) {
        return null;
      }
    }).when(connection).computeEncHash();
    return connection;
  }

  class Worker implements Callable<Void> {
    private CountDownLatch latch;
    private BaseHttpConnection connection;
    private boolean expectingInterrupt;

    public Worker(CountDownLatch latch, BaseHttpConnection connection, boolean expectingInterrupt) {
      this.latch = latch;
      this.connection = connection;
      this.expectingInterrupt = expectingInterrupt;
    }

    @Override
    public Void call() throws Exception {
      try {
        currentThread = Thread.currentThread();
        connection.connect();
        fail();
      } catch(Throwable t) {
        if (expectingInterrupt) {
          if (t instanceof ConnectException) {
            //ClosedByInterruptException via NettyConnectListener.operationComplete()
            assertTrue("Expected ClosedByInterruptException, received "
                    + Throwables.getStackTraceAsString(t.getCause()),
                t.getCause() instanceof ClosedByInterruptException);
          } else {
            // InterruptedException if TezBodyDeferringAsyncHandler quits
            assertTrue(t instanceof InterruptedException);
          }
        }
      } finally {
        latch.countDown();
        if (connection != null) {
          connection.cleanup(true);
        }
      }
      return null;
    }
  }
}
