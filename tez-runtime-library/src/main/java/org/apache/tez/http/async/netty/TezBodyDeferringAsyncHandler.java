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
package org.apache.tez.http.async.netty;

import com.ning.http.client.AsyncHandler;
import com.ning.http.client.HttpResponseBodyPart;
import com.ning.http.client.HttpResponseHeaders;
import com.ning.http.client.HttpResponseStatus;
import com.ning.http.client.Response;
import org.apache.hadoop.classification.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Same as {@link com.ning.http.client.BodyDeferringAsyncHandler} with additional checks handle
 * errors in getResponse(). Based on testing, at very high load {@link com.ning.http.client
 * .BodyDeferringAsyncHandler} gets to hung state in getResponse() as it tries to wait
 * indefinitely for headers to arrive.  This class tries to fix the problem by waiting only for
 * the connection timeout.
 */
@InterfaceAudience.Private
class TezBodyDeferringAsyncHandler implements AsyncHandler<Response> {
  private static final Logger LOG = LoggerFactory.getLogger(TezBodyDeferringAsyncHandler.class);

  private final Response.ResponseBuilder responseBuilder = new Response.ResponseBuilder();
  private final CountDownLatch headersArrived = new CountDownLatch(1);
  private final OutputStream output;

  private volatile boolean responseSet;
  private volatile boolean statusReceived;
  private volatile Response response;
  private volatile Throwable throwable;

  private final Semaphore semaphore = new Semaphore(1);

  private final URL url;
  private final int headerReceiveTimeout;

  TezBodyDeferringAsyncHandler(final OutputStream os, final URL url, final int timeout) {
    this.output = os;
    this.responseSet = false;
    this.url = url;
    this.headerReceiveTimeout = timeout;
  }

  public void onThrowable(Throwable t) {
    this.throwable = t;
    // Counting down to handle error cases too.
    // In "premature exceptions" cases, the onBodyPartReceived() and
    // onCompleted()
    // methods will never be invoked, leaving caller of getResponse() method
    // blocked forever.
    try {
      semaphore.acquire();
    } catch (InterruptedException e) {
      // Ignore
    } finally {
      LOG.error("Error in asyncHandler ", t);
      headersArrived.countDown();
      semaphore.release();
    }
    try {
      closeOut();
    } catch (IOException e) {
      // ignore
    }
  }

  public AsyncHandler.STATE onStatusReceived(HttpResponseStatus responseStatus) throws Exception {
    responseBuilder.reset();
    responseBuilder.accumulate(responseStatus);
    statusReceived = true;
    return AsyncHandler.STATE.CONTINUE;
  }

  public AsyncHandler.STATE onHeadersReceived(HttpResponseHeaders headers) throws Exception {
    responseBuilder.accumulate(headers);
    return AsyncHandler.STATE.CONTINUE;
  }

  public AsyncHandler.STATE onBodyPartReceived(HttpResponseBodyPart bodyPart) throws Exception {
    // body arrived, flush headers
    if (!responseSet) {
      response = responseBuilder.build();
      responseSet = true;
      headersArrived.countDown();
    }
    bodyPart.writeTo(output);
    return AsyncHandler.STATE.CONTINUE;
  }

  protected void closeOut() throws IOException {
    try {
      output.flush();
    } finally {
      output.close();
    }
  }

  public Response onCompleted() throws IOException {
    if (!responseSet) {
      response = responseBuilder.build();
      responseSet = true;
    }
    // Counting down to handle error cases too.
    // In "normal" cases, latch is already at 0 here
    // But in other cases, for example when because of some error
    // onBodyPartReceived() is never called, the caller
    // of getResponse() would remain blocked infinitely.
    // By contract, onCompleted() is always invoked, even in case of errors
    headersArrived.countDown();
    closeOut();
    try {
      semaphore.acquire();
      if (throwable != null) {
        IOException ioe = new IOException(throwable.getMessage());
        ioe.initCause(throwable);
        throw ioe;
      } else {
        // sending out current response
        return responseBuilder.build();
      }
    } catch (InterruptedException e) {
      return null;
    } finally {
      semaphore.release();
    }
  }

  /**
   * This method -- unlike Future<Reponse>.get() -- will block only as long,
   * as headers arrive. This is useful for large transfers, to examine headers
   * ASAP, and defer body streaming to it's fine destination and prevent
   * unneeded bandwidth consumption. The response here will contain the very
   * 1st response from server, so status code and headers, but it might be
   * incomplete in case of broken servers sending trailing headers. In that
   * case, the "usual" Future<Response>.get() method will return complete
   * headers, but multiple invocations of getResponse() will always return the
   * 1st cached, probably incomplete one. Note: the response returned by this
   * method will contain everything <em>except</em> the response body itself,
   * so invoking any method like Response.getResponseBodyXXX() will result in
   * error! Also, please not that this method might return <code>null</code>
   * in case of some errors.
   *
   * @return a {@link Response}
   * @throws InterruptedException
   */
  public Response getResponse() throws InterruptedException, IOException {
    /**
     * Based on testing, it is possible that it is in connected state, but the headers are not
     * received. Instead of waiting forever, close after timeout for next retry.
     */
    boolean result = headersArrived.await(headerReceiveTimeout, TimeUnit.MILLISECONDS);
    if (!result) {
      LOG.error("Breaking after timeout={}, url={}, responseSet={} statusReceived={}",
          headerReceiveTimeout, url, responseSet, statusReceived);
      return null;
    }
    try {
      semaphore.acquire();
      if (throwable != null) {
        IOException ioe = new IOException(throwable.getMessage());
        ioe.initCause(throwable);
        throw ioe;
      } else {
        return response;
      }
    } finally {
      semaphore.release();
    }
  }

  /**
   * A simple helper class that is used to perform automatic "join" for async
   * download and the error checking of the Future of the request.
   */
  static class BodyDeferringInputStream extends FilterInputStream {
    private final Future<Response> future;
    private final TezBodyDeferringAsyncHandler bdah;

    public BodyDeferringInputStream(final Future<Response> future,
        final TezBodyDeferringAsyncHandler bdah, final InputStream in) {
      super(in);
      this.future = future;
      this.bdah = bdah;
    }

    /**
     * Closes the input stream, and "joins" (wait for complete execution
     * together with potential exception thrown) of the async request.
     */
    public void close() throws IOException {
      // close
      super.close();
      // "join" async request
      try {
        getLastResponse();
      } catch (Exception e) {
        IOException ioe = new IOException(e.getMessage());
        ioe.initCause(e);
        throw ioe;
      }
    }

    /**
     * Delegates to {@link TezBodyDeferringAsyncHandler#getResponse()}. Will
     * blocks as long as headers arrives only. Might return
     * <code>null</code>. See
     * {@link TezBodyDeferringAsyncHandler#getResponse()} method for details.
     *
     * @return a {@link Response}
     * @throws InterruptedException
     */
    public Response getAsapResponse() throws InterruptedException, IOException {
      return bdah.getResponse();
    }

    /**
     * Delegates to <code>Future<Response>#get()</code> method. Will block
     * as long as complete response arrives.
     *
     * @return a {@link Response}
     * @throws InterruptedException
     * @throws java.util.concurrent.ExecutionException
     */
    public Response getLastResponse() throws InterruptedException, ExecutionException {
      return future.get();
    }
  }
}