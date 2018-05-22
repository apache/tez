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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.ListenableFuture;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilder;
import com.ning.http.client.Response;
import org.apache.commons.io.IOUtils;
import org.apache.tez.http.BaseHttpConnection;
import org.apache.tez.http.HttpConnectionParams;
import org.apache.tez.http.SSLFactory;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.apache.tez.util.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class AsyncHttpConnection extends BaseHttpConnection {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncHttpConnection.class);

  private final JobTokenSecretManager jobTokenSecretMgr;
  private String encHash;
  private String msgToEncode;

  private final HttpConnectionParams httpConnParams;
  private final StopWatch stopWatch;
  private final URL url;

  private static volatile AsyncHttpClient httpAsyncClient;

  private final TezBodyDeferringAsyncHandler handler;
  private final PipedOutputStream pos; //handler would write to this as and when it receives chunks
  private final PipedInputStream pis; //connected to pos, which can be used by fetchers

  private Response response;
  private ListenableFuture<Response> responseFuture;
  private TezBodyDeferringAsyncHandler.BodyDeferringInputStream dis;

  private void initClient(HttpConnectionParams httpConnParams) throws IOException {
    if (httpAsyncClient != null) {
      return;
    }

    if (httpAsyncClient == null) {
      synchronized (AsyncHttpConnection.class) {
        if (httpAsyncClient == null) {
          LOG.info("Initializing AsyncClient (TezBodyDeferringAsyncHandler)");
          AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();
          if (httpConnParams.isSslShuffle()) {
            //Configure SSL
            SSLFactory sslFactory = httpConnParams.getSslFactory();
            Preconditions.checkArgument(sslFactory != null, "SSLFactory can not be null");
            sslFactory.configure(builder);
          }

          /**
           * TODO : following settings need fine tuning.
           * Change following config to accept common thread pool later.
           * Change max connections based on the total inputs (ordered & unordered). Need to tune
           * setMaxConnections & addRequestFilter.
           */
          builder
              .setAllowPoolingConnections(httpConnParams.isKeepAlive())
              .setAllowPoolingSslConnections(httpConnParams.isKeepAlive())
              .setCompressionEnforced(false)
              //.setExecutorService(applicationThreadPool)
              //.addRequestFilter(new ThrottleRequestFilter())
              .setMaxConnectionsPerHost(1)
              .setConnectTimeout(httpConnParams.getConnectionTimeout())
              .setDisableUrlEncodingForBoundedRequests(true)
              .build();
            httpAsyncClient = new AsyncHttpClient(builder.build());
        }
      }
    }
  }

  public AsyncHttpConnection(URL url, HttpConnectionParams connParams,
      String logIdentifier, JobTokenSecretManager jobTokenSecretManager) throws IOException {
    this.jobTokenSecretMgr = jobTokenSecretManager;
    this.httpConnParams = connParams;
    this.url = url;
    this.stopWatch = new StopWatch();
    if (LOG.isDebugEnabled()) {
      LOG.debug("MapOutput URL :" + url.toString());
    }

    initClient(httpConnParams);
    pos = new PipedOutputStream();
    pis = new PipedInputStream(pos, httpConnParams.getBufferSize());
    handler = new TezBodyDeferringAsyncHandler(pos, url, UNIT_CONNECT_TIMEOUT);
  }

  @VisibleForTesting
  public void computeEncHash() throws IOException {
    // generate hash of the url
    msgToEncode = SecureShuffleUtils.buildMsgFrom(url);
    encHash = SecureShuffleUtils.hashFromString(msgToEncode, jobTokenSecretMgr);
  }

  /**
   * Connect to source
   *
   * @return true if connection was successful
   * false if connection was previously cleaned up
   * @throws IOException upon connection failure
   */
  public boolean connect() throws IOException, InterruptedException {
    computeEncHash();

    RequestBuilder rb = new RequestBuilder();
    rb.setHeader(SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
    rb.setHeader(ShuffleHeader.HTTP_HEADER_NAME, ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    rb.setHeader(ShuffleHeader.HTTP_HEADER_VERSION, ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
    Request request = rb.setUrl(url.toString()).build();

    //for debugging
    LOG.debug("Request url={}, encHash={}, id={}", url, encHash);

    try {
      //Blocks calling thread until it receives headers, but have the option to defer response body
      responseFuture = httpAsyncClient.executeRequest(request, handler);

      //BodyDeferringAsyncHandler would automatically manage producer and consumer frequency mismatch
      dis = new TezBodyDeferringAsyncHandler.BodyDeferringInputStream(responseFuture, handler, pis);

      response = dis.getAsapResponse();
      if (response == null) {
        throw new IOException("Response is null");
      }
    } catch(IOException e) {
      throw e;
    }

    //verify the response
    int rc = response.getStatusCode();
    if (rc != HttpURLConnection.HTTP_OK) {
      LOG.debug("Request url={}, id={}", response.getUri());
      throw new IOException("Got invalid response code " + rc + " from "
          + url + ": " + response.getStatusText());
    }
    return true;
  }

  public void validate() throws IOException {
    stopWatch.reset().start();
    // get the shuffle version
    if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME
        .equals(response.getHeader(ShuffleHeader.HTTP_HEADER_NAME))
        || !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION
        .equals(response.getHeader(ShuffleHeader.HTTP_HEADER_VERSION))) {
      throw new IOException("Incompatible shuffle response version");
    }

    // get the replyHash which is HMac of the encHash we sent to the server
    String replyHash = response.getHeader(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
    if (replyHash == null) {
      throw new IOException("security validation of TT Map output failed");
    }
    LOG.debug("url={};encHash={};replyHash={}", msgToEncode, encHash, replyHash);

    // verify that replyHash is HMac of encHash
    SecureShuffleUtils.verifyReply(replyHash, encHash, jobTokenSecretMgr);
    //Following log statement will be used by tez-tool perf-analyzer for mapping attempt to NM host
    LOG.info("for url={} sent hash and receievd reply {} ms", url, stopWatch.now(TimeUnit.MILLISECONDS));
  }

  /**
   * Get the inputstream from the connection
   *
   * @return DataInputStream
   * @throws IOException
   */
  public DataInputStream getInputStream() throws IOException, InterruptedException {
    Preconditions.checkState(response != null, "Response can not be null");
    return new DataInputStream(dis);
  }

  @VisibleForTesting
  public void close() {
    httpAsyncClient.close();
    httpAsyncClient = null;
  }
  /**
   * Cleanup the connection.
   *
   * @param disconnect
   * @throws IOException
   */
  public void cleanup(boolean disconnect) throws IOException {
    // Netty internally has its own connection management and takes care of it.
    if (response != null) {
      dis.close();
    }
    IOUtils.closeQuietly(pos);
    IOUtils.closeQuietly(pis);
    response = null;
  }

}
