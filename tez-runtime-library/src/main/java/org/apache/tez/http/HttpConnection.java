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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.concurrent.TimeUnit;

public class HttpConnection extends BaseHttpConnection {

  private static final Logger LOG = LoggerFactory.getLogger(HttpConnection.class);

  private URL url;
  private final String logIdentifier;

  @VisibleForTesting
  protected volatile HttpURLConnection connection;
  private volatile DataInputStream input;
  private volatile boolean connectionSucceeed;
  private volatile boolean cleanup;

  private final JobTokenSecretManager jobTokenSecretMgr;
  private String encHash;
  private String msgToEncode;

  private final HttpConnectionParams httpConnParams;
  private final Stopwatch stopWatch;

  /**
   * HttpConnection
   *
   * @param url
   * @param connParams
   * @param logIdentifier
   * @param jobTokenSecretManager
   * @throws IOException
   */
  public HttpConnection(URL url, HttpConnectionParams connParams,
      String logIdentifier, JobTokenSecretManager jobTokenSecretManager) throws IOException {
    this.logIdentifier = logIdentifier;
    this.jobTokenSecretMgr = jobTokenSecretManager;
    this.httpConnParams = connParams;
    this.url = url;
    this.stopWatch = new Stopwatch();
    if (LOG.isDebugEnabled()) {
      LOG.debug("MapOutput URL :" + url.toString());
    }
  }

  @VisibleForTesting
  public void computeEncHash() throws IOException {
    // generate hash of the url
    msgToEncode = SecureShuffleUtils.buildMsgFrom(url);
    encHash = SecureShuffleUtils.hashFromString(msgToEncode, jobTokenSecretMgr);
  }

  private void setupConnection() throws IOException {
    connection = (HttpURLConnection) url.openConnection();
    if (httpConnParams.isSslShuffle()) {
      //Configure for SSL
      SSLFactory sslFactory = httpConnParams.getSslFactory();
      Preconditions.checkArgument(sslFactory != null, "SSLFactory can not be null");
      sslFactory.configure(connection);
    }

    computeEncHash();

    // put url hash into http header
    connection.addRequestProperty(SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
    // set the read timeout
    connection.setReadTimeout(httpConnParams.getReadTimeout());
    // put shuffle version into http header
    connection.addRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
        ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
    connection.addRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
        ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
  }

  /**
   * Connect to source
   *
   * @return true if connection was successful
   * false if connection was previously cleaned up
   * @throws IOException upon connection failure
   */
  @Override
  public boolean connect() throws IOException {
    return connect(httpConnParams.getConnectionTimeout());
  }

  /**
   * Connect to source with specific timeout
   *
   * @param connectionTimeout
   * @return true if connection was successful
   * false if connection was previously cleaned up
   * @throws IOException upon connection failure
   */
  private boolean connect(int connectionTimeout) throws IOException {
    stopWatch.reset().start();
    if (connection == null) {
      setupConnection();
    }
    int unit = 0;
    if (connectionTimeout < 0) {
      throw new IOException("Invalid timeout " + "[timeout = " + connectionTimeout + " ms]");
    } else if (connectionTimeout > 0) {
      unit = Math.min(UNIT_CONNECT_TIMEOUT, connectionTimeout);
    }
    // set the connect timeout to the unit-connect-timeout
    connection.setConnectTimeout(unit);
    int connectionFailures = 0;
    while (true) {
      long connectStartTime = System.currentTimeMillis();
      try {
        connection.connect();
        connectionSucceeed = true;
        break;
      } catch (IOException ioe) {
        // Don't attempt another connect if already cleanedup.
        connectionFailures++;
        if (cleanup) {
          LOG.info("Cleanup is set to true. Not attempting to"
              + " connect again. Last exception was: ["
              + ioe.getClass().getName() + ", " + ioe.getMessage() + "]");
          return false;
        }
        // update the total remaining connect-timeout
        connectionTimeout -= unit;
        // throw an exception if we have waited for timeout amount of time
        // note that the updated value if timeout is used here
        if (connectionTimeout <= 0) {
          throw new IOException(
              "Failed to connect to " + url + ", #connectionFailures=" + connectionFailures, ioe);
        }
        long elapsed = System.currentTimeMillis() - connectStartTime;
        if (elapsed < unit) {
          try {
            long sleepTime = unit - elapsed;
            if (LOG.isDebugEnabled()) {
              LOG.debug("Sleeping for " + sleepTime + " while establishing connection to " + url +
                  ", since connectAttempt returned in " + elapsed + " ms");
            }
            Thread.sleep(sleepTime);
          } catch (InterruptedException e) {
            throw new IOException(
                "Connection establishment sleep interrupted, #connectionFailures=" +
                    connectionFailures, e);
          }
        }

        // reset the connect timeout for the last try
        if (connectionTimeout < unit) {
          unit = connectionTimeout;
          // reset the connect time out for the final connect
          connection.setConnectTimeout(unit);
        }

      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to connect to " + url.toString() +
          " " + stopWatch.elapsedTime(TimeUnit.MILLISECONDS) + " ms; connectionFailures="
          + connectionFailures);
    }
    return true;
  }

  @Override
  public void validate() throws IOException {
    stopWatch.reset().start();
    int rc = connection.getResponseCode();
    if (rc != HttpURLConnection.HTTP_OK) {
      throw new IOException("Got invalid response code " + rc + " from " + url
          + ": " + connection.getResponseMessage());
    }

    // get the shuffle version
    if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(connection
        .getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
        || !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(connection
        .getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))) {
      throw new IOException("Incompatible shuffle response version");
    }

    // get the replyHash which is HMac of the encHash we sent to the server
    String replyHash =
        connection
            .getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
    if (replyHash == null) {
      throw new IOException("security validation of TT Map output failed");
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("url=" + msgToEncode + ";encHash=" + encHash + ";replyHash="
          + replyHash);
    }

    // verify that replyHash is HMac of encHash
    SecureShuffleUtils.verifyReply(replyHash, encHash, jobTokenSecretMgr);
    //Following log statement will be used by tez-tool perf-analyzer for mapping attempt to NM host
    LOG.info("for url=" + url +
        " sent hash and receievd reply " + stopWatch.elapsedTime(TimeUnit.MILLISECONDS) + " ms");
  }

  /**
   * Get the inputstream from the connection
   *
   * @return DataInputStream
   * @throws IOException
   */
  @Override
  public DataInputStream getInputStream() throws IOException {
    stopWatch.reset().start();
    if (connectionSucceeed) {
      input = new DataInputStream(new BufferedInputStream(
              connection.getInputStream(), httpConnParams.getBufferSize()));
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to getInputStream (connect) " + url +
          " " + stopWatch.elapsedTime(TimeUnit.MILLISECONDS) + " ms");
    }
    return input;
  }

  /**
   * Cleanup the connection.
   *
   * @param disconnect Close the connection if this is true; otherwise respect keepalive
   * @throws IOException
   */
  @Override
  public void cleanup(boolean disconnect) throws IOException {
    cleanup = true;
    stopWatch.reset().start();
    try {
      if (input != null) {
        LOG.info("Closing input on " + logIdentifier);
        input.close();
        input = null;
      }
      if (httpConnParams.isKeepAlive() && connectionSucceeed) {
        // Refer:
        // http://docs.oracle.com/javase/6/docs/technotes/guides/net/http-keepalive.html
        readErrorStream(connection.getErrorStream());
      }
      if (connection != null && (disconnect || !httpConnParams.isKeepAlive())) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closing connection on " + logIdentifier);
        }
        connection.disconnect();
        connection = null;
      }
    } catch (IOException e) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Exception while shutting down fetcher " + logIdentifier, e);
      } else {
        LOG.info("Exception while shutting down fetcher " + logIdentifier
            + ": " + e.getMessage());
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to cleanup connection to " + url +
          " " + stopWatch.elapsedTime(TimeUnit.MILLISECONDS) + " ms");
    }
  }

  /**
   * Cleanup the error stream if any, for keepAlive connections
   *
   * @param errorStream
   */
  private void readErrorStream(InputStream errorStream) {
    if (errorStream == null) {
      return;
    }
    try {
      DataOutputBuffer errorBuffer = new DataOutputBuffer();
      IOUtils.copyBytes(errorStream, errorBuffer, 4096);
      IOUtils.closeStream(errorBuffer);
      IOUtils.closeStream(errorStream);
    } catch (IOException ioe) {
      // ignore
    }
  }
}

