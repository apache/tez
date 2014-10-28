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

package org.apache.tez.runtime.library.common.shuffle;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.HttpsURLConnection;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.tez.common.security.JobTokenSecretManager;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.orderedgrouped.ShuffleHeader;

import com.google.common.base.Stopwatch;

/**
 * HttpConnection which can be used for Unordered / Ordered shuffle.
 */
public class HttpConnection {

  private static final Log LOG = LogFactory.getLog(HttpConnection.class);

  /** Basic/unit connection timeout (in milliseconds) */
  private final static int UNIT_CONNECT_TIMEOUT = 60 * 1000;

  private URL url;
  private final String logIdentifier;

  //Shared by many threads
  private static SSLFactory sslFactory;

  @VisibleForTesting
  protected HttpURLConnection connection;
  private DataInputStream input;

  private boolean connectionSucceeed;
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

  private void setupConnection() throws IOException {
    connection = (HttpURLConnection) url.openConnection();
    if (sslFactory != null && httpConnParams.sslShuffle) {
      try {
        ((HttpsURLConnection) connection).setSSLSocketFactory(sslFactory
          .createSSLSocketFactory());
        ((HttpsURLConnection) connection).setHostnameVerifier(sslFactory
          .getHostnameVerifier());
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
    }
    // generate hash of the url
    msgToEncode = SecureShuffleUtils.buildMsgFrom(url);
    encHash = SecureShuffleUtils.hashFromString(msgToEncode, jobTokenSecretMgr);

    // put url hash into http header
    connection.addRequestProperty(SecureShuffleUtils.HTTP_HEADER_URL_HASH,
      encHash);
    // set the read timeout
    connection.setReadTimeout(httpConnParams.readTimeout);
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
   *         false if connection was previously cleaned up
   * @throws IOException upon connection failure
   */
  public boolean connect() throws IOException {
    return connect(httpConnParams.connectionTimeout);
  }

  /**
   * Connect to source with specific timeout
   * 
   * @param connectionTimeout
   * @return true if connection was successful
   *         false if connection was previously cleaned up
   * @throws IOException upon connection failure
   */
  public boolean connect(int connectionTimeout) throws IOException {
    stopWatch.reset().start();
    if (connection == null) {
      setupConnection();
    }
    int unit = 0;
    if (connectionTimeout < 0) {
      throw new IOException("Invalid timeout " + "[timeout = "
          + connectionTimeout + " ms]");
    } else if (connectionTimeout > 0) {
      unit = Math.min(UNIT_CONNECT_TIMEOUT, connectionTimeout);
    }
    // set the connect timeout to the unit-connect-timeout
    connection.setConnectTimeout(unit);
    int connectionFailures = 0;
    while (true) {
      try {
        connection.connect();
        connectionSucceeed = true;
        break;
      } catch (IOException ioe) {
        // Don't attempt another connect if already cleanedup.
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
          throw ioe;
        }
        // reset the connect timeout for the last try
        if (connectionTimeout < unit) {
          unit = connectionTimeout;
          // reset the connect time out for the final connect
          connection.setConnectTimeout(unit);
        }
        connectionFailures++;
      }
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Time taken to connect to " + url.toString() +
        " " + stopWatch.elapsedTime(TimeUnit.MILLISECONDS) + " ms; connectionFailures="+ connectionFailures);
    }
    return true;
  }

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
    LOG.info("for url=" + url +
      " sent hash and receievd reply " + stopWatch.elapsedTime(TimeUnit.MILLISECONDS) + " ms");
  }

  /**
   * Get the inputstream from the connection
   * 
   * @return DataInputStream
   * @throws IOException
   */
  public DataInputStream getInputStream() throws IOException {
    stopWatch.reset().start();
    DataInputStream input = null;
    if (connectionSucceeed) {
      input =
          new DataInputStream(new BufferedInputStream(
            connection.getInputStream(), httpConnParams.bufferSize));
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
   * @param disconnect
   *          Close the connection if this is true; otherwise respect keepalive
   * @throws IOException
   */
  public void cleanup(boolean disconnect) throws IOException {
    cleanup = true;
    stopWatch.reset().start();
    try {
      if (input != null) {
        LOG.info("Closing input on " + logIdentifier);
        input.close();
      }
      if (httpConnParams.keepAlive && connectionSucceeed) {
        // Refer:
        // http://docs.oracle.com/javase/6/docs/technotes/guides/net/http-keepalive.html
        readErrorStream(connection.getErrorStream());
      }
      if (connection != null && (disconnect || !httpConnParams.keepAlive)) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Closing connection on " + logIdentifier);
        }
        connection.disconnect();
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

  public static class HttpConnectionParams {
    private boolean keepAlive;
    private int keepAliveMaxConnections;
    private int connectionTimeout;
    private int readTimeout;
    private int bufferSize;
    private boolean sslShuffle;

    public boolean getKeepAlive() {
      return keepAlive;
    }

    public int getKeepAliveMaxConnections() {
      return keepAliveMaxConnections;
    }

    public int getConnectionTimeout() {
      return connectionTimeout;
    }

    public int getReadTimeout() {
      return readTimeout;
    }

    public void setReadTimeout(int readTimeout) {
      this.readTimeout = readTimeout;
    }

    public int getBufferSize() {
      return bufferSize;
    }

    public boolean isSSLShuffleEnabled() {
      return sslShuffle;
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("keepAlive=").append(keepAlive).append(", ");
      sb.append("keepAliveMaxConnections=").append(keepAliveMaxConnections).append(", ");
      sb.append("connectionTimeout=").append(connectionTimeout).append(", ");
      sb.append("readTimeout=").append(readTimeout).append(", ");
      sb.append("bufferSize=").append(bufferSize).append(", ");
      sb.append("sslShuffle=").append(sslShuffle);
      return sb.toString();
    }
  }

  public static class HttpConnectionParamsBuilder {
    private HttpConnectionParams params;

    public HttpConnectionParamsBuilder() {
      params = new HttpConnectionParams();
    }

    public HttpConnectionParamsBuilder setKeepAlive(boolean keepAlive,
        int keepAliveMaxConnections) {
      params.keepAlive = keepAlive;
      params.keepAliveMaxConnections = keepAliveMaxConnections;
      return this;
    }

    public HttpConnectionParamsBuilder setTimeout(int connectionTimeout,
        int readTimeout) {
      params.connectionTimeout = connectionTimeout;
      params.readTimeout = readTimeout;
      return this;
    }

    public synchronized HttpConnectionParamsBuilder setSSL(boolean sslEnabled,
        Configuration conf) {
      synchronized (HttpConnectionParamsBuilder.class) {
        params.sslShuffle = sslEnabled;
        if (sslEnabled) {
          //Create sslFactory if it is null or if it was destroyed earlier
          if (sslFactory == null || sslFactory.getKeystoresFactory()
              .getTrustManagers() == null) {
            LOG.info("Initializing SSL factory in HttpConnection");
            sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, conf);
            try {
              sslFactory.init();
            } catch (Exception ex) {
              sslFactory.destroy();
              sslFactory = null;
              throw new RuntimeException(ex);
            }
          }
        }
      }
      return this;
    }

    public HttpConnectionParamsBuilder setBufferSize(int bufferSize) {
      params.bufferSize = bufferSize;
      return this;
    }

    public HttpConnectionParams build() {
      return params;
    }
  }
}

