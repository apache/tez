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

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import javax.crypto.SecretKey;
import javax.net.ssl.HttpsURLConnection;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.security.ssl.SSLFactory;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.apache.tez.runtime.library.common.security.SecureShuffleUtils;
import org.apache.tez.runtime.library.common.shuffle.impl.MapOutput.Type;
import org.apache.tez.runtime.library.common.sort.impl.IFileInputStream;

import com.google.common.annotations.VisibleForTesting;

class Fetcher extends Thread {
  
  private static final Log LOG = LogFactory.getLog(Fetcher.class);
  
  /** Basic/unit connection timeout (in milliseconds) */
  private final static int UNIT_CONNECT_TIMEOUT = 60 * 1000;

  private static enum ShuffleErrors{IO_ERROR, WRONG_LENGTH, BAD_ID, WRONG_MAP,
                                    CONNECTION, WRONG_REDUCE}
  
  private final static String SHUFFLE_ERR_GRP_NAME = "Shuffle Errors";
  private final TezCounter connectionErrs;
  private final TezCounter ioErrs;
  private final TezCounter wrongLengthErrs;
  private final TezCounter badIdErrs;
  private final TezCounter wrongMapErrs;
  private final TezCounter wrongReduceErrs;
  private final MergeManager merger;
  private final ShuffleScheduler scheduler;
  private final ShuffleClientMetrics metrics;
  private final Shuffle shuffle;
  private final int id;
  private static int nextId = 0;
  private int currentPartition = -1;
  
  private final int connectionTimeout;
  private final int readTimeout;
  private final int bufferSize;
  
  // Decompression of map-outputs
  private final CompressionCodec codec;
  private final Decompressor decompressor;
  private final SecretKey jobTokenSecret;

  private volatile boolean stopped = false;
  
  private final boolean ifileReadAhead;
  private final int ifileReadAheadLength;

  private static boolean sslShuffle;
  private static SSLFactory sslFactory;
  
  private LinkedHashSet<InputAttemptIdentifier> remaining;

  public Fetcher(Configuration job, 
      ShuffleScheduler scheduler, MergeManager merger,
      ShuffleClientMetrics metrics,
      Shuffle shuffle, SecretKey jobTokenSecret, boolean ifileReadAhead, int ifileReadAheadLength, CompressionCodec codec, TezInputContext inputContext) throws IOException {
    this.scheduler = scheduler;
    this.merger = merger;
    this.metrics = metrics;
    this.shuffle = shuffle;
    this.id = ++nextId;
    this.jobTokenSecret = jobTokenSecret;
    ioErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.IO_ERROR.toString());
    wrongLengthErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_LENGTH.toString());
    badIdErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.BAD_ID.toString());
    wrongMapErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_MAP.toString());
    connectionErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.CONNECTION.toString());
    wrongReduceErrs = inputContext.getCounters().findCounter(SHUFFLE_ERR_GRP_NAME,
        ShuffleErrors.WRONG_REDUCE.toString());

    this.ifileReadAhead = ifileReadAhead;
    this.ifileReadAheadLength = ifileReadAheadLength;
    
    if (codec != null) {
      this.codec = codec;
      this.decompressor = CodecPool.getDecompressor(codec);
    } else {
      this.codec = null;
      this.decompressor = null;
    }

    this.connectionTimeout = 
        job.getInt(TezJobConfig.TEZ_RUNTIME_SHUFFLE_CONNECT_TIMEOUT,
            TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_STALLED_COPY_TIMEOUT);
    this.readTimeout = 
        job.getInt(TezJobConfig.TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT, 
            TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_READ_TIMEOUT);
    
    this.bufferSize = job.getInt(TezJobConfig.TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE, 
            TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_BUFFER_SIZE);

    setName("fetcher [" + TezUtils.cleanVertexName(inputContext.getSourceVertexName()) + "] #" + id);
    setDaemon(true);

    synchronized (Fetcher.class) {
      sslShuffle = job.getBoolean(TezJobConfig.TEZ_RUNTIME_SHUFFLE_ENABLE_SSL,
          TezJobConfig.DEFAULT_TEZ_RUNTIME_SHUFFLE_ENABLE_SSL);
      if (sslShuffle && sslFactory == null) {
        sslFactory = new SSLFactory(SSLFactory.Mode.CLIENT, job);
        try {
          sslFactory.init();
        } catch (Exception ex) {
          sslFactory.destroy();
          throw new RuntimeException(ex);
        }
      }
    }
  }  

  public void run() {
    try {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        remaining = null; // Safety.
        MapHost host = null;
        try {
          // If merge is on, block
          merger.waitForInMemoryMerge();

          // Get a host to shuffle from
          host = scheduler.getHost();
          metrics.threadBusy();

          // Shuffle
          copyFromHost(host);
        } finally {
          if (host != null) {
            scheduler.freeHost(host);
            metrics.threadFree();            
          }
        }
      }
    } catch (InterruptedException ie) {
      return;
    } catch (Throwable t) {
      shuffle.reportException(t);
    }
  }

  public void shutDown() throws InterruptedException {
    this.stopped = true;
    interrupt();
    try {
      join(5000);
    } catch (InterruptedException ie) {
      LOG.warn("Got interrupt while joining " + getName(), ie);
    }
    if (sslFactory != null) {
      sslFactory.destroy();
    }
  }

  @VisibleForTesting
  protected HttpURLConnection openConnection(URL url) throws IOException {
    HttpURLConnection conn = (HttpURLConnection) url.openConnection();
    if (sslShuffle) {
      HttpsURLConnection httpsConn = (HttpsURLConnection) conn;
      try {
        httpsConn.setSSLSocketFactory(sslFactory.createSSLSocketFactory());
      } catch (GeneralSecurityException ex) {
        throw new IOException(ex);
      }
      httpsConn.setHostnameVerifier(sslFactory.getHostnameVerifier());
    }
    return conn;
  }
  
  /**
   * The crux of the matter...
   * 
   * @param host {@link MapHost} from which we need to  
   *              shuffle available map-outputs.
   */
  @VisibleForTesting
  protected void copyFromHost(MapHost host) throws IOException {
    // Get completed maps on 'host'
    List<InputAttemptIdentifier> srcAttempts = scheduler.getMapsForHost(host);
    currentPartition = host.getPartitionId();
    
    // Sanity check to catch hosts with only 'OBSOLETE' maps, 
    // especially at the tail of large jobs
    if (srcAttempts.size() == 0) {
      return;
    }
    
    if(LOG.isDebugEnabled()) {
      LOG.debug("Fetcher " + id + " going to fetch from " + host + " for: "
        + srcAttempts + ", partitionId: " + currentPartition);
    }
    
    // List of maps to be fetched yet
    remaining = new LinkedHashSet<InputAttemptIdentifier>(srcAttempts);
    
    // Construct the url and connect
    DataInputStream input;
    boolean connectSucceeded = false;
    
    try {
      URL url = getMapOutputURL(host, srcAttempts);
      HttpURLConnection connection = openConnection(url);
      
      // generate hash of the url
      String msgToEncode = SecureShuffleUtils.buildMsgFrom(url);
      String encHash = SecureShuffleUtils.hashFromString(msgToEncode, jobTokenSecret);
      
      // put url hash into http header
      connection.addRequestProperty(
          SecureShuffleUtils.HTTP_HEADER_URL_HASH, encHash);
      // set the read timeout
      connection.setReadTimeout(readTimeout);
      // put shuffle version into http header
      connection.addRequestProperty(ShuffleHeader.HTTP_HEADER_NAME,
          ShuffleHeader.DEFAULT_HTTP_HEADER_NAME);
      connection.addRequestProperty(ShuffleHeader.HTTP_HEADER_VERSION,
          ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION);
      connect(connection, connectionTimeout);
      connectSucceeded = true;
      input = new DataInputStream(new BufferedInputStream(connection.getInputStream(), bufferSize));

      // Validate response code
      int rc = connection.getResponseCode();
      if (rc != HttpURLConnection.HTTP_OK) {
        throw new IOException(
            "Got invalid response code " + rc + " from " + url +
            ": " + connection.getResponseMessage());
      }
      // get the shuffle version
      if (!ShuffleHeader.DEFAULT_HTTP_HEADER_NAME.equals(
          connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME))
          || !ShuffleHeader.DEFAULT_HTTP_HEADER_VERSION.equals(
              connection.getHeaderField(ShuffleHeader.HTTP_HEADER_VERSION))) {
        throw new IOException("Incompatible shuffle response version");
      }
      // get the replyHash which is HMac of the encHash we sent to the server
      String replyHash = connection.getHeaderField(SecureShuffleUtils.HTTP_HEADER_REPLY_URL_HASH);
      if(replyHash==null) {
        throw new IOException("security validation of TT Map output failed");
      }
      LOG.debug("url="+msgToEncode+";encHash="+encHash+";replyHash="+replyHash);
      // verify that replyHash is HMac of encHash
      SecureShuffleUtils.verifyReply(replyHash, encHash, jobTokenSecret);
      LOG.info("for url="+msgToEncode+" sent hash and receievd reply");
    } catch (IOException ie) {
      ioErrs.increment(1);
      if (!connectSucceeded) {
        LOG.warn("Failed to connect to " + host + " with " + remaining.size() + " inputs", ie);
        connectionErrs.increment(1);
      } else {
        LOG.warn("Failed to verify reply after connecting to " + host + " with " + remaining.size()
          + " inputs pending", ie);
      }

      // At this point, either the connection failed, or the initial header verification failed.
      // The error does not relate to any specific Input. Report all of them as failed.
      
      // This ends up indirectly penalizing the host (multiple failures reported on the single host)

      for(InputAttemptIdentifier left: remaining) {
        // Need to be handling temporary glitches .. 
        // Report read error to the AM to trigger source failure heuristics
        scheduler.copyFailed(left, host, connectSucceeded, !connectSucceeded);
      }

      // Add back all remaining maps - which at this point is ALL MAPS the
      // Fetcher was started with. The Scheduler takes care of retries,
      // reporting too many failures etc.
      putBackRemainingMapOutputs(host);
      return;
    }
    
    try {
      // Loop through available map-outputs and fetch them
      // On any error, faildTasks is not null and we exit
      // after putting back the remaining maps to the 
      // yet_to_be_fetched list and marking the failed tasks.
      InputAttemptIdentifier[] failedTasks = null;
      while (!remaining.isEmpty() && failedTasks == null) {
        // fail immediately after first failure because we dont know how much to 
        // skip for this error in the input stream. So we cannot move on to the 
        // remaining outputs. YARN-1773. Will get to them in the next retry.
        failedTasks = copyMapOutput(host, input);
      }
      
      if(failedTasks != null && failedTasks.length > 0) {
        LOG.warn("copyMapOutput failed for tasks "+Arrays.toString(failedTasks));
        for(InputAttemptIdentifier left: failedTasks) {
          scheduler.copyFailed(left, host, true, false);
        }
      }
      
      IOUtils.cleanup(LOG, input);
      
      // Sanity check
      if (failedTasks == null && !remaining.isEmpty()) {
        throw new IOException("server didn't return all expected map outputs: "
            + remaining.size() + " left.");
      }
    } finally {
      putBackRemainingMapOutputs(host);
    }
  }
  
  private void putBackRemainingMapOutputs(MapHost host) {
    // Cycle through remaining MapOutputs
    boolean isFirst = true;
    InputAttemptIdentifier first = null;
    for (InputAttemptIdentifier left : remaining) {
      if (isFirst) {
        first = left;
        isFirst = false;
        continue;
      }
      scheduler.putBackKnownMapOutput(host, left);
    }
    if (first != null) { // Empty remaining list.
      scheduler.putBackKnownMapOutput(host, first);
    }
  }

  private static InputAttemptIdentifier[] EMPTY_ATTEMPT_ID_ARRAY = new InputAttemptIdentifier[0];
  
  private InputAttemptIdentifier[] copyMapOutput(MapHost host,
                                DataInputStream input) {
    MapOutput mapOutput = null;
    InputAttemptIdentifier srcAttemptId = null;
    long decompressedLength = -1;
    long compressedLength = -1;
    
    try {
      long startTime = System.currentTimeMillis();
      int forReduce = -1;
      //Read the shuffle header
      try {
        ShuffleHeader header = new ShuffleHeader();
        // TODO Review: Multiple header reads in case of status WAIT ? 
        header.readFields(input);
        if (!header.mapId.startsWith(InputAttemptIdentifier.PATH_PREFIX)) {
          throw new IllegalArgumentException(
              "Invalid header received: " + header.mapId + " partition: " + header.forReduce);
        }
        srcAttemptId = 
            scheduler.getIdentifierForFetchedOutput(header.mapId, header.forReduce);
        compressedLength = header.compressedLength;
        decompressedLength = header.uncompressedLength;
        forReduce = header.forReduce;
      } catch (IllegalArgumentException e) {
        badIdErrs.increment(1);
        LOG.warn("Invalid map id ", e);
        // Don't know which one was bad, so consider this one bad and dont read
        // the remaining because we dont know where to start reading from. YARN-1773
        return new InputAttemptIdentifier[] {getNextRemainingAttempt()};
      }

 
      // Do some basic sanity verification
      if (!verifySanity(compressedLength, decompressedLength, forReduce,
          remaining, srcAttemptId)) {
        if (srcAttemptId == null) {
          LOG.warn("Was expecting " + getNextRemainingAttempt() + " but got null");
          srcAttemptId = getNextRemainingAttempt();
        }
        assert(srcAttemptId != null);
        return new InputAttemptIdentifier[] {srcAttemptId};
      }
      
      if(LOG.isDebugEnabled()) {
        LOG.debug("header: " + srcAttemptId + ", len: " + compressedLength + 
            ", decomp len: " + decompressedLength);
      }
      
      // Get the location for the map output - either in-memory or on-disk
      try {
        mapOutput = merger.reserve(srcAttemptId, decompressedLength, id);
      } catch (IOException e) {
        // Kill the reduce attempt
        ioErrs.increment(1);
        scheduler.reportLocalError(e);
        return EMPTY_ATTEMPT_ID_ARRAY;
      }
      
      // Check if we can shuffle *now* ...
      if (mapOutput.getType() == Type.WAIT) {
        // TODO Review: Does this cause a tight loop ?
        LOG.info("fetcher#" + id + " - MergerManager returned Status.WAIT ...");
        //Not an error but wait to process data.
        return EMPTY_ATTEMPT_ID_ARRAY;
      } 
      
      // Go!
      LOG.info("fetcher#" + id + " about to shuffle output of map " + 
               mapOutput.getAttemptIdentifier() + " decomp: " +
               decompressedLength + " len: " + compressedLength + " to " +
               mapOutput.getType());
      if (mapOutput.getType() == Type.MEMORY) {
        shuffleToMemory(host, mapOutput, input, 
                        (int) decompressedLength, (int) compressedLength);
      } else {
        shuffleToDisk(host, mapOutput, input, compressedLength);
      }
      
      // Inform the shuffle scheduler
      long endTime = System.currentTimeMillis();
      scheduler.copySucceeded(srcAttemptId, host, compressedLength, decompressedLength, 
                              endTime - startTime, mapOutput);
      // Note successful shuffle
      remaining.remove(srcAttemptId);
      metrics.successFetch();
      return null;
    } catch (IOException ioe) {
      ioErrs.increment(1);
      if (srcAttemptId == null || mapOutput == null) {
        LOG.info("fetcher#" + id + " failed to read map header" + 
                 srcAttemptId + " decomp: " + 
                 decompressedLength + ", " + compressedLength, ioe);
        if(srcAttemptId == null) {
          return remaining.toArray(new InputAttemptIdentifier[remaining.size()]);
        } else {
          return new InputAttemptIdentifier[] {srcAttemptId};
        }
      }
      
      LOG.warn("Failed to shuffle output of " + srcAttemptId + 
               " from " + host.getHostIdentifier(), ioe); 

      // Inform the shuffle-scheduler
      mapOutput.abort();
      metrics.failedFetch();
      return new InputAttemptIdentifier[] {srcAttemptId};
    }

  }
  
  /**
   * Do some basic verification on the input received -- Being defensive
   * @param compressedLength
   * @param decompressedLength
   * @param forReduce
   * @param remaining
   * @param mapId
   * @return true/false, based on if the verification succeeded or not
   */
  private boolean verifySanity(long compressedLength, long decompressedLength,
      int forReduce, Set<InputAttemptIdentifier> remaining, InputAttemptIdentifier srcAttemptId) {
    if (compressedLength < 0 || decompressedLength < 0) {
      wrongLengthErrs.increment(1);
      LOG.warn(getName() + " invalid lengths in map output header: id: " +
          srcAttemptId + " len: " + compressedLength + ", decomp len: " + 
               decompressedLength);
      return false;
    }

    // partitionId verification. Isn't availalbe here because it is encoded into
    // URI
    if (forReduce != currentPartition) {
      wrongReduceErrs.increment(1);
      LOG.warn(getName() + " data for the wrong partition map: " + srcAttemptId + " len: "
          + compressedLength + " decomp len: " + decompressedLength + " for partition " + forReduce
          + ", expected partition: " + currentPartition);
      return false;
    }

    // Sanity check
    if (!remaining.contains(srcAttemptId)) {
      wrongMapErrs.increment(1);
      LOG.warn("Invalid map-output! Received output for " + srcAttemptId);
      return false;
    }
    
    return true;
  }
  
  private InputAttemptIdentifier getNextRemainingAttempt() {
    if (remaining.size() > 0) {
      return remaining.iterator().next();
    } else {
      return null;
    }
  }

  /**
   * Create the map-output-url. This will contain all the map ids
   * separated by commas
   * @param host
   * @param maps
   * @return
   * @throws MalformedURLException
   */
  private URL getMapOutputURL(MapHost host, List<InputAttemptIdentifier> srcAttempts
                              )  throws MalformedURLException {
    // Get the base url
    StringBuffer url = new StringBuffer(host.getBaseUrl());
    
    boolean first = true;
    for (InputAttemptIdentifier mapId : srcAttempts) {
      if (!first) {
        url.append(",");
      }
      url.append(mapId.getPathComponent());
      first = false;
    }
   
    if (LOG.isDebugEnabled()) {
      LOG.debug("MapOutput URL for " + host + " -> " + url.toString());
    }
    return new URL(url.toString());
  }
  
  /** 
   * The connection establishment is attempted multiple times and is given up 
   * only on the last failure. Instead of connecting with a timeout of 
   * X, we try connecting with a timeout of x < X but multiple times. 
   */
  private void connect(URLConnection connection, int connectionTimeout)
  throws IOException {
    int unit = 0;
    if (connectionTimeout < 0) {
      throw new IOException("Invalid timeout "
                            + "[timeout = " + connectionTimeout + " ms]");
    } else if (connectionTimeout > 0) {
      unit = Math.min(UNIT_CONNECT_TIMEOUT, connectionTimeout);
    }
    // set the connect timeout to the unit-connect-timeout
    connection.setConnectTimeout(unit);
    while (true) {
      try {
        connection.connect();
        break;
      } catch (IOException ioe) {
        // update the total remaining connect-timeout
        connectionTimeout -= unit;

        // throw an exception if we have waited for timeout amount of time
        // note that the updated value if timeout is used here
        if (connectionTimeout == 0) {
          throw ioe;
        }

        // reset the connect timeout for the last try
        if (connectionTimeout < unit) {
          unit = connectionTimeout;
          // reset the connect time out for the final connect
          connection.setConnectTimeout(unit);
        }
      }
    }
  }

  private void shuffleToMemory(MapHost host, MapOutput mapOutput, 
                               InputStream input, 
                               int decompressedLength, 
                               int compressedLength) throws IOException {    
    IFileInputStream checksumIn = 
      new IFileInputStream(input, compressedLength, ifileReadAhead, ifileReadAheadLength);

    input = checksumIn;       
  
    // Are map-outputs compressed?
    if (codec != null) {
      decompressor.reset();
      input = codec.createInputStream(input, decompressor);
    }
  
    // Copy map-output into an in-memory buffer
    byte[] shuffleData = mapOutput.getMemory();
    
    try {
      IOUtils.readFully(input, shuffleData, 0, shuffleData.length);
      metrics.inputBytes(shuffleData.length);
      LOG.info("Read " + shuffleData.length + " bytes from map-output for " +
               mapOutput.getAttemptIdentifier());
    } catch (IOException ioe) {      
      // Close the streams
      IOUtils.cleanup(LOG, input);

      // Re-throw
      throw ioe;
    }

  }
  
  private void shuffleToDisk(MapHost host, MapOutput mapOutput, 
                             InputStream input, 
                             long compressedLength) 
  throws IOException {
    // Copy data to local-disk
    OutputStream output = mapOutput.getDisk();
    long bytesLeft = compressedLength;
    try {
      final int BYTES_TO_READ = 64 * 1024;
      byte[] buf = new byte[BYTES_TO_READ];
      while (bytesLeft > 0) {
        int n = input.read(buf, 0, (int) Math.min(bytesLeft, BYTES_TO_READ));
        if (n < 0) {
          throw new IOException("read past end of stream reading " + 
                                mapOutput.getAttemptIdentifier());
        }
        output.write(buf, 0, n);
        bytesLeft -= n;
        metrics.inputBytes(n);
      }

      LOG.info("Read " + (compressedLength - bytesLeft) + 
               " bytes from map-output for " +
               mapOutput.getAttemptIdentifier());

      output.close();
    } catch (IOException ioe) {
      // Close the streams
      IOUtils.cleanup(LOG, input, output);

      // Re-throw
      throw ioe;
    }

    // Sanity check
    if (bytesLeft != 0) {
      throw new IOException("Incomplete map output received for " +
                            mapOutput.getAttemptIdentifier() + " from " +
                            host.getHostIdentifier() + " (" + 
                            bytesLeft + " bytes missing of " + 
                            compressedLength + ")"
      );
    }
  }
}
