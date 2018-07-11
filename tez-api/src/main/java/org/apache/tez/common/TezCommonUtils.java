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

package org.apache.tez.common;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
import org.apache.tez.common.security.JobTokenIdentifier;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezConstants;
import org.apache.tez.dag.api.TezUncheckedException;

import com.google.protobuf.ByteString;

@Private
public class TezCommonUtils {
  public static final FsPermission TEZ_AM_DIR_PERMISSION = FsPermission
      .createImmutable((short) 0700); // rwx--------
  public static final FsPermission TEZ_AM_FILE_PERMISSION = FsPermission
      .createImmutable((short) 0644); // rw-r--r--
  private static final Logger LOG = LoggerFactory.getLogger(TezClient.class);

  public static final String TEZ_SYSTEM_SUB_DIR = ".tez";

  /**
   * <p>
   * This function returns the staging directory defined in the config with
   * property name <code>TezConfiguration.TEZ_AM_STAGING_DIR</code>. If the
   * property is not defined in the conf, Tez uses the value defined as
   * <code>TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT</code>. In addition, the
   * function makes sure if the staging directory exists. If not, it creates the
   * directory with permission <code>TEZ_AM_DIR_PERMISSION</code>.
   * </p>
   * 
   * @param conf
   *          TEZ configuration
   * @return Fully qualified staging directory
   */
  public static Path getTezBaseStagingPath(Configuration conf) {
    String stagingDirStr = conf.get(TezConfiguration.TEZ_AM_STAGING_DIR,
        TezConfiguration.TEZ_AM_STAGING_DIR_DEFAULT);
    Path baseStagingDir;
    try {
      Path p = new Path(stagingDirStr);
      FileSystem fs = p.getFileSystem(conf);
      if (!fs.exists(p)) {
        mkDirForAM(fs, p);
        LOG.info("Stage directory " + p + " doesn't exist and is created");
      }
      baseStagingDir = fs.resolvePath(p);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    return baseStagingDir;
  }

  /**
   * <p>
   * This function returns the staging directory for TEZ system. Tez creates all
   * its temporary files under this sub-directory. The function creates a
   * sub-directory (<code>TEZ_SYSTEM_SUB_DIR</code>/<APP_ID>) under the base
   * staging directory, often provided by user.
   * </p>
   * 
   * @param conf
   *          Tez configuration
   * @param strAppId
   *          Application ID as string
   * @return TEZ system level staging directory used for Tez internals
   */
  @Private
  public static Path createTezSystemStagingPath(Configuration conf, String strAppId) {
    Path baseStagingPath = getTezBaseStagingPath(conf);
    Path tezStagingDir;
    try {
      tezStagingDir = new Path(baseStagingPath, TEZ_SYSTEM_SUB_DIR);
      FileSystem fs = tezStagingDir.getFileSystem(conf);
      tezStagingDir = new Path(tezStagingDir, strAppId);
      if (!fs.exists(tezStagingDir)) {
        mkDirForAM(fs, tezStagingDir);
        LOG.info("Tez system stage directory " + tezStagingDir + " doesn't exist and is created");
      }
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    return tezStagingDir;
  }

  /**
   * <p>
   * This function returns the staging directory for TEZ system. Tez creates all
   * its temporary files under this sub-directory. The function normally doesn't
   * creates any sub-directory under the base staging directory.
   * </p>
   * 
   * @param conf
   *          Tez configuration
   * @param strAppId
   *          Application ID as string
   * @return TEZ system level staging directory used for Tez internals
   */
  @Private
  public static Path getTezSystemStagingPath(Configuration conf, String strAppId) {
    Path baseStagingPath = getTezBaseStagingPath(conf);
    Path tezStagingDir;
    tezStagingDir = new Path(baseStagingPath, TEZ_SYSTEM_SUB_DIR);
    tezStagingDir = new Path(tezStagingDir, strAppId);
    return tezStagingDir;
  }

  /**
   * <p>
   * Returns a path to store binary configuration
   * </p>
   * 
   * @param tezSysStagingPath
   *          TEZ system level staging directory used for Tez internals
   * @return path to configuration
   */
  @Private
  public static Path getTezConfStagingPath(Path tezSysStagingPath) {
    return new Path(tezSysStagingPath, TezConstants.TEZ_PB_BINARY_CONF_NAME);
  }

  /**
   * <p>
   * Returns a path to store local resources/session jars
   * </p>
   * 
   * @param tezSysStagingPath
   *          TEZ system level staging directory used for Tez internals
   * @return path to store the session jars
   */
  @Private
  public static Path getTezAMJarStagingPath(Path tezSysStagingPath) {
    return new Path(tezSysStagingPath, TezConstants.TEZ_AM_LOCAL_RESOURCES_PB_FILE_NAME);
  }

  /**
   * <p>
   * Returns a path to store binary plan
   * </p>
   * 
   * @param tezSysStagingPath
   *          TEZ system level staging directory used for Tez internals
   * @return path to store the plan in binary
   */
  @Private
  public static Path getTezBinPlanStagingPath(Path tezSysStagingPath) {
    return new Path(tezSysStagingPath, TezConstants.TEZ_PB_PLAN_BINARY_NAME);
  }

  /**
   * <p>
   * Returns a path to store text plan
   * </p>
   * 
   * @param tezSysStagingPath
   *          TEZ system level staging directory used for Tez internals
   * @param strAppId
   *          Application ID
   * @param dagPBName
   *          DAG PB Name
   * @return path to store the plan in text
   */
  @Private
  public static Path getTezTextPlanStagingPath(Path tezSysStagingPath, String strAppId,
      String dagPBName) {
    String fileName = strAppId + "-" + dagPBName + "-" + TezConstants.TEZ_PB_PLAN_TEXT_NAME;
    return new Path(tezSysStagingPath, fileName);
  }

  /**
   * <p>
   * Returns a path to store recovery information
   * </p>
   * 
   * @param tezSysStagingPath
   *          TEZ system level staging directory used for Tez internals
   * @param conf
   *          Tez configuration
   * @return App recovery path
   * @throws IOException
   */
  @Private
  public static Path getRecoveryPath(Path tezSysStagingPath, Configuration conf)
      throws IOException {
    Path baseReecoveryPath = new Path(tezSysStagingPath,
        TezConstants.DAG_RECOVERY_DATA_DIR_NAME);
    FileSystem recoveryFS = baseReecoveryPath.getFileSystem(conf);
    return recoveryFS.makeQualified(baseReecoveryPath);
  }

  /**
   * <p>
   * Returns a path to store app attempt specific recovery details
   * </p>
   * 
   * @param recoveryPath
   *          TEZ recovery directory used for Tez internals
   * @param attemptID
   *          Application Attempt Id
   * @return App attempt specific recovery path
   */
  @Private
  public static Path getAttemptRecoveryPath(Path recoveryPath, int attemptID) {
    return new Path(recoveryPath, Integer.toString(attemptID));
  }

  /**
   * <p>
   * Returns a path to store DAG specific recovery info
   * </p>
   * 
   * @param attemptRecoverPath
   *          :TEZ system level staging directory used for Tez internals
   * @param dagID
   *          DagID as string
   * @return DAG specific recovery path
   */
  @Private
  public static Path getDAGRecoveryPath(Path attemptRecoverPath, String dagID) {
    return new Path(attemptRecoverPath, dagID + TezConstants.DAG_RECOVERY_RECOVER_FILE_SUFFIX);
  }

  /**
   * <p>
   * Returns a path to store summary info for recovery
   * </p>
   * 
   * @param attemptRecoverPath
   *          TEZ system level staging directory used for Tez internals
   * @return Summary event path used in recovery
   */
  @Private
  public static Path getSummaryRecoveryPath(Path attemptRecoverPath) {
    return new Path(attemptRecoverPath, TezConstants.DAG_RECOVERY_SUMMARY_FILE_SUFFIX);
  }

  /**
   * <p>
   * Create a directory with predefined directory permission
   * </p>
   * 
   * @param fs
   *          Filesystem
   * @param dir
   *          directory to be created
   * @throws IOException
   */
  public static void mkDirForAM(FileSystem fs, Path dir) throws IOException {
    fs.mkdirs(dir, new FsPermission(TEZ_AM_DIR_PERMISSION));
  }

  /**
   * <p>
   * Create a file with <code>TEZ_AM_FILE_PERMISSION</code> permission and
   * returns OutputStream
   * </p>
   * 
   * @param fs
   *          Filesystem
   * @param filePath
   *          file path to create the file
   * @return FSDataOutputStream
   * @throws IOException
   */
  public static FSDataOutputStream createFileForAM(FileSystem fs, Path filePath) throws IOException {
    return FileSystem.create(fs, filePath, new FsPermission(TEZ_AM_FILE_PERMISSION));
  }
  
  public static void addAdditionalLocalResources(Map<String, LocalResource> additionalLrs,
      Map<String, LocalResource> originalLRs, String logContext) {
    // TODO TEZ-1798. Handle contents of Tez archives for duplicate LocalResource checks
    if (additionalLrs != null && !additionalLrs.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, LocalResource> lrEntry : additionalLrs.entrySet()) {
        LocalResource originalLr = originalLRs.get(lrEntry.getKey());
        if (originalLr != null) {
          LocalResource additionalLr = lrEntry.getValue();
          if (originalLr.getSize() != additionalLr.getSize()) {
            throw new TezUncheckedException(
                "Duplicate Resources found with different size for [" + logContext + "]: " + lrEntry.getKey() +
                    " : " + "[" + additionalLr.getResource() + "=" + additionalLr.getSize() +
                    "],[" + originalLr.getResource() + "=" + originalLr.getSize());
          } else {
            if (originalLr.getResource().equals(additionalLr.getResource())) {
              sb.append("[").append(lrEntry.getKey()).append(" : Duplicate]");
            } else {
              sb.append("[").append(lrEntry.getKey()).append(" : DuplicateDifferentPath]");
            }
          }
        }
        // The LR either does not exist, or is an 'equivalent' dupe.
        // Prefer the tez specified LR instead of the equivalent user specified LR for container reuse matching
        originalLRs.put(lrEntry.getKey(), lrEntry.getValue());
      }
      String logString = sb.toString();
      if (!logString.isEmpty()) {
        LOG.warn("Found Resources Duplication in " + logContext + " after including resources from " +
            TezConfiguration.TEZ_LIB_URIS + " and " + TezConfiguration.TEZ_AUX_URIS + ": " +
            logString);
      }
    }
  }

  private static final boolean NO_WRAP = true;

  @Private
  public static Deflater newBestCompressionDeflater() {
    return new Deflater(Deflater.BEST_COMPRESSION, NO_WRAP);
  }

  @Private
  public static Deflater newBestSpeedDeflater() {
    return new Deflater(Deflater.BEST_SPEED, NO_WRAP);
  }

  @Private
  public static Inflater newInflater() {
    return new Inflater(NO_WRAP);
  }

  @Private
  public static ByteString compressByteArrayToByteString(byte[] inBytes) throws IOException {
    return compressByteArrayToByteString(inBytes, newBestCompressionDeflater());
  }

  @Private
  public static ByteString compressByteArrayToByteString(byte[] inBytes, Deflater deflater) throws IOException {
    deflater.reset();
    ByteString.Output os = ByteString.newOutput();
    DeflaterOutputStream compressOs = null;
    try {
      compressOs = new DeflaterOutputStream(os, deflater);
      compressOs.write(inBytes);
      compressOs.finish();
      ByteString byteString = os.toByteString();
      return byteString;
    } finally {
      if (compressOs != null) {
        compressOs.close();
      }
    }
  }

  @Private
  public static byte[] decompressByteStringToByteArray(ByteString byteString) throws IOException {
    Inflater inflater = newInflater();
    try {
      return decompressByteStringToByteArray(byteString, inflater);
    } finally {
      inflater.end();
    }
  }

  @Private
  public static byte[] decompressByteStringToByteArray(ByteString byteString, Inflater inflater) throws IOException {
    inflater.reset();
    try (InflaterInputStream inflaterInputStream = new InflaterInputStream(byteString.newInput(), inflater)) {
      return IOUtils.toByteArray(inflaterInputStream);
    }
  }

  public static String getCredentialsInfo(Credentials credentials, String identifier) {
    StringBuilder sb = new StringBuilder();
    sb.append("Credentials: #" + identifier + "Tokens=").append(credentials.numberOfTokens());
    if (credentials.numberOfTokens() > 0) {
      sb.append(", Services=");
      Iterator<Token<?>> tokenItr = credentials.getAllTokens().iterator();
      if (tokenItr.hasNext()) {
        Token token = tokenItr.next();
        sb.append(token.getService()).append("(").append(token.getKind()).append(")");

      }
      while(tokenItr.hasNext()) {
        Token token = tokenItr.next();
        sb.append(",").append(token.getService()).append("(").append(token.getKind()).append(")");
      }
    }
    return sb.toString();
  }

  public static ByteBuffer convertJobTokenToBytes(
      Token<JobTokenIdentifier> jobToken) throws IOException {
    DataOutputBuffer dob = new DataOutputBuffer();
    jobToken.write(dob);
    ByteBuffer bb = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
    return bb;
  }

  public static Credentials parseCredentialsBytes(byte[] credentialsBytes) throws IOException {
    Credentials credentials = new Credentials();
    DataInputBuffer dib = new DataInputBuffer();
    try {
      byte[] tokenBytes = credentialsBytes;
      dib.reset(tokenBytes, tokenBytes.length);
      credentials.readTokenStorageStream(dib);
      return credentials;
    } finally {
      dib.close();
    }
  }

  public static void logCredentials(Logger log, Credentials credentials, String identifier) {
    if (log.isDebugEnabled()) {
      log.debug(getCredentialsInfo(credentials, identifier));
    }
  }

  public static Collection<String> tokenizeString(String str, String delim) {
    List<String> values = new ArrayList<String>();
    if (str == null || str.isEmpty())
      return values;
    StringTokenizer tokenizer = new StringTokenizer(str, delim);
    while (tokenizer.hasMoreTokens()) {
      values.add(tokenizer.nextToken());
    }
    return values;
  }

  /**
   * Splits a comma separated value <code>String</code>, trimming leading and trailing whitespace on each value.
   * @param str a comma separated <String> with values
   * @return an array of <code>String</code> values
   */
  public static String[] getTrimmedStrings(String str) {
    if (null == str || (str = str.trim()).isEmpty()) {
      return ArrayUtils.EMPTY_STRING_ARRAY;
    }

    return str.split("\\s*,\\s*");
  }

  /**
   * A helper function to serialize the JobTokenIdentifier to be sent to the
   * ShuffleHandler as ServiceData.
   *
   * *NOTE* This is a copy of what is done by the MapReduce ShuffleHandler. Not using that directly
   * to avoid a dependency on mapreduce.
   *
   * @param jobToken
   *          the job token to be used for authentication of shuffle data
   *          requests.
   * @return the serialized version of the jobToken.
   */
  public static ByteBuffer serializeServiceData(Token<JobTokenIdentifier> jobToken)
      throws IOException {
    // TODO these bytes should be versioned
    DataOutputBuffer jobToken_dob = new DataOutputBuffer();
    jobToken.write(jobToken_dob);
    return ByteBuffer.wrap(jobToken_dob.getData(), 0, jobToken_dob.getLength());
  }

  public static String getSystemPropertiesToLog(Configuration conf) {
    Collection <String> keys = conf.getTrimmedStringCollection(
        TezConfiguration.TEZ_JVM_SYSTEM_PROPERTIES_TO_LOG);
    if (keys.isEmpty()) {
      keys = TezConfiguration.TEZ_JVM_SYSTEM_PROPERTIES_TO_LOG_DEFAULT;
    }
    StringBuilder sb = new StringBuilder();
    sb.append("\n/************************************************************\n");
    sb.append("[system properties]\n");
    for (String key : keys) {
      sb.append(key).append(": ").append(System.getProperty(key)).append('\n');
    }
    sb.append("************************************************************/");
    return sb.toString();
  }

  /**
   * Helper function to get the heartbeat interval for client-AM heartbeats
   * See {@link TezConfiguration#TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS} for more details.
   * @param conf Configuration object
   * @return heartbeat interval in milliseconds. -1 implies disabled.
   */
  public static long getAMClientHeartBeatTimeoutMillis(Configuration conf) {
    int val = conf.getInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS,
        TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS_DEFAULT);
    if (val < 0) {
      return -1;
    }
    if (val > 0 && val < TezConstants.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS_MINIMUM) {
      return TezConstants.TEZ_AM_CLIENT_HEARTBEAT_TIMEOUT_SECS_MINIMUM * 1000;
    }
    return val * 1000;
  }

  /**
   * Helper function to get the poll interval for client-AM heartbeats.
   * @param conf Configuration object
   * @param heartbeatIntervalMillis Heartbeat interval in milliseconds
   * @param buckets How many times to poll within the provided heartbeat interval
   * @return poll interval in milliseconds
   */
  public static long getAMClientHeartBeatPollIntervalMillis(Configuration conf,
                                                            long heartbeatIntervalMillis,
                                                            int buckets) {
    if (heartbeatIntervalMillis <= 0) {
      return -1;
    }
    int pollInterval = conf.getInt(TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_POLL_INTERVAL_MILLIS,
        TezConfiguration.TEZ_AM_CLIENT_HEARTBEAT_POLL_INTERVAL_MILLIS_DEFAULT);
    if (pollInterval > 0) {
      return Math.max(TezConstants.TEZ_AM_CLIENT_HEARTBEAT_POLL_INTERVAL_MILLIS_MINIMUM,
          pollInterval);
    }
    return Math.max(TezConstants.TEZ_AM_CLIENT_HEARTBEAT_POLL_INTERVAL_MILLIS_MINIMUM,
        heartbeatIntervalMillis/buckets);
  }

  public static long getDAGSessionTimeout(Configuration conf) {
    int timeoutSecs = conf.getInt(
        TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS,
        TezConfiguration.TEZ_SESSION_AM_DAG_SUBMIT_TIMEOUT_SECS_DEFAULT);
    if (timeoutSecs < 0) {
      return -1;
    }
    // Handle badly configured value to minimize impact of a spinning thread
    if (timeoutSecs == 0) {
      timeoutSecs = 1;
    }
    return 1000l * timeoutSecs;
  }

}
