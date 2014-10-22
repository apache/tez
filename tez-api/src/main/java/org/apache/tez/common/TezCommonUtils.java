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
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.client.TezClient;
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
  private static final Log LOG = LogFactory.getLog(TezClient.class);

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
      Map<String, LocalResource> originalLRs) {
    if (additionalLrs != null && !additionalLrs.isEmpty()) {
      for (Map.Entry<String, LocalResource> lr : additionalLrs.entrySet()) {
        if (originalLRs.containsKey(lr.getKey())) {
          throw new TezUncheckedException("Attempting to add duplicate resource: " + lr.getKey());
        } else {
          originalLRs.put(lr.getKey(), lr.getValue());
        }
      }
    }
  }

  @Private
  public static ByteString compressByteArrayToByteString(byte[] inBytes) throws IOException {
    ByteString.Output os = ByteString.newOutput();
    DeflaterOutputStream compressOs = new DeflaterOutputStream(os, new Deflater(
        Deflater.BEST_COMPRESSION));
    compressOs.write(inBytes);
    compressOs.finish();
    ByteString byteString = os.toByteString();
    return byteString;
  }

  @Private
  public static byte[] decompressByteStringToByteArray(ByteString byteString) throws IOException {
    InflaterInputStream in = new InflaterInputStream(byteString.newInput());
    byte[] bytes = IOUtils.toByteArray(in);
    return bytes;
  }

  public static void logCredentials(Log log, Credentials credentials, String identifier) {
    if (log.isDebugEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("#" + identifier + "Tokens=").append(credentials.numberOfTokens());
      if (credentials.numberOfTokens() > 0) {
        sb.append(", Services: ");
        for (Token<?> t : credentials.getAllTokens()) {
          sb.append(t.getService()).append(",");
        }
      }
      log.debug(sb.toString());
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
}
