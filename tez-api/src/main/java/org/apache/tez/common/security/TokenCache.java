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

package org.apache.tez.common.security;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.tez.dag.api.TezConfiguration;


/**
 * This class provides user facing APIs for transferring secrets from
 * the job client to the tasks.
 * The secrets can be stored just before submission of jobs and read during
 * the task execution.  
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TokenCache {
  
  private static final Logger LOG = LoggerFactory.getLogger(TokenCache.class);

  
  /**
   * auxiliary method to get user's secret keys..
   * @param alias
   * @return secret key from the storage
   */
  public static byte[] getSecretKey(Credentials credentials, Text alias) {
    if(credentials == null)
      return null;
    return credentials.getSecretKey(alias);
  }
  
  /**
   * Convenience method to obtain delegation tokens from namenodes 
   * corresponding to the paths passed.
   * @param credentials
   * @param ps array of paths
   * @param conf configuration
   * @throws IOException
   */
  public static void obtainTokensForFileSystems(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    if (!UserGroupInformation.isSecurityEnabled()) {
      return;
    }
    obtainTokensForFileSystemsInternal(credentials, ps, conf);
  }

  private static final int MAX_FS_OBJECTS = 10;
  static void obtainTokensForFileSystemsInternal(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    Set<FileSystem> fsSet = new HashSet<FileSystem>();
    boolean limitExceeded = false;
    for(Path p: ps) {
      FileSystem fs = p.getFileSystem(conf);
      if (!limitExceeded && fsSet.size() == MAX_FS_OBJECTS) {
        LOG.warn("No of FileSystem objects exceeds {}, updating tokens for all paths. This can" +
            " happen when fs.<scheme>.impl.disable.cache is set to true.", MAX_FS_OBJECTS);
        limitExceeded = true;
      }
      if (limitExceeded) {
        // Too many fs objects are being created, most likely the cache is disabled. Prevent an
        // OOM and just directly invoke instead of adding to the set.
        obtainTokensForFileSystemsInternal(fs, credentials, conf);
      } else {
        fsSet.add(fs);
      }
    }
    for (FileSystem fs : fsSet) {
      obtainTokensForFileSystemsInternal(fs, credentials, conf);
    }
  }

  static boolean isTokenRenewalExcluded(FileSystem fs, Configuration conf) {
    String[] nns =
            conf.getStrings(TezConfiguration.TEZ_JOB_FS_SERVERS_TOKEN_RENEWAL_EXCLUDE);
    if (nns != null) {
      String host = fs.getUri().getHost();
      for(int i = 0; i < nns.length; i++) {
        if (nns[i].equals(host)) {
          return true;
        }
      }
    }
    return false;
  }

  /**
   * get delegation token for a specific FS
   * @param fs
   * @param credentials
   * @param p
   * @param conf
   * @throws IOException
   */
  static void obtainTokensForFileSystemsInternal(FileSystem fs, 
      Credentials credentials, Configuration conf) throws IOException {
    // TODO Change this to use YARN utilities once YARN-1664 is fixed.
    // RM skips renewing token with empty renewer
    String delegTokenRenewer = "";
    if (!isTokenRenewalExcluded(fs, conf)) {
      delegTokenRenewer = Master.getMasterPrincipal(conf);
      if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
        throw new IOException(
                "Can't get Master Kerberos principal for use as renewer");
      }
    }

    final Token<?> tokens[] = fs.addDelegationTokens(delegTokenRenewer,
                                                     credentials);
    if (tokens != null) {
      for (Token<?> token : tokens) {
        LOG.info("Got dt for " + fs.getUri() + "; "+token);
      }
    }
  }

  private static final Text SESSION_TOKEN = new Text("SessionToken");

  /**
   * store session specific token
   * @param t
   */
  @InterfaceAudience.Private
  public static void setSessionToken(Token<? extends TokenIdentifier> t, 
      Credentials credentials) {
    credentials.addToken(SESSION_TOKEN, t);
  }
  /**
   * 
   * @return session token
   */
  @SuppressWarnings("unchecked")
  @InterfaceAudience.Private
  public static Token<JobTokenIdentifier> getSessionToken(Credentials credentials) {
    Token<?> token = credentials.getToken(SESSION_TOKEN);
    if (token == null) {
      return null;
    }
    return (Token<JobTokenIdentifier>) token;
  }

  /**
   * Merge tokens from a configured binary file into provided Credentials object
   * @param creds Credentials object to add new tokens to
   * @param tokenFilePath Location of tokens' binary file
   */
  @InterfaceAudience.Private
  public static void mergeBinaryTokens(Credentials creds,
      Configuration conf, String tokenFilePath)
      throws IOException {
    if (tokenFilePath == null || tokenFilePath.isEmpty()) {
      throw new RuntimeException("Invalid file path provided"
          + ", tokenFilePath=" + tokenFilePath);
    }
    LOG.info("Merging additional tokens from binary file"
        + ", binaryFileName=" + tokenFilePath);
    Credentials binary = Credentials.readTokenStorageFile(
        new Path("file:///" +  tokenFilePath), conf);

    // supplement existing tokens with the tokens in the binary file
    creds.mergeAll(binary);
  }

}
