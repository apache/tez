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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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


/**
 * This class provides user facing APIs for transferring secrets from
 * the job client to the tasks.
 * The secrets can be stored just before submission of jobs and read during
 * the task execution.  
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TokenCache {
  
  private static final Log LOG = LogFactory.getLog(TokenCache.class);

  
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

  static void obtainTokensForFileSystemsInternal(Credentials credentials,
      Path[] ps, Configuration conf) throws IOException {
    Set<FileSystem> fsSet = new HashSet<FileSystem>();
    for(Path p: ps) {
      fsSet.add(p.getFileSystem(conf));
    }
    for (FileSystem fs : fsSet) {
      obtainTokensForFileSystemsInternal(fs, credentials, conf);
    }
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
    String delegTokenRenewer = Master.getMasterPrincipal(conf);
    if (delegTokenRenewer == null || delegTokenRenewer.length() == 0) {
      throw new IOException(
          "Can't get Master Kerberos principal for use as renewer");
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
