/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common.security;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.TreeMap;

import javax.crypto.Mac;
import javax.crypto.SecretKey;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.security.token.SecretManager;
import org.apache.hadoop.security.token.Token;

/**
 * SecretManager for job token. It can be used to cache generated job tokens.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobTokenSecretManager extends SecretManager<JobTokenIdentifier> {
  private static final String DEFAULT_HMAC_ALGORITHM = "HmacSHA1";
  private final SecretKey masterKey;
  private final Map<String, SecretKey> currentJobTokens;
  private final Mac mac;

  /**
   * Convert the byte[] to a secret key
   * @param key the byte[] to create the secret key from
   * @return the secret key
   */
  public static SecretKey createSecretKey(byte[] key) {
    return SecretManager.createSecretKey(key);
  }

  /**
   * Compute the HMAC hash of the message using the key
   * @param msg the message to hash
   * @param key the key to use
   * @return the computed hash
   */
  public static byte[] computeHash(byte[] msg, SecretKey key) {
    return createPassword(msg, key);
  }

  /**
   * Compute the HMAC hash of the message using the key
   * @param msg the message to hash
   * @return the computed hash
   */
  public byte[] computeHash(byte[] msg) {
    synchronized (mac) {
      return mac.doFinal(msg);
    }
  }

  /**
   * Default constructor
   */
  public JobTokenSecretManager() {
    this(null);
  }

  public JobTokenSecretManager(SecretKey key) {
    this.masterKey = (key == null) ? generateSecret() : key;
    this.currentJobTokens = new TreeMap<String, SecretKey>();
    try {
      mac = Mac.getInstance(DEFAULT_HMAC_ALGORITHM);
      mac.init(masterKey);
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException("Can't find " + DEFAULT_HMAC_ALGORITHM + " algorithm.", nsa);
    } catch (InvalidKeyException ike) {
      throw new IllegalArgumentException("Invalid key to HMAC computation", ike);
    }
  }

  /**
   * Create a new password/secret for the given job token identifier.
   * @param identifier the job token identifier
   * @return token password/secret
   */
  @Override
  public byte[] createPassword(JobTokenIdentifier identifier) {
    return computeHash(identifier.getBytes());
  }

  /**
   * Add the job token of a job to cache
   * @param jobId the job that owns the token
   * @param token the job token
   */
  public void addTokenForJob(String jobId, Token<JobTokenIdentifier> token) {
    SecretKey tokenSecret = createSecretKey(token.getPassword());
    synchronized (currentJobTokens) {
      currentJobTokens.put(jobId, tokenSecret);
    }
  }

  /**
   * Remove the cached job token of a job from cache
   * @param jobId the job whose token is to be removed
   */
  public void removeTokenForJob(String jobId) {
    synchronized (currentJobTokens) {
      currentJobTokens.remove(jobId);
    }
  }

  /**
   * Look up the token password/secret for the given jobId.
   * @param jobId the jobId to look up
   * @return token password/secret as SecretKey
   * @throws InvalidToken
   */
  public SecretKey retrieveTokenSecret(String jobId) throws InvalidToken {
    SecretKey tokenSecret = null;
    synchronized (currentJobTokens) {
      tokenSecret = currentJobTokens.get(jobId);
    }
    if (tokenSecret == null) {
      throw new InvalidToken("Can't find job token for job " + jobId + " !!");
    }
    return tokenSecret;
  }

  /**
   * Look up the token password/secret for the given job token identifier.
   * @param identifier the job token identifier to look up
   * @return token password/secret as byte[]
   * @throws InvalidToken
   */
  @Override
  public byte[] retrievePassword(JobTokenIdentifier identifier)
      throws InvalidToken {
    return retrieveTokenSecret(identifier.getJobId().toString()).getEncoded();
  }

  /**
   * Create an empty job token identifier
   * @return a newly created empty job token identifier
   */
  @Override
  public JobTokenIdentifier createIdentifier() {
    return new JobTokenIdentifier();
  }
}
