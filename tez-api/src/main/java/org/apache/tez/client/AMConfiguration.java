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

package org.apache.tez.client;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.tez.common.TezYARNUtils;
import org.apache.tez.dag.api.TezConfiguration;
import org.apache.tez.dag.api.TezUncheckedException;

public class AMConfiguration {

  private final Path stagingDir;
  private final String queueName;
  private final Map<String, String> env;
  private final Map<String, LocalResource> localResources;
  private final TezConfiguration amConf;
  private final Credentials credentials;

  /**
   * @param env
   *          environment for the AM
   * @param localResources
   *          localResources which are required to run the AM
   * @param conf
   * @param credentials
   *          credentials which will be needed in the AM. This includes
   *          credentials which will be required to localize the specified
   *          localResources.
   */
  public AMConfiguration(Map<String, String> env,
      Map<String, LocalResource> localResources,
      TezConfiguration conf, Credentials credentials) {
    if (conf != null) {
      this.amConf = conf;
    } else {
      this.amConf = new TezConfiguration();
    }
    this.queueName = this.amConf.get(TezConfiguration.TEZ_QUEUE_NAME);

    this.env = new HashMap<String, String>();
    TezYARNUtils.setEnvFromInputString(this.env,
        this.amConf.get(TezConfiguration.TEZ_AM_ENV),
        File.pathSeparator);
    if (env != null) {
      this.env.putAll(env);
    }

    this.localResources = localResources;
    String stagingDirStr = amConf.get(TezConfiguration.TEZ_AM_STAGING_DIR);
    if (stagingDirStr == null || stagingDirStr.isEmpty()) {
      throw new TezUncheckedException("Staging directory for AM resources"
          + " not specified in config"
          + ", property=" + TezConfiguration.TEZ_AM_STAGING_DIR);
    }
    try {
      Path p = new Path(stagingDirStr);
      FileSystem fs = p.getFileSystem(amConf);
      this.stagingDir = fs.resolvePath(p);
    } catch (IOException e) {
      throw new TezUncheckedException(e);
    }
    this.credentials = credentials;
  }

  public Path getStagingDir() {
    return stagingDir;
  }

  public String getQueueName() {
    return queueName;
  }

  public Map<String, String> getEnv() {
    return env;
  }

  public Map<String, LocalResource> getLocalResources() {
    return localResources;
  }

  public TezConfiguration getAMConf() {
    return amConf;
  }

  public Credentials getCredentials() {
    return credentials;
  }

  public void isCompatible(AMConfiguration other) {
    // TODO implement
  }

}
