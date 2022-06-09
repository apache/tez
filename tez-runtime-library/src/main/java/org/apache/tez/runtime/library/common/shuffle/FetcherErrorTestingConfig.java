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
package org.apache.tez.runtime.library.common.shuffle;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.runtime.api.ObjectRegistry;
import org.apache.tez.runtime.library.api.TezRuntimeConfiguration;
import org.apache.tez.runtime.library.common.InputAttemptIdentifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FetcherErrorTestingConfig {
  private static final Logger LOG = LoggerFactory.getLogger(FetcherErrorTestingConfig.class);
  private static final String KEY_CACHED_HOSTNAME = "FetcherErrorTestingConfig.host";

  private String hostToFail = "*";
  private String srcNameTrimmedToFail = "*";
  private int probabilityPercent = 50;
  private Random random = new Random();
  /**
   * Whether to fail only in case of input attempts with index 0,
   * this prevents continuous failure, and helps simulating a real-life node failure.
   */
  private boolean failForFirstAttemptOnly = false;
  private ObjectRegistry objectRegistry;

  public FetcherErrorTestingConfig(Configuration conf, ObjectRegistry objectRegistry) {
    String errorConfig = conf.get(TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_TESTING_ERRORS_CONFIG,
        TezRuntimeConfiguration.TEZ_RUNTIME_SHUFFLE_FETCH_TESTING_ERRORS_CONFIG_DEFAULT);
    String[] configParts = errorConfig.split("#");

    // e.g. host_1
    if (configParts.length > 0) {
      hostToFail = configParts[0];
    }

    // e.g. Map 1 or Map_1, both will work
    if (configParts.length > 1) {
      srcNameTrimmedToFail = TezUtilsInternal.cleanVertexName(configParts[1]);
    }

    // e.g. 50
    if (configParts.length > 2) {
      probabilityPercent = Integer.parseInt(configParts[2]);
    }

    // e.g. fail_only_first
    if (configParts.length > 3) {
      List<String> features = Arrays.asList(configParts[3].split(","));
      if (features.contains("fail_only_first")) {
        failForFirstAttemptOnly = true;
      }
    }

    this.objectRegistry = objectRegistry;
    if (hostToFail.equals("_first_")) {
      String host = (String) objectRegistry.get(KEY_CACHED_HOSTNAME);
      if (host != null) {
        LOG.info("Get already stored hostname for fetcher test failures: " + host);
        hostToFail = host;
      }
    }
  }

  public boolean shouldFail(String host, String srcNameTrimmed, InputAttemptIdentifier inputAttemptIdentifier) {
    if (matchHost(host) && matchSourceVertex(srcNameTrimmed)) {
      return (!failForFirstAttemptOnly || failForFirstAttemptOnly && inputAttemptIdentifier.getAttemptNumber() == 0)
          && random.nextInt(100) < probabilityPercent;
    }
    return false;
  }

  private boolean matchHost(String host) {
    if (hostToFail.equals("_first_")) {
      objectRegistry.cacheForVertex(KEY_CACHED_HOSTNAME, host);
      hostToFail = host;
    }
    return "*".equals(hostToFail) || host.equalsIgnoreCase(hostToFail);
  }

  private boolean matchSourceVertex(String srcNameTrimmed) {
    return "*".equals(srcNameTrimmedToFail) || srcNameTrimmed.equalsIgnoreCase(srcNameTrimmedToFail);
  }

  @Override
  public String toString() {
    return String.format(
        "[FetcherErrorTestingConfig: host: %s, source vertex: %s, probability: %d%%, failForFirstAttemptOnly: %s]",
        hostToFail, srcNameTrimmedToFail, probabilityPercent, failForFirstAttemptOnly);
  }
}
