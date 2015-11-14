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

package org.apache.tez.hadoop.shim;

import java.util.ServiceLoader;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.util.VersionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Private
public class HadoopShimsLoader {

  private static final Logger LOG = LoggerFactory.getLogger(HadoopShimsLoader.class);

  private static ServiceLoader<HadoopShimProvider> shimLoader =
      ServiceLoader.load(HadoopShimProvider.class);

  private final HadoopShim currentShim;
  private final HadoopShimProvider currentShimProvider;

  public HadoopShimsLoader() {
    String versionStr = VersionInfo.getVersion();
    Version version = new Version(versionStr);
    HadoopShim selectedShim = null;
    HadoopShimProvider selectedShimProvider = null;
    LOG.info("Trying to locate HadoopShimProvider for "
        + "hadoopVersion=" + versionStr
        + ", majorVersion=" + version.majorVersion
        + ", minorVersion=" + version.minorVersion);
    synchronized (shimLoader) {
      for (HadoopShimProvider provider : shimLoader) {
        LOG.debug("Trying HadoopShimProvider : "
            + provider.getClass().getName());
        HadoopShim hadoopShim = null;
        try {
          hadoopShim = provider.createHadoopShim(versionStr,
              version.majorVersion, version.minorVersion);

          if (hadoopShim != null) {
            selectedShim = hadoopShim;
            selectedShimProvider = provider;
            break;
          } else {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Cannot pick " + provider.getClass().getName()
                  + " as the HadoopShimProvider - returned null hadoop shim");
            }
          }
        } catch (Exception e) {
          LOG.info("Failed to use " + provider.getClass().getName()
              + " due to error: ", e);
        }
      }
      if (selectedShim == null) {
        currentShim = new DefaultHadoopShim();
        currentShimProvider = null;
      } else {
        currentShim = selectedShim;
        currentShimProvider = selectedShimProvider;
      }
    }
    LOG.info("Picked HadoopShim " + currentShim.getClass().getName()
        + ", providerName="
        + (currentShimProvider != null ? currentShimProvider.getClass().getName() : "null" )
        + ", hadoopVersion=" + versionStr
        + ", majorVersion=" + version.majorVersion
        + ", minorVersion=" + version.minorVersion);

  }

  public HadoopShim getHadoopShim() {
    return currentShim;
  }

  private static class Version {

    int majorVersion = -1;
    int minorVersion = -1;

    public Version(String versionString) {
      int index = 0;
      StringTokenizer tokenizer = new StringTokenizer(versionString, ".-", true);
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        if (token.equals("-")) {
          break;
        }
        if (token.equals(".")) {
          continue;
        }
        try {
          int i = Integer.parseInt(token);
          if (index == 0) {
            majorVersion = i;
          } else if (index == 1) {
            minorVersion = i;
            break;
          }
          ++index;
        } catch (NumberFormatException nfe) {
          break;
        }
      }
    }
  }

}
