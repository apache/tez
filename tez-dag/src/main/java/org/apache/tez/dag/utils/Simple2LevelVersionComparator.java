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

package org.apache.tez.dag.utils;

import java.util.StringTokenizer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Basic class to compare 2 version strings.
 * Handles basic versions containing integers separated by '.' followed by a
 * hyphen and a classifier.
 * For example, 0.1.1 or 0.1-SNAPSHOT or 1.0.0-SNAPSHOT
 * When comparing, it only compares the first 2 integer digits. i.e. for versions in
 * the format x.y.z-qualifier, it will only look for and compare x.y. For example,
 * 0.1.1 will be equal to 0.1.2 but 0.1.2 and 0.2.1 are not equal.
 */
public class Simple2LevelVersionComparator {

  private static final Logger LOG = LoggerFactory.getLogger(Simple2LevelVersionComparator.class);

  public static class Version {

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
          int i = Integer.valueOf(token);
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

  public int compare(String versionStr1, String versionStr2) {
    Version v1 = new Version(versionStr1);
    Version v2 = new Version(versionStr2);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Comparing versions"
          + " version1=" + v1.majorVersion + "." + v1.minorVersion
          + ", version2=" + v2.majorVersion + "." + v2.minorVersion);
    }

    if (v1.majorVersion == -1 || v2.majorVersion == -1) {
      return -1;
    }
    if (v1.majorVersion == v2.majorVersion) {
      if ((v1.minorVersion == -1 && v2.minorVersion != -1)
          || (v1.minorVersion != -1 && v2.minorVersion == -1)) {
        return -1;
      }
      if (v1.minorVersion > v2.minorVersion) {
        return 1;
      } else if (v1.minorVersion < v2.minorVersion) {
        return -1;
      } else {
        return 0;
      }
    } else {
      if (v1.majorVersion > v2.majorVersion) {
        return 1;
      } else {
        return -1;
      }
    }
  }
}
