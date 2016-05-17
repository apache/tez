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

package org.apache.tez.common;

import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.tez.dag.api.TezException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Unstable
@Private
public class JavaOptsChecker {

  private static final Logger LOG = LoggerFactory.getLogger(JavaOptsChecker.class);
  private static final Pattern pattern = Pattern.compile("\\s*(-XX:([\\+|\\-]?)(\\S+))\\s*");

  public void checkOpts(String opts) throws TezException {
    Set<String> gcOpts = new TreeSet<String>();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking JVM GC opts: " + opts);
    }
    Matcher matcher = pattern.matcher(opts);
    while (matcher.find()) {
      if (matcher.groupCount() != 3) {
        continue;
      }

      String opt = matcher.group(3);
      if (!opt.matches("Use.+GC")) {
        continue;
      }

      int val = ( matcher.group(2).equals("+") ? 1 : -1 );
      if (gcOpts.contains(opt)) {
        val += 1;
      }

      if (val > 0) {
        gcOpts.add(opt);
      } else {
        gcOpts.remove(opt);
      }
    }

    if (gcOpts.size() > 1) {
      // Handle special case for " -XX:+UseParNewGC -XX:+UseConcMarkSweepGC "
      // which can be specified together.
      if (gcOpts.size() == 2) {
        if (gcOpts.contains("UseParNewGC")
          && gcOpts.contains("UseConcMarkSweepGC")) {
          return;
        }
      }

      if (LOG.isDebugEnabled()) {
        LOG.debug("Found clashing GC opts"
            + ", conflicting GC Values=" + gcOpts);
      }
      throw new TezException("Invalid/conflicting GC options found,"
          + " cmdOpts=\"" + opts + "\"");
    }

  }

}
