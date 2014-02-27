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

package org.apache.tez.common.impl;

import org.apache.commons.logging.Log;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.token.Token;

@Private
public class LogUtils {

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
}
