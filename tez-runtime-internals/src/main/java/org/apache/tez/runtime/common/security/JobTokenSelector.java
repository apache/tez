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

package org.apache.tez.runtime.common.security;

import java.util.Collection;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.tez.common.security.JobTokenIdentifier;

/**
 * Look through tokens to find the first job token that matches the service
 * and return it.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class JobTokenSelector implements TokenSelector<JobTokenIdentifier> {

  @SuppressWarnings("unchecked")
  @Override
  public Token<JobTokenIdentifier> selectToken(Text service,
                                               Collection<Token<? extends TokenIdentifier>> tokens) {
    if (service == null) {
      return null;
    }
    for (Token<? extends TokenIdentifier> token : tokens) {
      if (JobTokenIdentifier.KIND_NAME.equals(token.getKind())
          && service.equals(token.getService())) {
        return (Token<JobTokenIdentifier>) token;
      }
    }
    return null;
  }
}
