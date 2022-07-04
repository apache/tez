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

import java.lang.annotation.Annotation;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.KerberosInfo;
import org.apache.hadoop.security.SecurityInfo;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.security.token.TokenInfo;
import org.apache.hadoop.security.token.TokenSelector;
import org.apache.hadoop.yarn.security.client.ClientToAMTokenSelector;
import org.apache.tez.dag.api.client.rpc.DAGClientAMProtocolBlockingPB;

@Private
public class DAGClientSecurityInfo extends SecurityInfo {

  @Override
  public KerberosInfo getKerberosInfo(Class<?> protocol, Configuration conf) {
    return null;
  }

  @Override
  public TokenInfo getTokenInfo(Class<?> protocol, Configuration conf) {
    if (!protocol.equals(DAGClientAMProtocolBlockingPB.class)) {
      return null;
    }
    return new TokenInfo() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public Class<? extends TokenSelector<? extends TokenIdentifier>>
      value() {
        return ClientToAMTokenSelector.class;
      }
    };
  }
}
