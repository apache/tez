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

package org.apache.tez.dag.api;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.tez.common.annotation.ConfigurationClass;
import org.apache.tez.common.annotation.ConfigurationProperty;

/**
 * Contains fields which will be set automatically by Tez in the Configuration
 */
@ConfigurationClass(templateFileName = "tez-conf-constants.xml")
@Private
public class TezConfigurationConstants {

  static {
    TezConfiguration.setupConfigurationScope(TezConfigurationConstants.class);
  }

  /**
   * String value. Set automatically by the client. The host name of the client the Tez application
   * was submitted from.
   */
  @Private
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_SUBMIT_HOST = TezConfiguration.TEZ_PREFIX + "submit.host";

  /**
   * String value. Set automatically by the client. The host address of the client the Tez
   * application was submitted from.
   */
  @Private
  @ConfigurationScope(Scope.AM)
  @ConfigurationProperty
  public static final String TEZ_SUBMIT_HOST_ADDRESS =
      TezConfiguration.TEZ_PREFIX + "submit.host.address";

}
