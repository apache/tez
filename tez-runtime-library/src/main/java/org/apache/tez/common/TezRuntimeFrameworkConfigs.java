
/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.tez.common;

import org.apache.hadoop.classification.InterfaceAudience.Private;

/**
 * Configuration parameters which are internal to the Inputs and Outputs which exist in the Runtime
 */
@Private
public class TezRuntimeFrameworkConfigs {

  private static final String TEZ_RUNTIME_FRAMEWORK_PREFIX = "tez.runtime.framework.";

  /**
   * List of directories available to the Runtime.
   */
  public static final String LOCAL_DIRS = TEZ_RUNTIME_FRAMEWORK_PREFIX + "local.dirs";

  public static final String TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS = TEZ_RUNTIME_FRAMEWORK_PREFIX + "num.expected.partitions";

  public static final String TEZ_RUNTIME_METRICS_SESSION_ID = TEZ_RUNTIME_FRAMEWORK_PREFIX +
      "metrics.session.id";
  public static final String TEZ_RUNTIME_METRICS_SESSION_ID_DEFAULT = "";
}
