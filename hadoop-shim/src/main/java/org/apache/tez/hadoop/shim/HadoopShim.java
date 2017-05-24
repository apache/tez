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

import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Unstable;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

@Public
@Unstable
public abstract class HadoopShim {

  /**
   * Set up Hadoop Caller Context
   * @param context Context to be set
   */
  public void setHadoopCallerContext(String context) {
    // Nothing to do
  }

  /**
   * Clear the Hadoop Caller Context
   */
  public void clearHadoopCallerContext() {
    // Nothing to do
  }

  public static String CPU_RESOURCE = "CPU";
  public static String MEMORY_RESOURCE = "MEMORY";

  /**
   * Extract supported Resource types from the RM's response when the AM registers
   * @param response ApplicationMasterResponse from RM after registering
   * @return Set of Resource types that are supported
   */
  public Set<String> getSupportedResourceTypes(RegisterApplicationMasterResponse response) {
    return null;
  }

  public FinalApplicationStatus applyFinalApplicationStatusCorrection(FinalApplicationStatus orig,
      boolean isSessionMode, boolean isError) {
    return orig;
  }
}
