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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

@Private
public class HadoopShim27 extends HadoopShim {

  @Override
  public void setHadoopCallerContext(String context) {
    // Not supported
  }

  @Override
  public void clearHadoopCallerContext() {
    // Not supported
  }

  @Override
  public Set<String> getSupportedResourceTypes(RegisterApplicationMasterResponse response) {
    EnumSet<SchedulerResourceTypes> supportedResourceTypes = response.getSchedulerResourceTypes();
    Set<String> supportedTypes = new HashSet<String>();
    for (SchedulerResourceTypes resourceType : supportedResourceTypes) {
      supportedTypes.add(resourceType.name());
    }
    return supportedTypes;
  }
}
