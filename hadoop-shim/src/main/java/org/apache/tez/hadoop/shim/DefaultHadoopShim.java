/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tez.hadoop.shim;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

/** Default Hadoop Shim. Provides Hadoop 3.x baseline capabilities. */
@Private
public class DefaultHadoopShim extends HadoopShim {

  private static final CallerContext NULL_CALLER_CONTEXT = new CallerContext.Builder("").build();

  @Override
  public void setHadoopCallerContext(String context) {
    CallerContext.setCurrent(new CallerContext.Builder(context).build());
  }

  @Override
  public void clearHadoopCallerContext() {
    CallerContext.setCurrent(NULL_CALLER_CONTEXT);
  }

  @Override
  public Set<String> getSupportedResourceTypes(RegisterApplicationMasterResponse response) {
    EnumSet<SchedulerResourceTypes> supportedResourceTypes = response.getSchedulerResourceTypes();
    Set<String> supportedTypes = new HashSet<>();
    for (SchedulerResourceTypes resourceType : supportedResourceTypes) {
      supportedTypes.add(resourceType.name());
    }
    return supportedTypes;
  }
}
