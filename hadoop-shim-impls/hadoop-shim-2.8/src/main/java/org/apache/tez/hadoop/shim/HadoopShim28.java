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

import org.apache.hadoop.ipc.CallerContext;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.proto.YarnServiceProtos.SchedulerResourceTypes;

public class HadoopShim28 extends HadoopShim {

  final static CallerContext nullCallerContext = new CallerContext.Builder("").build();

  @Override
  public void setHadoopCallerContext(String context) {
    CallerContext.setCurrent(new CallerContext.Builder(context).build());
  }

  @Override
  public void clearHadoopCallerContext() {
    CallerContext.setCurrent(nullCallerContext);
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

  @Override
  public FinalApplicationStatus applyFinalApplicationStatusCorrection(FinalApplicationStatus orig,
      boolean isSessionMode, boolean isError) {
    switch (orig) {
      case FAILED:
        // App is failed if dag failed in non-session mode or there was an error.
        return (!isSessionMode || isError) ?
            FinalApplicationStatus.FAILED : FinalApplicationStatus.ENDED;
      case SUCCEEDED:
        return isSessionMode ? FinalApplicationStatus.ENDED : FinalApplicationStatus.SUCCEEDED;
      default:
        return orig;
    }
  }
}
