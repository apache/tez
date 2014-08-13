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

package org.apache.tez.runtime.api.impl;

import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.TaskCounter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;

public class TezCountersDelegate extends TezCounters {

  private final String groupModifier;
  private final TezCounters original;
  
  public TezCountersDelegate(TezCounters original, String taskVertexName, String edgeVertexName,
      String type) {
    this.original = original;
    this.groupModifier = TezUtilsInternal.cleanVertexName(taskVertexName) + "_" + type + "_"
        + TezUtilsInternal.cleanVertexName(edgeVertexName);
  }

  // Should only be called while setting up Inputs / Outputs - rather than being
  // the standard mechanism to find a counter.
  @Override
  public TezCounter findCounter(String groupName, String counterName) {
    if (groupName.equals(TaskCounter.class.getName())) {
      groupName = TaskCounter.class.getSimpleName();
    }
    String modifiedGroupName = groupName + "_" + this.groupModifier;
    return original.findCounter(modifiedGroupName, counterName);
  }
}
