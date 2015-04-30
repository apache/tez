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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.tez.common.TezUtilsInternal;
import org.apache.tez.common.counters.AbstractCounter;
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
    String simpleGroupName;
    if (groupName.equals(TaskCounter.class.getName())) {
      simpleGroupName = TaskCounter.class.getSimpleName();
    } else  {
      simpleGroupName = groupName;
    }
    String modifiedGroupName = simpleGroupName + "_" + this.groupModifier;
    final TezCounter modifiedGroupCounter = original.findCounter(modifiedGroupName, counterName);
    final TezCounter originalGroupCounter = original.findCounter(groupName, counterName);
    return new CompositeCounter(modifiedGroupCounter, originalGroupCounter);
  }

  /*
   * A counter class to wrap multiple counters. increment operation will increment both counters
   */
  private static class CompositeCounter extends AbstractCounter {

    TezCounter modifiedCounter;
    TezCounter originalCounter;

    public CompositeCounter(TezCounter modifiedCounter, TezCounter originalCounter) {
      this.modifiedCounter = modifiedCounter;
      this.originalCounter = originalCounter;
    }

    @Override
    public String getName() {
      return modifiedCounter.getName();
    }

    @Override
    public String getDisplayName() {
      return modifiedCounter.getName();
    }

    @Override
    public long getValue() {
      return modifiedCounter.getValue();
    }

    @Override
    public void setValue(long value) {
      modifiedCounter.setValue(value);
      originalCounter.setValue(value);
    }

    @Override
    public void increment(long increment) {
      modifiedCounter.increment(increment);
      originalCounter.increment(increment);
    }

    @Override
    public TezCounter getUnderlyingCounter() {
      return this;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
      assert false : "shouldn't be called";
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
      assert false : "shouldn't be called";
    }
  }
}
