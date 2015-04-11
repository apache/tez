/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.tez.runtime.api.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringInterner;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

public class TaskStatistics implements Writable {
  // The memory usage of this is minimal (<10MB for 10K tasks x 10 inputs)
  // TestMockDAGAppMaster#testBasicStatisticsMemory
  private Map<String, IOStatistics> ioStatistics = Maps.newConcurrentMap();

  public void addIO(String edgeName) {
    addIO(edgeName, new IOStatistics());
  }
  
  public void addIO(String edgeName, IOStatistics stats) {
    Preconditions.checkArgument(stats != null, edgeName);
    ioStatistics.put(StringInterner.weakIntern(edgeName), stats);    
  }
  
  public Map<String, IOStatistics> getIOStatistics() {
    return ioStatistics;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    int numEntries = ioStatistics.size();
    out.writeInt(numEntries);
    for (Map.Entry<String, IOStatistics> entry : ioStatistics.entrySet()) {
      IOStatistics edgeStats = entry.getValue();
      Text.writeString(out, entry.getKey());
      out.writeLong(edgeStats.getDataSize());
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numEntries = in.readInt();
    for (int i=0; i<numEntries; ++i) {
      String edgeName = Text.readString(in);
      IOStatistics edgeStats = new IOStatistics();
      edgeStats.setDataSize(in.readLong());
      addIO(edgeName, edgeStats);
    }
  }
}
