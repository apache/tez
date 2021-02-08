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

import org.apache.hadoop.io.Writable;
import org.apache.tez.dag.api.OutputDescriptor;
import org.apache.tez.util.StringInterner;

public class OutputSpec implements Writable {

  private String destinationVertexName;
  private OutputDescriptor outputDescriptor;
  private int physicalEdgeCount;

  public OutputSpec() {
  }

  public OutputSpec(String destinationVertexName,
      OutputDescriptor outputDescriptor, int physicalEdgeCount) {
    this.destinationVertexName = StringInterner.intern(destinationVertexName);
    this.outputDescriptor = outputDescriptor;
    this.physicalEdgeCount = physicalEdgeCount;
  }

  public String getDestinationVertexName() {
    return destinationVertexName;
  }

  public OutputDescriptor getOutputDescriptor() {
    return outputDescriptor;
  }

  public int getPhysicalEdgeCount() {
    return physicalEdgeCount;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    // TODO TEZ-305 convert this to PB
    out.writeUTF(destinationVertexName);
    out.writeInt(physicalEdgeCount);
    outputDescriptor.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    destinationVertexName = StringInterner.intern(in.readUTF());
    physicalEdgeCount = in.readInt();
    outputDescriptor = new OutputDescriptor();
    outputDescriptor.readFields(in);
  }

  public String toString() {
    return "{ destinationVertexName=" + destinationVertexName
        + ", physicalEdgeCount=" + physicalEdgeCount
        + ", outputClassName=" + outputDescriptor.getClassName()
        + " }";
  }
}
