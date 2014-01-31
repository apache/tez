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
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringInterner;
import org.apache.tez.dag.api.InputDescriptor;

import com.google.common.collect.Lists;

public class GroupInputSpec implements Writable{

  private String groupName;
  private List<String> groupVertices;
  private InputDescriptor mergedInputDescriptor;
  
  public GroupInputSpec() {
    // for Writable
  }
  
  public String getGroupName() {
    return groupName;
  }
  
  public List<String> getGroupVertices() {
    return groupVertices;
  }
  
  public InputDescriptor getMergedInputDescriptor() {
    return mergedInputDescriptor;
  }
  
  public GroupInputSpec(String groupName, List<String> groupVertices, InputDescriptor inputDescriptor) {
    this.groupName = StringInterner.weakIntern(groupName);
    this.groupVertices = groupVertices;
    this.mergedInputDescriptor = inputDescriptor;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, groupName);
    out.writeInt(groupVertices.size());
    for (String s : groupVertices) {
      Text.writeString(out, s);
    }
    mergedInputDescriptor.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    groupName = StringInterner.weakIntern(Text.readString(in));
    int numMembers = in.readInt();
    groupVertices = Lists.newArrayListWithCapacity(numMembers);
    for (int i=0; i<numMembers; ++i) {
      groupVertices.add(StringInterner.weakIntern(Text.readString(in)));
    }
    mergedInputDescriptor = new InputDescriptor();
    mergedInputDescriptor.readFields(in);
  }
  
  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    sb.append("Group: " + groupName + " { ");
    for (String s: groupVertices) {
      sb.append(s + " ");
    }
    sb.append("} MergedInputDescriptor: " + mergedInputDescriptor.getClassName());
    return sb.toString();
  }

}
