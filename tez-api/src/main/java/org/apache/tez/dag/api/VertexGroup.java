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

package org.apache.tez.dag.api;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.classification.InterfaceAudience.Public;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Represents a virtual collection of vertices whose members can be treated as a single 
 * named collection for graph operations. Only the following connections are valid.
 * A VertexGroup can be connected as an input to a consumer Vertex. The tasks of 
 * the destination vertex see a single input named after the VertexGroup instead 
 * multiple inputs from the members of the VertexGroup. 
 * An output can be added to a VertexGroup.
 * All outgoing edges & outputs of a VertexGroup are automatically transferred to the 
 * member vertices of the VertexGroup.
 * A VertexGroup is not part of the final DAG.
 */
@Public
public class VertexGroup {

  static class GroupInfo {
    String groupName;
    Set<Vertex> members = new HashSet<Vertex>();
    Set<String> outputs = new HashSet<String>();
    // destination vertex name to merged input map
    Map<String, InputDescriptor> edgeMergedInputs = Maps.newHashMap();
    
    GroupInfo(String groupName, Vertex... vertices) {
      this.groupName = groupName;
      members = Sets.newHashSetWithExpectedSize(vertices.length);
      for (Vertex v : vertices) {
        members.add(v);
      }
    }
    String getGroupName() {
      return groupName;
    }
    Set<Vertex> getMembers() {
      return members;
    }
    Set<String> getOutputs() {
      return outputs;
    }
  }
  
  GroupInfo groupInfo;
  
  /**
   * Create an object representing a group of vertices
   * @param groupName name of the group
   */
  VertexGroup(String groupName, Vertex... members) {
    if (members == null || members.length < 2) {
      throw new IllegalArgumentException("VertexGroup must have at least 2 members");
    }
    this.groupInfo = new GroupInfo(groupName, members);
  }

  /**
   * Get the name of the group
   * @return name
   */
  public String getGroupName() {
    return groupInfo.groupName;
  }
  
  /**
   * Add an common data sink to the group of vertices.
   * Refer to {@link Vertex#addDataSink(String, DataSinkDescriptor)}
   * @return this object for further chained method calls
   */
  public VertexGroup addDataSink(String outputName, DataSinkDescriptor dataSinkDescriptor) {
    RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor> leafOutput = 
        new RootInputLeafOutput<OutputDescriptor, OutputCommitterDescriptor>(outputName,
        dataSinkDescriptor.getOutputDescriptor(), dataSinkDescriptor.getOutputCommitterDescriptor());
    this.groupInfo.outputs.add(outputName);
    
    // also add output to its members
    for (Vertex member : getMembers()) {
      member.addAdditionalDataSink(leafOutput);
    }
    
    return this;
  }
  
  @Override
  public String toString() {
    return "[ VertexGroup: " + groupInfo.getGroupName() + "]";
  }

  GroupInfo getGroupInfo() {
    return groupInfo;
  }
  
  Set<Vertex> getMembers() {
    return groupInfo.members;
  }
  
  void addOutputVertex(Vertex outputVertex, GroupInputEdge edge) {
    this.groupInfo.edgeMergedInputs.put(outputVertex.getName(), edge.getMergedInput());
  }
  
}
