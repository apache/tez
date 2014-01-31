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

import org.apache.hadoop.classification.InterfaceAudience.Private;

public class GroupInputEdge {

  private final VertexGroup inputVertexGroup;
  private final Vertex outputVertex;
  private final EdgeProperty edgeProperty;
  private final InputDescriptor mergedInput;

  // InputVertex(EdgeInput) ----- Edge ----- OutputVertex(EdgeOutput)]
  /**
   * An Edge that connects a VertexGroup to a destination Vertex. The framework
   * takes care of connecting the VertexGroup members with the destination
   * vertex. The tasks of the destination vertex see only 1 input named after
   * the VertexGroup instead of individual inputs from group members. These
   * individual inputs are merged using the mergedInput before presenting them
   * to the destination task.
   * 
   * @param inputVertexGroup source VertexGroup
   * @param outputVertex destination Vertex
   * @param edgeProperty edge properties
   * @param mergedInput MergedLogicalInput 
   */
  public GroupInputEdge(VertexGroup inputVertexGroup, 
      Vertex outputVertex, 
      EdgeProperty edgeProperty,
      InputDescriptor mergedInput) {
    this.inputVertexGroup = inputVertexGroup;
    this.outputVertex = outputVertex;
    this.edgeProperty = edgeProperty;
    if (mergedInput == null) {
      throw new TezUncheckedException(
          "Merged input must be specified when using GroupInputEdge");
    }
    this.mergedInput = mergedInput;
  }

  public VertexGroup getInputVertexGroup() {
    return inputVertexGroup;
  }

  public Vertex getOutputVertex() {
    return outputVertex;
  }

  public EdgeProperty getEdgeProperty() {
    return edgeProperty;
  }
  
  InputDescriptor getMergedInput() {
    return mergedInput;
  }

  /*
   * Used to identify the edge in the configuration
   */
  @Private
  public String getId() {
    return String.valueOf(this.hashCode());
  }
 
  @Override
  public String toString() {
    return inputVertexGroup + " -> " + outputVertex + " (" + edgeProperty + ")";
  }
  
}
