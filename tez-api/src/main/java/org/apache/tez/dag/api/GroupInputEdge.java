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
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.runtime.api.MergedLogicalInput;

/**
 * A composite edge that represents a common connection between a 
 * group of input vertices and a single output vertex. This can be 
 * used to perform e.g. an efficient union of the data produced by 
 * the input vertices. The output vertex tasks see a unified/merged
 * view of the data from all the input vertices.
 */
@Public
public class GroupInputEdge {

  private final VertexGroup inputVertexGroup;
  private final Vertex outputVertex;
  private final EdgeProperty edgeProperty;
  private final InputDescriptor mergedInput;

  // InputVertex(EdgeInput) ----- Edge ----- OutputVertex(EdgeOutput)]
  private GroupInputEdge(VertexGroup inputVertexGroup,
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

  /**
   * An Edge that connects a {@link VertexGroup} to a destination Vertex. The
   * framework takes care of connecting the {@link VertexGroup} members with the
   * destination vertex. The tasks of the destination vertex see only 1 input
   * named after the VertexGroup instead of individual inputs from group
   * members. These individual inputs are merged using the mergedInput before
   * presenting them to the destination task.
   *
   * @param inputVertexGroup
   *          source {@link VertexGroup}
   * @param outputVertex
   *          destination Vertex
   * @param edgeProperty
   *          the common {@link EdgeProperty} for this {@link GroupInputEdge}
   * @param mergedInput
   *          {@link MergedLogicalInput} This input is responsible for merging
   *          the data from the input vertex tasks to create a single input for
   *          the output vertex tasks
   */
  public static GroupInputEdge create(VertexGroup inputVertexGroup,
                                      Vertex outputVertex,
                                      EdgeProperty edgeProperty,
                                      InputDescriptor mergedInput) {
    return new GroupInputEdge(inputVertexGroup, outputVertex, edgeProperty, mergedInput);
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

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result
        + ((inputVertexGroup == null) ? 0 : inputVertexGroup.hashCode());
    result = prime * result
        + ((outputVertex == null) ? 0 : outputVertex.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    GroupInputEdge other = (GroupInputEdge) obj;
    if (inputVertexGroup == null) {
      if (other.inputVertexGroup != null)
        return false;
    } else if (!inputVertexGroup.equals(other.inputVertexGroup))
      return false;
    if (outputVertex == null) {
      if (other.outputVertex != null)
        return false;
    } else if (!outputVertex.equals(other.outputVertex))
      return false;
    return true;
  }
}
