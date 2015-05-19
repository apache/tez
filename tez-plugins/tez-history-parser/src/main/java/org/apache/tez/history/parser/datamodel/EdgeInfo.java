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

package org.apache.tez.history.parser.datamodel;

import static org.apache.hadoop.classification.InterfaceAudience.Public;
import static org.apache.hadoop.classification.InterfaceStability.Evolving;

@Public
@Evolving
public class EdgeInfo {

  private final String inputVertexName;
  private final String outputVertexName;
  private final String dataMovementType;
  private final String edgeSourceClass;
  private final String edgeDestinationClass;
  private final String inputUserPayloadAsText;
  private final String outputUserPayloadAsText;

  private VertexInfo sourceVertex;
  private VertexInfo destinationVertex;

  public EdgeInfo(String inputVertexName, String outputVertexName, String dataMovementType,
      String edgeSourceClass, String edgeDestinationClass, String inputUserPayloadAsText, String
      outputUserPayloadAsText) {
    this.inputVertexName = inputVertexName;
    this.outputVertexName = outputVertexName;
    this.dataMovementType = dataMovementType;
    this.edgeSourceClass = edgeSourceClass;
    this.edgeDestinationClass = edgeDestinationClass;
    this.inputUserPayloadAsText = inputUserPayloadAsText;
    this.outputUserPayloadAsText = outputUserPayloadAsText;
  }

  public final String getInputVertexName() {
    return inputVertexName;
  }

  public final String getOutputVertexName() {
    return outputVertexName;
  }

  public final String getDataMovementType() {
    return dataMovementType;
  }

  public final String getEdgeSourceClass() {
    return edgeSourceClass;
  }

  public final String getEdgeDestinationClass() {
    return edgeDestinationClass;
  }

  public final String getInputUserPayloadAsText() {
    return inputUserPayloadAsText;
  }

  public final String getOutputUserPayloadAsText() {
    return outputUserPayloadAsText;
  }

  public final VertexInfo getSourceVertex() {
    return sourceVertex;
  }

  public final void setSourceVertex(VertexInfo sourceVertex) {
    this.sourceVertex = sourceVertex;
  }

  public final VertexInfo getDestinationVertex() {
    return destinationVertex;
  }

  public final void setDestinationVertex(VertexInfo destinationVertex) {
    this.destinationVertex = destinationVertex;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[");
    sb.append("inputVertexName=").append(inputVertexName).append(", ");
    sb.append("outputVertexName=").append(outputVertexName).append(", ");
    sb.append("dataMovementType=").append(dataMovementType).append(", ");
    sb.append("edgeSourceClass=").append(edgeSourceClass).append(", ");
    sb.append("edgeDestinationClass=").append(edgeDestinationClass).append(", ");
    sb.append("inputUserPayloadAsText=").append(inputUserPayloadAsText).append(",");
    sb.append("outputUserPayloadAsText=").append(outputUserPayloadAsText).append(", ");
    sb.append("sourceVertex=").append(sourceVertex.getVertexName()).append(", ");
    sb.append("destinationVertex=").append(destinationVertex.getVertexName()).append(", ");
    sb.append("outputUserPayloadAsText=").append(outputUserPayloadAsText);
    sb.append("]");
    return sb.toString();
  }
}
