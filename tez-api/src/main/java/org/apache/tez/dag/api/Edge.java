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

/**
 * Edge defines the connection between a producer and consumer vertex in the DAG.
 * @link {@link EdgeProperty} defines the relationship between them. The producer
 * vertex provides input to the edge and the consumer vertex reads output from the 
 * edge.
 * 
 */
@Public
public class Edge {

  private final Vertex inputVertex;
  private final Vertex outputVertex;
  private final EdgeProperty edgeProperty;

  private Edge(Vertex inputVertex,
               Vertex outputVertex,
               EdgeProperty edgeProperty) {
    this.inputVertex = inputVertex;
    this.outputVertex = outputVertex;
    this.edgeProperty = edgeProperty;
  }



  /**
   * Creates an edge between the specified vertices.
   *
   * InputVertex(EdgeInput) ----- Edge ----- OutputVertex(EdgeOutput)]
   *
   * @param inputVertex the vertex which generates data to the edge.
   * @param outputVertex the vertex which consumes data from the edge
   * @param edgeProperty {@link org.apache.tez.dag.api.EdgeProperty} associated with this edge
   * @return the {@link org.apache.tez.dag.api.Edge}
   */
  public static Edge create(Vertex inputVertex,
                            Vertex outputVertex,
                            EdgeProperty edgeProperty) {
    return new Edge(inputVertex, outputVertex, edgeProperty);
  }

  /**
   * The @link {@link Vertex} that provides input to the edge
   * @return  {@link Vertex}
   */
  public Vertex getInputVertex() {
    return inputVertex;
  }

  /**
   * The @link {@link Vertex} that reads output from the edge
   * @return {@link Vertex} 
   */
  public Vertex getOutputVertex() {
    return outputVertex;
  }

  /**
   * The @link {@link EdgeProperty} for this edge
   * @return {@link EdgeProperty}
   */
  public EdgeProperty getEdgeProperty() {
    return edgeProperty;
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
    return inputVertex + " -> " + outputVertex + " (" + edgeProperty + ")";
  }
  
}
