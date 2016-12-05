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
 * edge. Edge could be either named or not.
 * 
 */
@Public
public class Edge {

  private final Vertex inputVertex;
  private final Vertex outputVertex;
  private final EdgeProperty edgeProperty;
  private final String name;

  private Edge(Vertex inputVertex,
               Vertex outputVertex,
               EdgeProperty edgeProperty,
               String name) {
    this.inputVertex = inputVertex;
    this.outputVertex = outputVertex;
    this.edgeProperty = edgeProperty;
    this.name = name;
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
    return new Edge(inputVertex, outputVertex, edgeProperty, null);
  }

  /**
   * Creates an edge with specified name between the specified vertices.
   *
   * InputVertex(EdgeInput) ----- Edge ----- OutputVertex(EdgeOutput)]
   *
   * @param inputVertex the vertex which generates data to the edge.
   * @param outputVertex the vertex which consumes data from the edge
   * @param edgeProperty {@link org.apache.tez.dag.api.EdgeProperty} associated with this edge
   * @param name name of edge
   * @return the {@link org.apache.tez.dag.api.Edge}
   */
  public static Edge create(Vertex inputVertex,
                            Vertex outputVertex,
                            EdgeProperty edgeProperty,
                            String name) {
    return new Edge(inputVertex, outputVertex, edgeProperty, name);
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

  /**
   * The name of this edge (or null if edge has no name)
   * @return edge name or null
   */
  public String getName() {
    return name;
  }
  
  /*
   * Used to identify the edge in the configuration
   */
  @Private
  public String getId() {
    // ensure it is unique.
    return String.valueOf(System.identityHashCode(this));
  }
 
  @Override
  public String toString() {
    return "{" + name + " : " + inputVertex + " -> " + outputVertex + " " + edgeProperty + "}";
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) return false;

    Edge edge = (Edge) other;

    if (inputVertex != null ? !inputVertex.equals(edge.inputVertex) : edge.inputVertex != null)
      return false;
    if (outputVertex != null ? !outputVertex.equals(edge.outputVertex) : edge.outputVertex != null)
      return false;
    return name != null ? name.equals(edge.name) : edge.name == null;

  }

  @Override
  public int hashCode() {
    int result = inputVertex != null ? inputVertex.hashCode() : 0;
    result = 31 * result + (outputVertex != null ? outputVertex.hashCode() : 0);
    result = 31 * result + (name != null ? name.hashCode() : 0);
    return result;
  }
}
