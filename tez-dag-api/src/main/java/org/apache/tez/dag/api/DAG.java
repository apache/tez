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

import java.util.ArrayList;
import java.util.List;

public class DAG { // FIXME rename to Topology
  List<Vertex> vertices;
  List<Edge> edges;
  
  public DAG() {
    this.vertices = new ArrayList<Vertex>();
    this.edges = new ArrayList<Edge>();
  }

  public synchronized void addVertex(Vertex vertex) {
    if (vertices.contains(vertex)) {
      throw new IllegalArgumentException(
          "Vertex " + vertex + " already defined!");
    }
    vertices.add(vertex);
  }
  
  public synchronized void addEdge(Edge edge) {
    // Sanity checks
    if (!vertices.contains(edge.getInputVertex())) {
      throw new IllegalArgumentException(
          "Input vertex " + edge.getInputVertex() + " doesn't exist!");
    }
    if (!vertices.contains(edge.getOutputVertex())) {
      throw new IllegalArgumentException(
          "Output vertex " + edge.getOutputVertex() + " doesn't exist!");    
    }
    if (edges.contains(edge)) {
      throw new IllegalArgumentException(
          "Edge " + edge + " already defined!");
    }
    
    // Inform the vertices
    edge.getInputVertex().addOutputVertex(edge.getOutputVertex(), edge.getId());
    edge.getOutputVertex().addInputVertex(edge.getInputVertex(), edge.getId());
    
    edges.add(edge);
  }
  
  public void verify() throws TezException { // FIXME better exception

    //FIXME are task resources compulsory or will the DAG AM put in a default
    //for each vertex if not specified?

  }
    
  // FIXME DAGConfiguration is not public API
  public DAGConfiguration serializeDag() {
    DAGConfiguration dagConf = new DAGConfiguration();
    
    dagConf.setVertices(vertices);
    dagConf.setEdgeProperties(edges);
    
    for(Vertex vertex : vertices) {
      if(vertex.getInputVertices() != null) {
        dagConf.setInputVertices(vertex.getVertexName(), vertex.getInputVertices());
        dagConf.setInputEdgeIds(vertex.getVertexName(), vertex.getInputEdgeIds());
      }
      if(vertex.getOutputVertices() != null) {
        dagConf.setOutputVertices(vertex.getVertexName(), vertex.getOutputVertices());
        dagConf.setOutputEdgeIds(vertex.getVertexName(), vertex.getOutputEdgeIds());
      }
    }
    
    return dagConf;
  }
  
}
