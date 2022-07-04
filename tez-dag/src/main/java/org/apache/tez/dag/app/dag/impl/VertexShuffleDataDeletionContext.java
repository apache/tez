/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.dag.app.dag.impl;

import org.apache.tez.dag.app.dag.Vertex;

import java.util.HashSet;
import java.util.Set;

public class VertexShuffleDataDeletionContext {
  private int deletionHeight;
  private int incompleteChildrenVertices;
  private Set<Vertex> ancestors;
  private Set<Vertex> children;

  public VertexShuffleDataDeletionContext(int deletionHeight) {
    this.deletionHeight = deletionHeight;
    this.incompleteChildrenVertices = 0;
    this.ancestors = new HashSet<>();
    this.children = new HashSet<>();
  }

  public void setSpannedVertices(Vertex vertex) {
    getSpannedVerticesAncestors(vertex, ancestors, deletionHeight);
    getSpannedVerticesChildren(vertex, children, deletionHeight);
    setIncompleteChildrenVertices(children.size());
  }

  /**
   * get all the ancestor vertices at a particular depth.
   */
  private static void getSpannedVerticesAncestors(Vertex vertex, Set<Vertex> ancestorVertices, int level) {
    if (level == 0) {
      ancestorVertices.add(vertex);
      return;
    }

    if (level == 1) {
      ancestorVertices.addAll(vertex.getInputVertices().keySet());
      return;
    }

    vertex.getInputVertices().forEach((inVertex, edge) -> getSpannedVerticesAncestors(inVertex, ancestorVertices,
        level - 1));
  }

  /**
   * get all the child vertices at a particular depth.
   */
  private static void getSpannedVerticesChildren(Vertex vertex, Set<Vertex> childVertices, int level) {
    if (level == 0) {
      childVertices.add(vertex);
      return;
    }

    if (level == 1) {
      childVertices.addAll(vertex.getOutputVertices().keySet());
      return;
    }

    vertex.getOutputVertices().forEach((outVertex, edge) -> getSpannedVerticesChildren(outVertex, childVertices,
        level - 1));
  }

  public void setIncompleteChildrenVertices(int incompleteChildrenVertices) {
    this.incompleteChildrenVertices = incompleteChildrenVertices;
  }

  public int getIncompleteChildrenVertices() {
    return this.incompleteChildrenVertices;
  }

  public Set<Vertex> getAncestors() {
    return this.ancestors;
  }

  public Set<Vertex> getChildren() {
    return this.children;
  }
}
