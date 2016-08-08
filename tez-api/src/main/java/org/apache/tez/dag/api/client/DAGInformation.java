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

package org.apache.tez.dag.api.client;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.tez.dag.api.DAG;
import org.apache.tez.dag.api.records.DAGProtos;

/**
 * Some information about the {@link DAG}
 */
@Public
public class DAGInformation {

  DAGProtos.DAGInformationProtoOrBuilder proxy = null;
  private List<VertexInformation> vertexInformationList = null;
  private AtomicBoolean vertexListInitialized = new AtomicBoolean(false);

  @Private
  public DAGInformation(DAGProtos.DAGInformationProtoOrBuilder proxy) {
    this.proxy = proxy;
  }

  public String getName() {
    return proxy.getName();
  }

  public String getDAGID() {
    return proxy.getDagId();
  }

  public List<VertexInformation> getVertexInformation() {
    if (vertexListInitialized.get()) {
      return vertexInformationList;
    }

    vertexInformationList = new ArrayList<>(proxy.getVerticesCount());
    for(DAGProtos.VertexInformationProto vertexProto : proxy.getVerticesList()) {
      vertexInformationList.add(new VertexInformation(vertexProto));
    }

    vertexListInitialized.set(true);
    return vertexInformationList;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    if (o instanceof DAGInformation) {
      DAGInformation other = (DAGInformation) o;

      List<VertexInformation> vertexList = getVertexInformation();
      List<VertexInformation> otherVertexList = other.getVertexInformation();
      return getName().equals(other.getName())
        && getDAGID().equals(other.getDAGID())
        &&
        ((vertexList == null && otherVertexList == null)
          || vertexList.equals(otherVertexList));
    }
    return false;
  }

  @Override
  public int hashCode() {
    final int prime = 46021;
    int result = prime + getName().hashCode();

    String id = getDAGID();
    List<VertexInformation> vertexInformationList = getVertexInformation();

    result = prime * result +
      ((id == null)? 0 : id.hashCode());
    result = prime * result +
      ((vertexInformationList == null)? 0 : vertexInformationList.hashCode());

    return result;
  }

  @Override
  public String toString() {
    String vertexListStr = StringUtils.join(getVertexInformation(), ",");
    return ("name=" + getName()
      + ", dagId=" + getDAGID()
      + ", VertexInformationList=" + vertexListStr);
  }
}
