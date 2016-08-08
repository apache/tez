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

import org.apache.tez.dag.api.records.DAGProtos.VertexInformationProto;
import org.apache.tez.dag.api.records.DAGProtos.DAGInformationProto;

public class DAGInformationBuilder extends DAGInformation {

  public DAGInformationBuilder() {
    super(DAGInformationProto.newBuilder());
  }

  // name id vertex list
  public void setName(String name) {
    getBuilder().setName(name);
  }

  public void setDagId(String id) {
    getBuilder().setDagId(id);
  }

  public void setVertexInformationList(List<VertexInformation> vertexInformationList) {
    List<VertexInformationProto> vertexProtos = new ArrayList<>(vertexInformationList.size());
    for(VertexInformation vertexInformation : vertexInformationList) {
      VertexInformationBuilder vertexBuilder = new VertexInformationBuilder();
      vertexBuilder.setId(vertexInformation.getId());
      vertexBuilder.setName(vertexInformation.getName());
      vertexBuilder.setTaskInformationList(vertexInformation.getTaskInformationList());

      vertexProtos.add(vertexBuilder.getProto());
    }
    getBuilder().addAllVertices(vertexProtos);
  }

  public DAGInformationProto getProto() {
    return getBuilder().build();
  }

  private DAGInformationProto.Builder getBuilder() {
    return (DAGInformationProto.Builder) this.proxy;
  }

}
