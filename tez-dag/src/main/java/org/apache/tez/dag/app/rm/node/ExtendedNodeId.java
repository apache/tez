/*
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

package org.apache.tez.dag.app.rm.node;

import java.util.Objects;

import org.apache.hadoop.yarn.api.records.NodeId;

/**
 * ExtendedNodeId extends NodeId with unique identifier in addition to hostname and port.
 */
public class ExtendedNodeId extends NodeId {
  private NodeId nodeId;
  private String host;
  private int port;
  private final String uniqueIdentifier;

  public ExtendedNodeId(NodeId nodeId, String uniqueIdentifier) {
    this.nodeId = Objects.requireNonNull(nodeId);
    this.uniqueIdentifier = uniqueIdentifier == null ? "" : uniqueIdentifier.trim();
  }

  @Override
  public String getHost() {
    return nodeId.getHost();
  }

  @Override
  protected void setHost(final String host) {
    this.host = host;
    build();
  }

  @Override
  public int getPort() {
    return nodeId.getPort();
  }

  @Override
  protected void setPort(final int port) {
    this.port = port;
    build();
  }

  @Override
  protected void build() {
    this.nodeId = NodeId.newInstance(host, port);
  }

  @Override
  public String toString() {
    if (!uniqueIdentifier.isEmpty()) {
      return super.toString() + ":" + uniqueIdentifier;
    }
    return super.toString();
  }

  @Override
  public int hashCode() {
    if (!uniqueIdentifier.isEmpty()) {
      return super.hashCode() + 31 * uniqueIdentifier.hashCode();
    }
    return super.hashCode();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    } else if (obj == null) {
      return false;
    } else if (this.getClass() != obj.getClass()) {
      return false;
    } else {
      ExtendedNodeId amNodeId = (ExtendedNodeId) obj;
      return super.equals(obj) && Objects.equals(uniqueIdentifier, amNodeId.uniqueIdentifier);
    }
  }
}
