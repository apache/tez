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

import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ExecutionType;
import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.Token;
import org.apache.hadoop.yarn.util.RackResolver;
import org.apache.tez.util.StringInterner;

/**
 * Convenience wrapper around  {@link org.apache.hadoop.yarn.api.records.Container}
 */
public class TezContainer extends Container {

  public final static TezContainer NULL_TEZ_CONTAINER = new TezContainer(null);
  private final Container container;

  public TezContainer(Container container) {
    this.container = container;
  }

  @Override
  public ContainerId getId() {
    return container != null ? container.getId() : null;
  }

  @Override
  public void setId(ContainerId id) {
    container.setId(id);
  }

  @Override
  public NodeId getNodeId() {
    return container != null ? container.getNodeId() : null;
  }

  @Override
  public void setNodeId(NodeId nodeId) {
    container.setNodeId(nodeId);
  }

  @Override
  public String getNodeHttpAddress() {
    return container != null ? StringInterner.intern(container.getNodeHttpAddress()) : null;
  }

  @Override
  public void setNodeHttpAddress(String nodeHttpAddress) {
    container.setNodeHttpAddress(nodeHttpAddress);
  }

  @Override
  public Map<String, List<Map<String, String>>> getExposedPorts() {
    return container.getExposedPorts();
  }

  @Override
  public void setExposedPorts(Map<String, List<Map<String, String>>> ports) {
    container.setExposedPorts(ports);
  }

  @Override
  public Resource getResource() {
    return container.getResource();
  }

  @Override
  public void setResource(Resource resource) {
    container.setResource(resource);
  }

  @Override
  public Priority getPriority() {
    return container.getPriority();
  }

  @Override
  public void setPriority(Priority priority) {
    container.setPriority(priority);
  }

  @Override
  public Token getContainerToken() {
    return container.getContainerToken();
  }

  @Override
  public void setContainerToken(Token containerToken) {
    container.setContainerToken(containerToken);
  }

  @Override
  public ExecutionType getExecutionType() {
    return container.getExecutionType();
  }

  @Override
  public void setExecutionType(ExecutionType executionType) {
    container.setExecutionType(executionType);
  }

  @Override
  public int compareTo(Container other) {
    if (this.getId().compareTo(other.getId()) == 0) {
      if (this.getNodeId().compareTo(other.getNodeId()) == 0) {
        return this.getResource().compareTo(other.getResource());
      } else {
        return this.getNodeId().compareTo(other.getNodeId());
      }
    } else {
      return this.getId().compareTo(other.getId());
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other.getClass().isAssignableFrom(this.getClass())) {
      Container otherContainer = ((TezContainer) other).container;
      if (this.container == null && otherContainer == null) {
        return true;
      } else if (this.container == null) {
        return false;
      }
      return this.container.equals((otherContainer));
    }
    return false;
  }

  @Override
  public int hashCode() {
    return container.hashCode();
  }

  public String getRackName() {
    return StringInterner.intern(RackResolver.resolve(container.getNodeId().getHost()).getNetworkLocation());
  }
}
