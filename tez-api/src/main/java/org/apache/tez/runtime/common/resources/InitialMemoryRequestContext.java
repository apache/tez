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

package org.apache.tez.runtime.common.resources;

import com.google.common.base.Preconditions;
import org.apache.hadoop.classification.InterfaceAudience.Private;


@Private
public class InitialMemoryRequestContext {

  public static enum ComponentType {
    INPUT, OUTPUT, PROCESSOR
  }

  private long requestedSize;
  // TODO Replace this with the entire descriptor at some point. ComponentType
  // automatically goes away.
  private String componentClassName;
  private ComponentType componentType;
  private String componentVertexName;

  public InitialMemoryRequestContext(long requestedSize, String componentClassName,
      ComponentType componentType, String componentVertexName) {
    Preconditions.checkNotNull(componentClassName, "componentClassName is null");
    Preconditions.checkNotNull(componentType, "componentType is null");
    Preconditions.checkNotNull(componentVertexName, "componentVertexName is null");
    this.requestedSize = requestedSize;
    this.componentClassName = componentClassName;
    this.componentType = componentType;
    this.componentVertexName = componentVertexName;
  }

  public long getRequestedSize() {
    return requestedSize;
  }

  public String getComponentClassName() {
    return componentClassName;
  }

  public ComponentType getComponentType() {
    return componentType;
  }

  public String getComponentVertexName() {
    return componentVertexName;
  }

}
