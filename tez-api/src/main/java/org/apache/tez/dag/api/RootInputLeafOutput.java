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

@Private
public class RootInputLeafOutput <T extends EntityDescriptor<T>, S extends EntityDescriptor<S>> {

  private final String name;
  private final T ioDescriptor;
  private final S controllerDescriptor;

  public RootInputLeafOutput(String name, T ioDescriptor, S controllerDescriptor) {
    this.name = name;
    this.ioDescriptor = ioDescriptor;
    this.controllerDescriptor = controllerDescriptor;
  }
  
  public String getName() {
    return this.name;
  }

  public T getIODescriptor() {
    return this.ioDescriptor;
  }
  
  public S getControllerDescriptor() {
    return this.controllerDescriptor;
  }

  @Override
  public String toString() {
    return "{InputName=" + name + "}, {Descriptor=" + ioDescriptor + "}, {ControllerDescriptor=" +
        controllerDescriptor + "}";
  }

}
