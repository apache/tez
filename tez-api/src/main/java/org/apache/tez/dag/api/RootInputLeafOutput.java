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
class  RootInputLeafOutput <T extends TezEntityDescriptor> {

  private final String name;
  private final T descriptor;
  private final Class<?> initializerClazz;

  RootInputLeafOutput(String name, T descriptor, Class<?> initializerClazz) {
    this.name = name;
    this.descriptor = descriptor;
    this.initializerClazz = initializerClazz;
  }
  
  public String getName() {
    return this.name;
  }

  public T getDescriptor() {
    return this.descriptor;
  }
  
  public Class<?> getInitializerClass() {
    return this.initializerClazz;
  }

}
