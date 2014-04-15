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
package org.apache.tez.runtime.api;

import java.util.List;

/**
 * The abstract implementation of {@link LogicalOutput}. It includes default
 * implementations of few methods for the convenience.
 * 
 */
public abstract class AbstractLogicalOutput implements LogicalOutput {

  protected int numPhysicalOutputs;
  protected TezOutputContext outputContext;

  @Override
  public void setNumPhysicalOutputs(int numOutputs) {
    this.numPhysicalOutputs = numOutputs;
  }

  @Override
  public List<Event> initialize(TezOutputContext _outputContext) throws Exception {
    this.outputContext = _outputContext;
    return initialize();
  }
  
  public abstract List<Event> initialize() throws Exception;

  public int getNumPhysicalOutputs() {
    return numPhysicalOutputs;
  }

  public TezOutputContext getContext() {
    return outputContext;
  }

}
