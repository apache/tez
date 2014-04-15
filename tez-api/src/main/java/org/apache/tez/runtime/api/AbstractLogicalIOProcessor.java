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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
/**
 * Abstract representation of the interface {@link LogicalIOProcessor}.
 * Implements the base logic of some methods into this class.
 * It will reduce the code for any processor implementation.
 *
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public abstract class AbstractLogicalIOProcessor implements LogicalIOProcessor {
  protected TezProcessorContext context;

  @Override
  public void initialize(TezProcessorContext processorContext) throws Exception {
    this.context = processorContext;
    initialize();
  }

  public abstract void initialize() throws Exception;

  public TezProcessorContext getContext() {
    return context;
  }

}
