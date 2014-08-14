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

import org.apache.hadoop.classification.InterfaceAudience.Public;

/**
 * Abstract representation of the interface {@link LogicalIOProcessor}.
 * Implements the base logic of some methods into this class and is expected 
 * to be the base class that is derived to implement the user {@link Processor}
 *
 */
@Public
public abstract class AbstractLogicalIOProcessor implements LogicalIOProcessor,
    LogicalIOProcessorFrameworkInterface {
  private final ProcessorContext context;

  /**
   * Constructor an instance of the LogicalProcessor. Classes extending this one to create a
   * LogicalProcessor, must provide the same constructor so that Tez can create an instance of the
   * class at runtime.
   *
   * @param context the {@link org.apache.tez.runtime.api.ProcessorContext} which provides
   *                the Processor with context information within the running task.
   */
  public AbstractLogicalIOProcessor(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public abstract void initialize() throws Exception;

  public final ProcessorContext getContext() {
    return context;
  }

}
