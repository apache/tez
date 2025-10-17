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
package org.apache.tez.runtime.library;

import org.apache.tez.dag.api.client.VertexStatus;
import org.apache.tez.runtime.api.OutputCommitter;
import org.apache.tez.runtime.api.OutputCommitterContext;

public class FakeOutputCommitter extends OutputCommitter {

  /**
   * Constructs an instance of {@code OutputCommitter}.
   * <p>
   * Classes extending this one to create a custom {@code OutputCommitter} must provide
   * the same constructor signature so that Tez can instantiate the class at runtime.
   * </p>
   *
   * @param committerContext  the context that provides access to the payload,
   *                          vertex properties, and other commit-related data
   */
  public FakeOutputCommitter(OutputCommitterContext committerContext) {
    super(committerContext);
  }

  @Override
  public void initialize() throws Exception {

  }

  @Override
  public void setupOutput() throws Exception {

  }

  @Override
  public void commitOutput() throws Exception {

  }

  @Override
  public void abortOutput(VertexStatus.State finalState) throws Exception {

  }
}
