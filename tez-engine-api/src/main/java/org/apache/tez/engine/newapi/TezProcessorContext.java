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

package org.apache.tez.engine.newapi;

import java.io.IOException;

/**
 * Context handle for the Processor to initialize itself.
 */
public interface TezProcessorContext extends TezTaskContext {

  /**
   * Set the overall progress of this Task Attempt
   * @param progress Progress in the range from [0.0 - 1.0f]
   */
  public void setProgress(float progress);

  /**
   * Check whether this attempt can commit its output
   * @return true if commit allowed
   * @throws IOException
   */
  public boolean canCommit() throws IOException;

}
