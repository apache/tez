/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

/**
 * Execution context for a running task
 *
 * This interface is not meant to be implemented by users
 */
@InterfaceAudience.Public
public interface ExecutionContext {

  /**
   * Get the hostname on which the JVM is running.
   * @return the hostname
   */
  public String getHostName();
}
