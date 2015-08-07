/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tez.common;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Defines a lifecycle for a Service. The typical implementation for services when used within the
 * Tez framework would be
 * 1. Construct the object.
 * 2. initialize()
 * 3. start()
 * stop() - is invoked when the service is no longer required, and could be invoked while in any
 * state, in case of failures
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public interface ServicePluginLifecycle {

  /**
   * Perform any additional initialization which may be required beyond the constructor.
   */
  void initialize() throws Exception;

  /**
   * Start the service. This will be invoked after initialization.
   */
  void start() throws Exception;

  /**
   * Shutdown the service. This will be invoked when the service is shutting down.
   */
  void shutdown() throws Exception;

}
