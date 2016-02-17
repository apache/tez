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

package org.apache.tez.serviceplugins.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.tez.dag.api.UserPayload;

/**
 * Base interface for ServicePluginContexts
 */
public interface ServicePluginContextBase {

  /**
   * Get the UserPayload that was configured while setting up the launcher
   *
   * @return the initially configured user payload
   */
  UserPayload getInitialUserPayload();

  /**
   * Get information on the currently executing dag
   * @return info on the currently running dag, or null if no dag is executing
   */
  @Nullable
  DagInfo getCurrentDagInfo();

  /**
   * Report an error from the service. This results in the specific DAG being killed.
   *
   * @param servicePluginError the error category
   * @param message      A diagnostic message associated with this error
   * @param dagInfo      the affected dag
   */
  void reportError(@Nonnull ServicePluginError servicePluginError, String message, DagInfo dagInfo);
}
