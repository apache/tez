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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceAudience.Public
@InterfaceStability.Unstable
/**
 * Represents errors from a ServicePlugin. The default implementation {@link ServicePluginErrorDefaults}
 * lists a basic set of errors.
 * This can be extended by implementing this interface, if the default set is not adequate
 */
public interface ServicePluginError {

  enum ErrorType {
    TEMPORARY, PERMANENT,
  }

  /**
   * Get the enum representation
   *
   * @return an enum representation of the ServicePluginError
   */
  Enum getEnum();

  /**
   * The type of the error
   *
   * @return the type of the error
   */
  ErrorType getErrorType();


}
