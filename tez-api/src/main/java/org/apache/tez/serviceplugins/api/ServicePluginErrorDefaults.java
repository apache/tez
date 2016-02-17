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

package org.apache.tez.serviceplugins.api;/*
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

import org.apache.hadoop.classification.InterfaceAudience;

/**
 * A default set of errors from ServicePlugins
 *
 * Errors are marked as fatal or non-fatal for the Application.
 * Fatal errors cause the AM to go down.
 *
 */
@InterfaceAudience.Public
public enum ServicePluginErrorDefaults implements ServicePluginError {
  /**
   * Indicates that the service is currently unavailable.
   * This is a temporary error.
   */
  SERVICE_UNAVAILABLE(ErrorType.TEMPORARY),

  /** Indicates that the service is in an inconsistent state.
   * This is a fatal error.
   */
  INCONSISTENT_STATE(ErrorType.PERMANENT),

  /**
   * Other temporary error,
   */
  OTHER(ErrorType.TEMPORARY),

  /**
   * Other fatal error.
   */
  OTHER_FATAL(ErrorType.PERMANENT);

  private ErrorType errorType;

  ServicePluginErrorDefaults(ErrorType errorType) {
    this.errorType = errorType;
  }

  @Override
  public Enum getEnum() {
    return this;
  }

  @Override
  public ErrorType getErrorType() {
    return errorType;
  }
}
