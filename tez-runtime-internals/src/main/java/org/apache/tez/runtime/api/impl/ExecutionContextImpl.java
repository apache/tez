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

package org.apache.tez.runtime.api.impl;

import org.apache.tez.runtime.api.ExecutionContext;

public class ExecutionContextImpl implements ExecutionContext {

  private final String hostname;
  private String containerId = null;

  public ExecutionContextImpl(String hostname) {
    this.hostname = hostname;
  }

  @Override
  public String getHostName() {
    return hostname;
  }

  public ExecutionContext withContainerId(String containerId) {
    this.containerId = containerId;
    return this;
  }

  public String getContainerId() {
    return containerId;
  }
}
