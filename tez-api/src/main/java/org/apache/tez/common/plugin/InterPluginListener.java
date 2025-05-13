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

package org.apache.tez.common.plugin;

import java.util.Objects;
import java.util.UUID;

/**
 * Callback called when a value is sent to {@link ServicePluginAware}.
 */
public abstract class InterPluginListener {
  /**
   * Identifier for this instance.
   */
  private UUID uniqueID = UUID.randomUUID();

  /**
   * Called when a value is sent.
   * @param value sent value.
   */
  public abstract void onSentValue(final Object value);

  @Override
  public final boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InterPluginListener that = (InterPluginListener) o;
    return Objects.equals(uniqueID, that.uniqueID);
  }

  @Override
  public final int hashCode() {
    return Objects.hash(uniqueID);
  }
}
