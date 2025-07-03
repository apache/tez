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

package org.apache.tez.serviceplugins.api;

import java.util.Objects;

/**
 * Id for {@link org.apache.tez.common.plugin.InterPluginCommunicator}.
 */
public final class InterPluginId {
  /**
   * Id that identifies the object.
   */
  private Object id;

  /**
   * Create new object.
   * @param newId id
   */
  private InterPluginId(final Object newId) {
    this.id = newId;
  }

  /**
   * Conveniernce method to construct an instance of this class.
   * @param id id for the object.
   * @return the constructed object.
   */
  public static InterPluginId fromObject(final Object id) {
    return new InterPluginId(id);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    InterPluginId that = (InterPluginId) o;
    return Objects.equals(id, that.id);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id);
  }
}
