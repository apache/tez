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

package org.apache.tez.client.registry;

public interface AMRegistryClientListener {

  void onAdd(AMRecord record);

  /**
   * Default implementation of {@code onUpdate} delegates to {@code onAdd}.
   *
   * <p>This provides a convenient backward-compatible behavior for consumers that
   * store {@link AMRecord} instances in collections keyed by something stable
   * (such as ApplicationId). In such cases, re-adding an {@link AMRecord}
   * effectively overwrites the previous entry, making an explicit update handler
   * unnecessary for many implementations.</p>
   *
   * @param record the updated {@link AMRecord} instance
   */
  default void onUpdate(AMRecord record){
    onAdd(record);
  }

  void onRemove(AMRecord record);
}
