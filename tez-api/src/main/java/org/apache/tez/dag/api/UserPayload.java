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

package org.apache.tez.dag.api;

import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Public;

/**
 * Wrapper class to hold user payloads
 * Provides a version to help in evolving the payloads
 */
@Public
public final class UserPayload {
  private final byte[] payload;
  private final int version;

  public UserPayload(@Nullable byte[] payload) {
    this(payload, 0);
  }

  public UserPayload(@Nullable byte[] payload, int version) {
    this.payload = payload;
    this.version = version;
  }

  @Nullable
  public byte[] getPayload() {
    return payload;
  }

  public int getVersion() {
    return version;
  }

  public boolean hasPayload() {
    return payload != null;
  }
}
