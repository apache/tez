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

import com.google.common.annotations.VisibleForTesting;
import java.nio.ByteBuffer;
import javax.annotation.Nullable;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Wrapper class to hold user payloads
 * Provides a version to help in evolving the payloads
 */
@Public
public final class UserPayload {
  private final ByteBuffer payload;
  private final int version;
  private static final ByteBuffer EMPTY_BYTE = ByteBuffer.wrap(new byte[0]);

  private UserPayload(@Nullable ByteBuffer payload) {
    this(payload, 0);
  }

  private UserPayload(@Nullable ByteBuffer payload, int version) {
    this.payload = payload == null ? EMPTY_BYTE : payload;
    this.version = version;
  }

  /**
   * Create a UserPayload instance which encapsulates a {@link java.nio.ByteBuffer}. The version number used will be 0
   * @param payload the {@link java.nio.ByteBuffer} payload
   * @return an instance of {@link org.apache.tez.dag.api.UserPayload}
   */
  public static UserPayload create(@Nullable ByteBuffer payload) {
    return new UserPayload(payload);
  }

  /**
   * Create an explicitly versioned UserPayload instance which encapsulates a {@link java.nio.ByteBuffer}.
   * @param payload the {@link java.nio.ByteBuffer} payload
   * @return an instance of {@link org.apache.tez.dag.api.UserPayload}
   */
  public static UserPayload create(@Nullable ByteBuffer payload, int version) {
    return new UserPayload(payload, version);
  }

  /**
   * Return the payload as a read-only ByteBuffer.
   * @return read-only ByteBuffer.
   */
  @Nullable
  public ByteBuffer getPayload() {
    // Note: Several bits of serialization, including deepCopyAsArray depend on a new instance of the
    // ByteBuffer being returned, since they modify it. If changing this code to return the same
    // ByteBuffer - deepCopyAsArray and TezEntityDescriptor need to be looked at.
    return payload == EMPTY_BYTE ? null : payload.asReadOnlyBuffer();
  }

  public int getVersion() {
    return version;
  }

  public boolean hasPayload() {
    return payload != null && payload != EMPTY_BYTE;
  }

  @InterfaceStability.Unstable
  @VisibleForTesting
  public byte[] deepCopyAsArray() {
    ByteBuffer src = getPayload();
    if (src != null) {
      byte[] dst = new byte[src.limit() - src.position()];
      src.get(dst);
      return dst;
    } else {
      return new byte[0];
    }
  }
}
