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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/**
 * Describes a given user code entity. Consists of the name of the class implementing
 * the user logic and a payload that can be used to configure an object instance of
 * that class. In addition some history information can be set for logging/debugging.
  * <br>This is not supposed to be extended by users. Users are expected to use the derived
  * classes for specific entities
 */
@Public
@SuppressWarnings("unchecked")
public abstract class EntityDescriptor<T extends EntityDescriptor<T>> implements Writable {

  private static final int SERIALIZE_BUFFER_SIZE = 8192;
  private UserPayload userPayload = null;
  private String className;
  protected String historyText;

  @Private // for Writable
  public EntityDescriptor() {
  }

  public EntityDescriptor(String className) {
    this.className = className;
  }

  public UserPayload getUserPayload() {
    return userPayload;
  }

  /**
   * Set user payload for this entity descriptor
   * @param userPayload User Payload
   * @return this object for further chained method calls
   */
  public T setUserPayload(UserPayload userPayload) {
    Objects.requireNonNull(userPayload);
    this.userPayload = userPayload;
    return (T) this;
  }

  /**
   * Provide a human-readable version of the user payload that can be
   * used in the TEZ UI
   * @param historyText History text
   * For better support in the UI, the history text should be a json-encoded string.
   * The following keys in the json object will be recognized:
   *    "desc" : A string-value describing the entity
   *    "config" : A key-value map to represent configuration
   * @return this object for further chained method calls
   */
  public T setHistoryText(String historyText) {
    this.historyText = historyText;
    return (T) this;
  }

  @Private // Internal use only
  public String getHistoryText() {
    return this.historyText;
  }

  public String getClassName() {
    return this.className;
  }

  void writeSingular(DataOutput out, ByteBuffer bb) throws IOException {
    out.write(bb.array(), 0, bb.array().length);
  }

  void writeSegmented(DataOutput out, ByteBuffer bb) throws IOException {
    // This code is just for fallback in case serialization is changed to
    // use something other than DataOutputBuffer.
    int len;
    byte[] buf = new byte[SERIALIZE_BUFFER_SIZE];
    do {
      len = Math.min(bb.remaining(), SERIALIZE_BUFFER_SIZE);
      bb.get(buf, 0, len);
      out.write(buf, 0, len);
    } while (bb.remaining() > 0);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, className);
    // TODO: TEZ-305 - using protobuf serde instead of Writable serde.
    ByteBuffer bb = DagTypeConverters.convertFromTezUserPayload(userPayload);
    if (bb == null || bb.remaining() == 0) {
      out.writeInt(-1);
      return;
    }

    // write size
    out.writeInt(bb.remaining());
    if (bb.hasArray()) {
      writeSingular(out, bb);
    } else {
      writeSegmented(out, bb);
    }
    out.writeInt(userPayload.getVersion());
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.className = Text.readString(in);
    int payloadLength = in.readInt();
    if (payloadLength != -1) {
      byte[] bb = new byte[payloadLength];
      in.readFully(bb);
      int version =in.readInt();
      this.userPayload = DagTypeConverters.convertToTezUserPayload(ByteBuffer.wrap(bb), version);
    }
  }

  @Override
  public String toString() {
    boolean hasPayload =
        userPayload == null ? false : userPayload.getPayload() == null ? false : true;
    return "ClassName=" + className + ", hasPayload=" + hasPayload;
  }
}
