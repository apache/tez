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
import org.apache.hadoop.classification.InterfaceAudience.Private;
import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import com.google.common.base.Preconditions;

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
    Preconditions.checkNotNull(userPayload);
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
  
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, className);
    // TODO: TEZ-305 - using protobuf serde instead of Writable serde.
    ByteBuffer bb = DagTypeConverters.convertFromTezUserPayload(userPayload);
    if (bb == null) {
      out.writeInt(-1);
    } else {
      int size = bb.limit() - bb.position();
      if (size == 0) {
        out.writeInt(-1);
      } else {
        out.writeInt(size);
        byte[] bytes = new byte[size];
        // This modified the ByteBuffer, and primarily works since UserPayload.getByteBuffer
        // return a new copy each time
        bb.get(bytes);
        // TODO: TEZ-305 - should be more efficient by using protobuf serde.
        out.write(bytes);
      }
      out.writeInt(userPayload.getVersion());
    }
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
