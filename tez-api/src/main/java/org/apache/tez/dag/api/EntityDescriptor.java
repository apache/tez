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
   * @return
   */
  public T setUserPayload(UserPayload userPayload) {
    Preconditions.checkNotNull(userPayload);
    this.userPayload = userPayload;
    return (T) this;
  }

  /**
   * Provide a human-readable version of the user payload that can be
   * used in the History UI
   * @param historyText History text
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
    byte[] bb = DagTypeConverters.convertFromTezUserPayload(userPayload);
    if (bb == null) {
      out.writeInt(-1);
    } else {
      out.writeInt(bb.length);
      out.write(bb);
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
      this.userPayload = DagTypeConverters.convertToTezUserPayload(bb, version);
    }
  }
}
