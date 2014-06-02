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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.tez.common.TezUserPayload;

public abstract class TezEntityDescriptor implements Writable {

  protected TezUserPayload userPayload;
  private String className;

  @Private // for Writable
  public TezEntityDescriptor() {
  }
  
  public TezEntityDescriptor(String className) {
    this.className = className;
  }

  public byte[] getUserPayload() {
    return (userPayload == null) ? null : userPayload.getPayload();
  }

  public TezEntityDescriptor setUserPayload(byte[] userPayload) {
    this.userPayload = DagTypeConverters.convertToTezUserPayload(userPayload);
    return this;
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
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    this.className = Text.readString(in);
    int payloadLength = in.readInt();
    if (payloadLength != -1) {
      byte[] bb = new byte[payloadLength];
      in.readFully(bb);
      this.userPayload = DagTypeConverters.convertToTezUserPayload(bb);
    }
  }
}
