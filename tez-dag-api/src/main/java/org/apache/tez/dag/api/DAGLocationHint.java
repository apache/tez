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
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class DAGLocationHint implements Writable {

  private Map<String, VertexLocationHint> vertexLocationHints;

  public DAGLocationHint() {
    vertexLocationHints = new TreeMap<String, VertexLocationHint>();
  }

  public Map<String, VertexLocationHint> getVertexLocationHints() {
    return vertexLocationHints;
  }

  public VertexLocationHint getVertexLocationHint(String vertexName) {
    return vertexLocationHints.get(vertexName);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(vertexLocationHints.size());
    for (Entry<String, VertexLocationHint> entry :
        vertexLocationHints.entrySet()) {
      Text.writeString(out, entry.getKey());
      out.writeBoolean(entry.getValue() != null);
      if (entry.getValue() != null) {
        entry.getValue().write(out);
      }
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int entryCount = in.readInt();
    vertexLocationHints = new TreeMap<String, VertexLocationHint>();
    for (int i = 0; i < entryCount; ++i) {
      String vertexName = Text.readString(in);
      if (!in.readBoolean()) {
        vertexLocationHints.put(vertexName, null);
      } else {
        VertexLocationHint hint = new VertexLocationHint();
        hint.readFields(in);
        vertexLocationHints.put(vertexName, hint);
      }
    }
  }

  public static DAGLocationHint initDAGDagLocationHint(
      String locationHintFile) throws IOException {
    DataInput in = new DataInputStream(new FileInputStream(locationHintFile));
    DAGLocationHint dagLocationHint = new DAGLocationHint();
    dagLocationHint.readFields(in);
    return dagLocationHint;
  }

  public static void writeDAGDagLocationHint(
      DAGLocationHint dagLocationHint,
      String locationHintFile) throws IOException {
    DataOutput out = new DataOutputStream(new FileOutputStream(
        locationHintFile));
    dagLocationHint.write(out);
  }
}
