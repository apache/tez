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

package org.apache.tez.common;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class InputSpec implements Writable {

  private String vertexName;
  private int inDegree;
  private String inputClassName;
  
  public InputSpec() {
  }
  
  public InputSpec(String vertexName, int inDegree,
      String inputClassName) {
    this.vertexName = vertexName;
    this.inDegree = inDegree;
    this.inputClassName = inputClassName;
  }
  
  /**
   * @return the name of the input vertex.
   */
  public String getVertexName() {
    return this.vertexName;
  }
  
  /**
   * @return the number of inputs for this task, which will be available from
   *         the specified vertex.
   */
  public int getNumInputs() {
    return this.inDegree;
  }

  /**
   * @return Input class name
   */
  public String getInputClassName() {
    return this.inputClassName;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    Text.writeString(out, vertexName);
    out.writeInt(inDegree);
    Text.writeString(out, inputClassName);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    vertexName = Text.readString(in);
    this.inDegree = in.readInt();
    inputClassName = Text.readString(in);
  }
  
  @Override
  public String toString() {
    return "VertexName: " + vertexName + ", InDegree: " + inDegree
        + ", InputClassName=" + inputClassName;
  }
}
