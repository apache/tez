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

package org.apache.tez.engine.newapi.impl;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.tez.dag.records.TezTaskAttemptID;

/**
 * Class that encapsulates all the information to identify the unique
 * object that either generated an Event or is the recipient of an Event.
 */
public class EventMetaData implements Writable {

  public static enum EventGenerator {
    INPUT,
    PROCESSOR,
    OUTPUT,
    SYSTEM
  }

  /**
   * Source Type ( one of Input/Output/Processor ) that generated the Event.
   */
  private EventGenerator generator;

  /**
   * Name of the vertex where the event was generated.
   */
  private String taskVertexName;

  /**
   * Name of the vertex to which the Input or Output is connected to.
   */
  private String edgeVertexName;

  /**
   * i'th physical input/output that this event maps to.
   */
  private int index;

  /**
   * Task Attempt ID
   */
  private TezTaskAttemptID taskAttemptID;

  public EventMetaData() {
  }

  public EventMetaData(EventGenerator generator,
      String taskVertexName, String edgeVertexName,
      TezTaskAttemptID taskAttemptID) {
    this.generator = generator;
    this.taskVertexName = taskVertexName;
    this.edgeVertexName = edgeVertexName;
    this.taskAttemptID = taskAttemptID;
  }

  public EventGenerator getEventGenerator() {
    return generator;
  }

  public TezTaskAttemptID getTaskAttemptID() {
    return taskAttemptID;
  }

  public String getTaskVertexName() {
    return taskVertexName;
  }

  public String getEdgeVertexName() {
    return edgeVertexName;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(generator.ordinal());
    if (taskVertexName != null) {
      out.writeBoolean(true);
      out.writeUTF(taskVertexName);
    } else {
      out.writeBoolean(false);
    }
    if (edgeVertexName != null) {
      out.writeBoolean(true);
      out.writeUTF(edgeVertexName);
    } else {
      out.writeBoolean(false);
    }
    taskAttemptID.write(out);
    out.writeInt(index);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    generator = EventGenerator.values()[in.readInt()];
    if (in.readBoolean()) {
      taskVertexName = in.readUTF();
    }
    if (in.readBoolean()) {
      edgeVertexName = in.readUTF();
    }
    taskAttemptID = new TezTaskAttemptID();
    taskAttemptID.readFields(in);
    index = in.readInt();
  }

  public int getIndex() {
    return index;
  }

  public void setIndex(int index) {
    this.index = index;
  }

}
