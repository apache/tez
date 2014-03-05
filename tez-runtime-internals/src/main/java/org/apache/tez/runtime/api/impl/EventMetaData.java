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

package org.apache.tez.runtime.api.impl;

import static com.google.common.base.Preconditions.checkNotNull;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import javax.annotation.Nullable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.StringInterner;
import org.apache.tez.dag.records.TezTaskAttemptID;

/**
 * Class that encapsulates all the information to identify the unique
 * object that either generated an Event or is the recipient of an Event.
 */
public class EventMetaData implements Writable {

  public static enum EventProducerConsumerType {
    INPUT,
    PROCESSOR,
    OUTPUT,
    SYSTEM
  }

  /**
   * Producer Type ( one of Input/Output/Processor ) that generated the Event
   * or Consumer Type that will consume the Event.
   */
  private EventProducerConsumerType producerConsumerType;

  /**
   * Name of the vertex where the event was generated.
   */
  private String taskVertexName;

  /**
   * Name of the vertex to which the Input or Output is connected to.
   */
  private String edgeVertexName;

  /**
   * Task Attempt ID
   */
  private TezTaskAttemptID taskAttemptID;

  public EventMetaData() {
  }

  public EventMetaData(EventProducerConsumerType generator,
      String taskVertexName, String edgeVertexName,
      @Nullable TezTaskAttemptID taskAttemptID) {
    checkNotNull(generator, "generator is null");
    checkNotNull(taskVertexName, "taskVertexName is null");
    checkNotNull(edgeVertexName, "edgeVertexName is null");
    this.producerConsumerType = generator;
    this.taskVertexName = StringInterner.weakIntern(taskVertexName);
    this.edgeVertexName = StringInterner.weakIntern(edgeVertexName);
    this.taskAttemptID = taskAttemptID;
  }

  public EventProducerConsumerType getEventGenerator() {
    return producerConsumerType;
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
    out.writeInt(producerConsumerType.ordinal());
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
    if(taskAttemptID != null) {
      out.writeBoolean(true);
      taskAttemptID.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    producerConsumerType = EventProducerConsumerType.values()[in.readInt()];
    if (in.readBoolean()) {
      taskVertexName = StringInterner.weakIntern(in.readUTF());
    }
    if (in.readBoolean()) {
      edgeVertexName = StringInterner.weakIntern(in.readUTF());
    }
    if (in.readBoolean()) {
      taskAttemptID = TezTaskAttemptID.readTezTaskAttemptID(in);
    }
  }

  @Override
  public String toString() {
    return "{ producerConsumerType=" + producerConsumerType
        + ", taskVertexName=" + taskVertexName
        + ", edgeVertexName=" + edgeVertexName
        + ", taskAttemptId=" + (taskAttemptID == null? "null" : taskAttemptID)
        + " }";
  }
}
