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

package org.apache.tez.runtime.api.events;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.impl.TaskStatistics;

public class TaskStatusUpdateEvent extends Event implements Writable {

  private TezCounters tezCounters;
  private float progress;
  private TaskStatistics statistics;

  public TaskStatusUpdateEvent() {
  }

  public TaskStatusUpdateEvent(TezCounters tezCounters, float progress, TaskStatistics statistics) {
    this.tezCounters = tezCounters;
    this.progress = progress;
    this.statistics = statistics;
  }

  public TezCounters getCounters() {
    return tezCounters;
  }

  public float getProgress() {
    return progress;
  }
  
  public TaskStatistics getStatistics() {
    return statistics;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeFloat(progress);
    if (tezCounters != null) {
      out.writeBoolean(true);
      tezCounters.write(out);
    } else {
      out.writeBoolean(false);
    }
    if (statistics != null) {
      out.writeBoolean(true);
      statistics.write(out);
    } else {
      out.writeBoolean(false);
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    progress = in.readFloat();
    if (in.readBoolean()) {
      tezCounters = new TezCounters();
      tezCounters.readFields(in);
    }
    if (in.readBoolean()) {
      statistics = new TaskStatistics();
      statistics.readFields(in);
    }
  }

}
