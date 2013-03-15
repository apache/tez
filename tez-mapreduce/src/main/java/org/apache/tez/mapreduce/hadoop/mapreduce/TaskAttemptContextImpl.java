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

package org.apache.tez.mapreduce.hadoop.mapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.Task;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.tez.mapreduce.hadoop.IDConverter;
import org.apache.tez.mapreduce.processor.MRTaskReporter;

/**
 * The context for task attempts.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptContextImpl extends JobContextImpl 
    implements TaskAttemptContext {
  private final TaskAttemptID taskId;
  private String status = "";
  private MRTaskReporter reporter;

  public TaskAttemptContextImpl(Configuration conf, 
                                TaskAttemptID taskId) {
    this(conf, taskId, null);
  }

  public TaskAttemptContextImpl(Configuration conf, 
      TaskAttemptID taskId, MRTaskReporter reporter) {
    super(conf, IDConverter.fromMRJobId(taskId.getJobID()));
    this.taskId = taskId;
    this.reporter = reporter;
  }

  /**
   * Get the unique name for this task attempt.
   */
  public TaskAttemptID getTaskAttemptID() {
    return taskId;
  }

  /**
   * Get the last set status message.
   * @return the current status message
   */
  public String getStatus() {
    return status;
  }

  public Counter getCounter(Enum<?> counterName) {
    return (Counter) reporter.getCounter(counterName);
  }

  public Counter getCounter(String groupName, String counterName) {
    return (Counter) reporter.getCounter(groupName, counterName);
  }

  /**
   * Report progress.
   */
  public void progress() {
    reporter.progress();
  }

  protected void setStatusString(String status) {
    this.status = status;
  }

  /**
   * Set the current status of the task to the given string.
   */
  public void setStatus(String status) {
    String normalizedStatus = Task.normalizeStatus(status, conf);
    setStatusString(normalizedStatus);
    reporter.setStatus(normalizedStatus);
  }

  public static class DummyReporter extends StatusReporter {
    public void setStatus(String s) {
    }
    public void progress() {
    }
    public Counter getCounter(Enum<?> name) {
      return new Counters().findCounter(name);
    }
    public Counter getCounter(String group, String name) {
      return new Counters().findCounter(group, name);
    }
    public float getProgress() {
      return 0f;
    }
  }
  
  public float getProgress() {
    return reporter.getProgress();
  }
}
