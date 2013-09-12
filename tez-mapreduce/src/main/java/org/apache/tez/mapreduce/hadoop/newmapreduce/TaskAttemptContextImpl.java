/* Licensed to the Apache Software Foundation (ASF) under one
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

package org.apache.tez.mapreduce.hadoop.newmapreduce;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskAttemptID;
import org.apache.hadoop.mapred.TaskID;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.tez.engine.newapi.TezInputContext;
import org.apache.tez.mapreduce.common.Utils;

// NOTE: NEWTEZ: This is a copy of org.apache.tez.mapreduce.hadoop.mapred (not mapreduce). mapred likely does not need it's own copy of this class.
// Meant for use by the "mapreduce" API

@InterfaceAudience.Private
@InterfaceStability.Unstable
public class TaskAttemptContextImpl
       extends org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl {
  private TezInputContext inputContext;

  // FIXME we need to use DAG Id but we are using App Id
  public TaskAttemptContextImpl(Configuration conf, TezInputContext inputContext) {
    // TODO NEWTEZ Figure out how to compute the TaskType - MAP or REDUCE. For
    // SimpleInput, it likely doesn't matter - but setting it to MAP
    // TODO NEWTEZ Can the jt Identifier string be inputContext.getUniqueId ?
    super(conf, new TaskAttemptID(
        new TaskID(String.valueOf(inputContext.getApplicationId()
            .getClusterTimestamp()), inputContext.getApplicationId().getId(),
            TaskType.MAP, inputContext.getTaskIndex()),
        inputContext.getAttemptNumber()));
    this.inputContext = inputContext;
    
  }
  
  @Override
  public float getProgress() {
    // TODO NEWTEZ Will this break anything ?
    return 0.0f;
  }

  @Override
  public Counter getCounter(Enum<?> counterName) {
    return Utils.getMRCounter(inputContext.getCounters().findCounter(counterName));
  }

  @Override
  public Counter getCounter(String groupName, String counterName) {
    return Utils.getMRCounter(inputContext.getCounters().findCounter(groupName, counterName));
  }

  /**
   * Report progress.
   */
  @Override
  public void progress() {
    // Nothing to do.
  }

  /**
   * Set the current status of the task to the given string.
   */
  @Override
  public void setStatus(String status) {
    setStatusString(status);
    // Nothing to do until InputContext supports some kind of custom string
    // diagnostics.
  }
}
