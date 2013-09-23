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

import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.io.Writable;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.records.TezTaskAttemptID;

// TODO NEWTEZ Get rid of this.
public interface TezTaskStatus extends Writable {

  //enumeration for reporting current phase of a task.
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static enum Phase{STARTING, MAP, SHUFFLE, SORT, REDUCE, CLEANUP}

  // what state is the task in?
  @InterfaceAudience.Private
  @InterfaceStability.Unstable
  public static enum State {RUNNING, SUCCEEDED, FAILED, UNASSIGNED, KILLED, 
                            COMMIT_PENDING, FAILED_UNCLEAN, KILLED_UNCLEAN}

  public abstract TezTaskAttemptID getTaskAttemptId();

  public abstract float getProgress();

  public abstract void setProgress(float progress);

  public abstract State getRunState();

  public abstract void setRunState(State runState);

  public abstract String getDiagnosticInfo();

  public abstract void setDiagnosticInfo(String info);

  // TODOTEZDAG Remove stateString / rename
  public abstract String getStateString();

  public abstract void setStateString(String stateString);

  public abstract long getFinishTime();

  public abstract void setFinishTime(long finishTime);
  
  // TODOTEZDAG Can shuffle / merge be made generic ? Otherwise just a single finish time.
  public abstract long getShuffleFinishTime();

  public abstract void setShuffleFinishTime(long shuffleFinishTime);
  
  public abstract long getMapFinishTime();

  public abstract void setMapFinishTime(long mapFinishTime);
  
  public abstract long getSortFinishTime();
  
  public abstract void setSortFinishTime(long sortFinishTime);
  
  public abstract long getStartTime();
  
  public abstract void setStartTime(long startTime);

  // TODOTEZDAG Remove phase
  public abstract Phase getPhase();

  public abstract void setPhase(Phase phase);

  public abstract TezCounters getCounters();

  public abstract void setCounters(TezCounters counters);

  public abstract List<TezTaskAttemptID> getFailedDependencies();

  public abstract void addFailedDependency(TezTaskAttemptID taskAttempttId);

  public abstract void clearStatus();

  public abstract void statusUpdate(float f, String string, TezCounters counters);

  // TODOTEZDAG maybe remove ?
  public abstract long getLocalOutputSize();

  public abstract void setOutputSize(long l);

}
