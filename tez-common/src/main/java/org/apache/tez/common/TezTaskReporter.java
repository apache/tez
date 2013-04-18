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

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;
import org.apache.hadoop.util.Progressable;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.records.TezTaskAttemptID;
import org.apache.tez.engine.records.TezTaskDependencyCompletionEventsUpdate;

public interface TezTaskReporter extends Progressable, Master {

  public void setStatus(String status);

  public float getProgress();

  public void setProgress(float progress);
  
  public void progress();

  public TezCounter getCounter(String group, String name);

  public TezCounter getCounter(Enum<?> name);

  public void incrCounter(String group, String counter, long amount);

  public void incrCounter(Enum<?> key, long amount);

  public void reportFatalError(TezTaskAttemptID taskAttemptId, 
      Throwable exception, String logMsg);

  public final TezTaskReporter NULL = new TezTaskReporter() {

    @Override
    public TezTaskDependencyCompletionEventsUpdate getDependentTasksCompletionEvents(
        int fromEventIdx, int maxEventsToFetch,
        TezTaskAttemptID reduce) {
      return null;
    }
    
    @Override
    public void setStatus(String status) {
    }
    
    @Override
    public void setProgress(float progress) {
    }
    
    @Override
    public void progress() {
    }
    
    @Override
    public void incrCounter(Enum<?> key, long amount) {
    }
    
    @Override
    public void incrCounter(String group, String counter, long amount) {
    }
    
    @Override
    public float getProgress() {
      return 0.0f;
    }
    
    @Override
    public TezCounter getCounter(Enum<?> name) {
      return null;
    }
    
    @Override
    public TezCounter getCounter(String group, String name) {
      return null;
    }

    @Override
    public void reportFatalError(TezTaskAttemptID taskAttemptId,
        Throwable exception, String logMsg) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public long getProtocolVersion(String protocol, long clientVersion)
        throws IOException {
      // TODO TEZAM3
      return 0;
    }

    @Override
    public ProtocolSignature getProtocolSignature(String protocol,
        long clientVersion, int clientMethodsHash) throws IOException {
      // TODO TEZAM3
      return null;
    }
  };
}
