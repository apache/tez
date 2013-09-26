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

package org.apache.tez.runtime.library.input;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezJobConfig;
import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.library.api.KVReader;
import org.apache.tez.runtime.library.broadcast.input.BroadcastKVReader;
import org.apache.tez.runtime.library.broadcast.input.BroadcastShuffleManager;

import com.google.common.base.Preconditions;

public class ShuffledUnorderedKVInput implements LogicalInput {

  private static final Log LOG = LogFactory.getLog(ShuffledUnorderedKVInput.class);
  
  private Configuration conf;
  private int numInputs = -1;
  private BroadcastShuffleManager shuffleManager;
  @SuppressWarnings("rawtypes")
  private BroadcastKVReader kvReader;
  
  
  
  public ShuffledUnorderedKVInput() {
  }

  @SuppressWarnings("rawtypes")
  @Override
  public List<Event> initialize(TezInputContext inputContext) throws Exception {
    Preconditions.checkArgument(numInputs != -1, "Number of Inputs has not been set");
    this.conf = TezUtils.createConfFromUserPayload(inputContext.getUserPayload());
    this.conf.setStrings(TezJobConfig.LOCAL_DIRS, inputContext.getWorkDirs());
    
    this.shuffleManager = new BroadcastShuffleManager(inputContext, conf, numInputs);
    this.shuffleManager.run();
    this.kvReader = new BroadcastKVReader(shuffleManager, conf);
    return null;
  }

  @Override
  public KVReader getReader() throws Exception {
    return this.kvReader;
  }

  @Override
  public void handleEvents(List<Event> inputEvents) {
    shuffleManager.handleEvents(inputEvents);
  }

  @Override
  public List<Event> close() throws Exception {
    this.shuffleManager.shutdown();
    return null;
  }

  @Override
  public void setNumPhysicalInputs(int numInputs) {
    this.numInputs = numInputs;
  }
}
