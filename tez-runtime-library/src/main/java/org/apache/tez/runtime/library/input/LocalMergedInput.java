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

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.tez.common.TezUtils;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.TezInputContext;
import org.apache.tez.runtime.library.common.localshuffle.LocalShuffle;

/**
 * <code>LocalMergedInput</code> in an {@link LogicalInput} which shuffles intermediate
 * sorted data, merges them and provides key/<values> to the consumer. 
 */
public class LocalMergedInput extends ShuffledMergedInputLegacy {

  @Override
  public List<Event> initialize(TezInputContext inputContext) throws IOException {
    this.inputContext = inputContext;
    this.inputContext.requestInitialMemory(0l, null); // mandatory call.
    this.inputContext.inputIsReady();
    this.conf = TezUtils.createConfFromUserPayload(inputContext.getUserPayload());

    if (numInputs == 0) {
      return Collections.emptyList();
    }

    LocalShuffle localShuffle = new LocalShuffle(inputContext, conf, numInputs);
    rawIter = localShuffle.run();
    createValuesIterator();
    return Collections.emptyList();
  }
  
  @Override
  public void start() throws IOException {
  }

  @Override
  public List<Event> close() throws IOException {
    if (numInputs != 0) {
      rawIter.close();
    }
    return Collections.emptyList();
  }
}