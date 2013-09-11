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
package org.apache.tez.engine.lib.input;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.tez.common.TezUtils;
import org.apache.tez.engine.common.localshuffle.LocalShuffle;
import org.apache.tez.engine.newapi.Event;
import org.apache.tez.engine.newapi.LogicalInput;
import org.apache.tez.engine.newapi.TezInputContext;

/**
 * <code>LocalMergedInput</code> in an {@link LogicalInput} which shuffles intermediate
 * sorted data, merges them and provides key/<values> to the consumer. 
 */
public class LocalMergedInput extends ShuffledMergedInput {


  // TODO NEWTEZ Fix CombineProcessor
  //private CombineInput raw;

  @Override
  public List<Event> initialize(TezInputContext inputContext) throws IOException {
    this.inputContext = inputContext;
    this.conf = TezUtils.createConfFromUserPayload(inputContext.getUserPayload());

    LocalShuffle localShuffle = new LocalShuffle(inputContext, conf, numInputs);
    // TODO NEWTEZ async run and checkIfComplete methods
    rawIter = localShuffle.run();
    return Collections.emptyList();
  }

  @Override
  public List<Event> close() throws IOException {
    rawIter.close();
    return Collections.emptyList();
  }
}
