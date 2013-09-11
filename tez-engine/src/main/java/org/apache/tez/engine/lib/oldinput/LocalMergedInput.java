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
package org.apache.tez.engine.lib.oldinput;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.tez.common.TezEngineTaskContext;
import org.apache.tez.engine.api.Input;
import org.apache.tez.engine.api.Master;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;

/**
 * {@link LocalMergedInput} in an {@link Input} which shuffles intermediate
 * sorted data, merges them and provides key/<values> to the consumer. 
 */
public class LocalMergedInput extends OldShuffledMergedInput {

  public LocalMergedInput(TezEngineTaskContext task, int index) {
    super(task, index);
  }

  public void initialize(Configuration conf, Master master) throws IOException,
      InterruptedException {
  }

  public boolean hasNext() throws IOException, InterruptedException {
    return false;
  }

  public Object getNextKey() throws IOException, InterruptedException {
    return null;
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public Iterable getNextValues() 
      throws IOException, InterruptedException {
    return null;
  }

  public float getProgress() throws IOException, InterruptedException {
    return 0f;
  }

  public void close() throws IOException {
  }

  public TezRawKeyValueIterator getIterator() {
    return null;
  }
  
}
