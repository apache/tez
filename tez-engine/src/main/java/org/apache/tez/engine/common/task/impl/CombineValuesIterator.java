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

package org.apache.tez.engine.common.task.impl;

import java.io.IOException;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.RawComparator;
import org.apache.tez.common.TezTaskReporter;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.engine.common.sort.impl.TezRawKeyValueIterator;

/** Iterator to return Combined values */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class CombineValuesIterator<KEY,VALUE>
extends ValuesIterator<KEY,VALUE> {

  private final TezCounter combineInputCounter;

  public CombineValuesIterator(TezRawKeyValueIterator in,
      RawComparator<KEY> comparator, Class<KEY> keyClass,
      Class<VALUE> valClass, Configuration conf, TezTaskReporter reporter,
      TezCounter combineInputCounter) throws IOException {
    super(in, comparator, keyClass, valClass, conf, reporter);
    this.combineInputCounter = combineInputCounter;
  }

  public VALUE next() {
    combineInputCounter.increment(1);
    return super.next();
  }
}
