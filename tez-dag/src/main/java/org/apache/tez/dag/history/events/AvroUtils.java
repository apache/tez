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

package org.apache.tez.dag.history.events;

import java.util.ArrayList;

import org.apache.avro.util.Utf8;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.dag.history.avro.TezCounter;
import org.apache.tez.dag.history.avro.TezCounterGroup;
import org.apache.tez.dag.history.avro.TezCounters;

public class AvroUtils {

  public static TezCounters toAvro(
      org.apache.tez.common.counters.TezCounters counters) {
    TezCounters result = new TezCounters();
    result.groups = new ArrayList<TezCounterGroup>(0);
    if (counters == null) return result;
    for (CounterGroup group : counters) {
      TezCounterGroup g = new TezCounterGroup();
      g.name = new Utf8(group.getName());
      g.displayName = new Utf8(group.getDisplayName());
      g.counts = new ArrayList<TezCounter>(group.size());
      for (org.apache.tez.common.counters.TezCounter counter : group) {
        TezCounter c = new TezCounter();
        c.name = new Utf8(counter.getName());
        c.displayName = new Utf8(counter.getDisplayName());
        c.value = counter.getValue();
        g.counts.add(c);
      }
      result.groups.add(g);
    }
    return result;
  }

}
