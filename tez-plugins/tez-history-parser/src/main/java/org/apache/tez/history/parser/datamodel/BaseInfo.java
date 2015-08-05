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

package org.apache.tez.history.parser.datamodel;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.history.parser.utils.Utils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.apache.hadoop.classification.InterfaceAudience.Public;
import static org.apache.hadoop.classification.InterfaceStability.Evolving;

@Public
@Evolving
public abstract class BaseInfo {

  protected TezCounters tezCounters;
  protected List<Event> eventList;

  BaseInfo(JSONObject jsonObject) throws JSONException {
    final JSONObject otherInfoNode = jsonObject.getJSONObject(Constants.OTHER_INFO);
    //parse tez counters
    tezCounters = Utils.parseTezCountersFromJSON(
        otherInfoNode.optJSONObject(Constants.COUNTERS));

    //parse events
    eventList = Lists.newArrayList();
    Utils.parseEvents(jsonObject.optJSONArray(Constants.EVENTS), eventList);
  }

  public TezCounters getTezCounters() {
    return tezCounters;
  }

  /**
   * Get start time w.r.t DAG
   *
   * @return long
   */
  public abstract long getStartTimeInterval();

  /**
   * Get finish time w.r.t DAG
   *
   * @return long
   */
  public abstract long getFinishTimeInterval();

  /**
   * Get absolute start time
   *
   * @return long
   */
  public abstract long getStartTime();

  /**
   * Get absolute finish time
   *
   * @return long
   */
  public abstract long getFinishTime();

  public abstract String getDiagnostics();

  public List<Event> getEvents() {
    return eventList;
  }

  /**
   * Get counter for a specific counter group name.
   * If counterGroupName is not mentioned, it would end up returning counter found in all
   * groups
   *
   * @param counterGroupName
   * @param counter
   * @return Map<String, TezCounter> tez counter at every counter group level
   */
  public Map<String, TezCounter> getCounter(String counterGroupName, String counter) {
    //TODO: FS, TaskCounters are directly getting added as TezCounters always pass those.  Need a
    // way to get rid of these.
    Map<String, TezCounter> result = Maps.newHashMap();
    Iterator<String> iterator = tezCounters.getGroupNames().iterator();
    boolean found = false;
    while (iterator.hasNext()) {
      CounterGroup counterGroup = tezCounters.getGroup(iterator.next());
      if (counterGroupName != null) {
        String groupName = counterGroup.getName();
        if (groupName.equals(counterGroupName)) {
          found = true;
        }
      }

      //Explicitly mention that no need to create the counter if not present
      TezCounter tezCounter = counterGroup.getUnderlyingGroup().findCounter(counter, false);
      if (tezCounter != null) {
        result.put(counterGroup.getName(), tezCounter);
      }

      if (found) {
        //Retrieved counter specific to a counter group. Safe to exit.
        break;
      }

    }
    return result;
  }

  /**
   * Find a counter in all counter groups
   *
   * @param counter
   * @return Map of countergroup to TezCounter mapping
   */
  public Map<String, TezCounter> getCounter(String counter) {
    return getCounter(null, counter);
  }

}
