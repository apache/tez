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

package org.apache.tez.history.parser.utils;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.util.StringInterner;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.tez.common.counters.CounterGroup;
import org.apache.tez.common.counters.TezCounter;
import org.apache.tez.common.counters.TezCounters;
import org.apache.tez.dag.history.logging.EntityTypes;
import org.apache.tez.history.parser.datamodel.Constants;
import org.apache.tez.history.parser.datamodel.Event;
import org.apache.tez.history.parser.datamodel.TaskAttemptInfo.DataDependencyEvent;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.List;

@InterfaceAudience.Private
public class Utils {

  private static final String LOG4J_CONFIGURATION = "log4j.configuration";

  /**
   * Parse tez counters from json
   *
   * @param jsonObject
   * @return TezCounters
   * @throws JSONException
   */
  public static TezCounters parseTezCountersFromJSON(JSONObject jsonObject)
      throws JSONException {
    TezCounters counters = new TezCounters();

    if (jsonObject == null) {
      return counters; //empty counters.
    }

    final JSONArray counterGroupNodes = jsonObject.optJSONArray(Constants.COUNTER_GROUPS);
    if (counterGroupNodes != null) {
      for (int i = 0; i < counterGroupNodes.length(); i++) {
        JSONObject counterGroupNode = counterGroupNodes.optJSONObject(i);
        final String groupName = counterGroupNode.optString(Constants.COUNTER_GROUP_NAME);
        final String groupDisplayName = counterGroupNode.optString(
            Constants.COUNTER_GROUP_DISPLAY_NAME, groupName);

        CounterGroup group = counters.addGroup(groupName, groupDisplayName);

        final JSONArray counterNodes = counterGroupNode.optJSONArray(Constants.COUNTERS);

        //Parse counter nodes
        for (int j = 0; j < counterNodes.length(); j++) {
          JSONObject counterNode = counterNodes.optJSONObject(j);
          final String counterName = counterNode.getString(Constants.COUNTER_NAME);
          final String counterDisplayName =
              counterNode.optString(Constants.COUNTER_DISPLAY_NAME, counterName);
          final long counterValue = counterNode.getLong(Constants.COUNTER_VALUE);
          TezCounter counter = group.findCounter(
              counterName,
              counterDisplayName);
          counter.setValue(counterValue);
        }
      }
    }
    return counters;
  }
  
  public static List<DataDependencyEvent> parseDataEventDependencyFromJSON(JSONObject jsonObject) 
      throws JSONException {
    List<DataDependencyEvent> events = Lists.newArrayList();
    JSONArray fields = jsonObject.optJSONArray(Constants.LAST_DATA_EVENTS);
    for (int i=0; i<fields.length(); i++) {
      JSONObject eventMap = fields.getJSONObject(i);
      events.add(new DataDependencyEvent(
          StringInterner.weakIntern(eventMap.optString(EntityTypes.TEZ_TASK_ATTEMPT_ID.name())),
          eventMap.optLong(Constants.TIMESTAMP)));
    }
    return events;
  }

  /**
   * Parse events from json
   *
   * @param eventNodes
   * @param eventList
   * @throws JSONException
   */
  public static void parseEvents(JSONArray eventNodes, List<Event> eventList) throws
      JSONException {
    if (eventNodes == null) {
      return;
    }
    for (int i = 0; i < eventNodes.length(); i++) {
      JSONObject eventNode = eventNodes.optJSONObject(i);
      final String eventInfo = eventNode.optString(Constants.EVENT_INFO);
      final String eventType = eventNode.optString(Constants.EVENT_TYPE);
      final long time = eventNode.optLong(Constants.EVENT_TIME_STAMP);

      Event event = new Event(eventInfo, eventType, time);

      eventList.add(event);

    }
  }

  public static void setupRootLogger() {
    if (Strings.isNullOrEmpty(System.getProperty(LOG4J_CONFIGURATION))) {
      //By default print to console with INFO level
      Logger.getRootLogger().
          addAppender(new ConsoleAppender(new PatternLayout(PatternLayout.TTCC_CONVERSION_PATTERN)));
      Logger.getRootLogger().setLevel(Level.INFO);
    }
  }

}
