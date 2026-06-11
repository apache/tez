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

import Ember from 'ember';

import TimelineSerializer from './timeline';

function getEndTime(source) {
  var time = Ember.get(source, 'otherInfo.endTime'),
      event = source.events;

  if(!time && event) {
    event = event.findBy('eventType', 'QUERY_COMPLETED');
    if(event) {
      time = event.timestamp;
    }
  }

  return time;
}

function getStatus(source) {
  var status = Ember.get(source, 'otherInfo.STATUS');

  switch(status) {
    case true:
      return "SUCCEEDED";
    case false:
      return "FAILED";
    default:
      return "RUNNING";
  }
}

export default TimelineSerializer.extend({
  maps: {
    queryText: 'otherInfo.QUERY.queryText',

    dagID: 'primaryFilters.DAG_ID',
    appID: 'primaryFilters.APP_ID',
    sessionID: 'otherInfo.INVOKER_INFO',
    operationID: 'primaryFilters.operationid.0',
    llapAppID: 'otherInfo.LLAP_APP_ID',

    instanceType: 'otherInfo.HIVE_INSTANCE_TYPE',
    executionMode: 'primaryFilters.executionmode.0',

    domain: 'domainId',
    threadName: 'otherInfo.THREAD_NAME',
    queue: 'primaryFilters.queue.0',
    version: 'otherInfo.VERSION',

    hiveAddress: 'otherInfo.HIVE_ADDRESS',
    clientAddress: 'otherInfo.CLIENT_IP_ADDRESS',

    user: 'primaryFilters.user.0',
    requestUser: 'primaryFilters.requestuser.0',

    tablesRead: 'primaryFilters.tablesread',
    tablesWritten: 'primaryFilters.tableswritten',

    status: getStatus,

    configsJSON: "otherInfo.CONF",

    startTime: 'startTime',
    endTime: getEndTime,

    perf: "otherInfo.PERF"
  },

  extractAttributes: function (modelClass, resourceHash) {
    var data = resourceHash.data,
        query = Ember.get(resourceHash, "data.otherInfo.QUERY"),
        perf = Ember.get(resourceHash, "data.otherInfo.PERF");

    if(query) {
      try{
        data.otherInfo.QUERY = JSON.parse(query);
      }catch(e){}
    }

    if(!data.otherInfo.CLIENT_IP_ADDRESS) {
      data.otherInfo.CLIENT_IP_ADDRESS = data.otherInfo.HIVE_ADDRESS;
    }

    if(perf) {
      try{
        let PERF = JSON.parse(perf);
        PERF["PostATSHook"] = PERF["PostHook.org.apache.hadoop.hive.ql.hooks.ATSHook"];
        data.otherInfo.PERF = PERF;
      }catch(e){}
    }

    data.primaryFilters = data.primaryFilters || {};
    if(!Ember.get(data, "primaryFilters.tablesread.length")) {
      data.primaryFilters.tablesread = new Error("None");
    }
    if(!Ember.get(data, "primaryFilters.tableswritten.length")) {
      data.primaryFilters.tableswritten = new Error("None");
    }

    return this._super(modelClass, resourceHash);
  },

});
