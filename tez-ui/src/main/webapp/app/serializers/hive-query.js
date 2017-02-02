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
  var time = Ember.get(source, 'otherinfo.endTime'),
      event = source.events;

  if(!time && event) {
    event = event.findBy('eventtype', 'QUERY_COMPLETED');
    if(event) {
      time = event.timestamp;
    }
  }

  return time;
}

function getStatus(source) {
  var status = Ember.get(source, 'otherinfo.STATUS');

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
    queryName: 'primaryfilters.queryname.0',

    queryText: 'otherinfo.QUERY.queryText',

    sessionID: 'otherinfo.INVOKER_INFO',
    operationID: 'primaryfilters.operationid.0',

    instanceType: 'otherinfo.HIVE_INSTANCE_TYPE',
    executionMode: 'primaryfilters.executionmode.0',

    domain: 'domain',
    threadName: 'otherinfo.THREAD_NAME',
    queue: 'primaryfilters.queue.0',
    version: 'otherinfo.VERSION',

    hiveAddress: 'otherinfo.HIVE_ADDRESS',
    clientAddress: 'otherinfo.CLIENT_IP_ADDRESS',

    user: 'primaryfilters.user.0',
    requestUser: 'primaryfilters.requestuser.0',

    tablesRead: 'primaryfilters.tablesread',
    tablesWritten: 'primaryfilters.tableswritten',

    status: getStatus,

    configsJSON: "otherinfo.CONF",

    startTime: 'starttime',
    endTime: getEndTime,

    perf: "otherinfo.PERF"
  },

  extractAttributes: function (modelClass, resourceHash) {
    var data = resourceHash.data,
        query = Ember.get(resourceHash, "data.otherinfo.QUERY"),
        perf = Ember.get(resourceHash, "data.otherinfo.PERF");

    if(query) {
      try{
        data.otherinfo.QUERY = JSON.parse(query);
      }catch(e){}
    }

    if(!data.otherinfo.CLIENT_IP_ADDRESS) {
      data.otherinfo.CLIENT_IP_ADDRESS = data.otherinfo.HIVE_ADDRESS;
    }

    if(perf) {
      try{
        let PERF = JSON.parse(perf);
        PERF["PostATSHook"] = PERF["PostHook.org.apache.hadoop.hive.ql.hooks.ATSHook"];
        data.otherinfo.PERF = PERF;
      }catch(e){}
    }

    data.primaryfilters = data.primaryfilters || {};
    if(!Ember.get(data, "primaryfilters.tablesread.length")) {
      data.primaryfilters.tablesread = new Error("None");
    }
    if(!Ember.get(data, "primaryfilters.tableswritten.length")) {
      data.primaryfilters.tableswritten = new Error("None");
    }

    return this._super(modelClass, resourceHash);
  },

});
