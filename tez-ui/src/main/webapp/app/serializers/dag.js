/*global more*/
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

var MoreObject = more.Object;

function getStatus(source) {
  var status = Ember.get(source, 'otherinfo.status') || Ember.get(source, 'primaryfilters.status.0'),
      event = source.events;

  if(!status && event) {
    if(event.findBy('eventtype', 'DAG_STARTED')) {
      status = 'RUNNING';
    }
  }

  return status;
}

function getStartTime(source) {
  var time = Ember.get(source, 'otherinfo.startTime'),
      event = source.events;

  if(!time && event) {
    event = event.findBy('eventtype', 'DAG_STARTED');
    if(event) {
      time = event.timestamp;
    }
  }

  return time;
}

function getEndTime(source) {
  var time = Ember.get(source, 'otherinfo.endTime'),
      event = source.events;

  if(!time && event) {
    event = event.findBy('eventtype', 'DAG_FINISHED');
    if(event) {
      time = event.timestamp;
    }
  }

  return time;
}

function getContainerLogs(source) {
  var containerLogs = [],
      otherinfo = Ember.get(source, 'otherinfo');

  if(!otherinfo) {
    return undefined;
  }

  for (var key in otherinfo) {
    if (key.indexOf('inProgressLogsURL_') === 0) {
      let logs = Ember.get(source, 'otherinfo.' + key);
      if (logs.indexOf('http') !== 0) {
        logs = 'http://' + logs;
      }
      let attemptID = key.substring(18);
      containerLogs.push({
        text : attemptID,
        href: logs
      });
    }
  }

  return containerLogs;
}

function getIdNameMap(source) {
  var nameIdMap = Ember.get(source, 'otherinfo.vertexNameIdMapping'),
      idNameMap = {};

  if(nameIdMap) {
    MoreObject.forEach(nameIdMap, function (name, id) {
      idNameMap[id] = name;
    });
  }

  return idNameMap;
}

export default TimelineSerializer.extend({
  maps: {
    name: 'primaryfilters.dagName.0',

    submitter: 'primaryfilters.user.0',

    atsStatus: getStatus,
    // progress

    startTime: getStartTime,
    endTime: getEndTime,
    // duration

    vertices: 'otherinfo.dagPlan.vertices',
    edges: 'otherinfo.dagPlan.edges',
    vertexGroups: 'otherinfo.dagPlan.vertexGroups',

    // appID
    domain: 'domain',

    queueName: 'otherinfo.queueName',

    containerLogs: getContainerLogs,

    vertexIdNameMap: getIdNameMap,
    vertexNameIdMap: 'otherinfo.vertexNameIdMapping',

    callerID: 'primaryfilters.callerId.0',
    callerContext: 'callerContext',
    callerDescription: 'callerDescription',
    callerType: 'callerType',

    amWsVersion: 'otherinfo.amWebServiceVersion',
  },

  normalizeResourceHash: function (resourceHash) {
    var data = resourceHash.data,
        dagInfo = Ember.get(resourceHash, "data.otherinfo.dagPlan.dagInfo"), // New style, from TEZ-2851
        dagContext = Ember.get(resourceHash, "data.otherinfo.dagPlan.dagContext"); // Old style

    if(dagContext) {
      data.callerContext = Ember.String.classify((Ember.get(dagContext, "context")||"").toLowerCase());
      data.callerDescription = Ember.get(dagContext, "description");
      data.callerType = Ember.get(dagContext, "callerType");
    }
    else if(dagInfo) {
      let infoObj = {};
      try{
        infoObj = JSON.parse(dagInfo);
      }catch(e){
        infoObj = dagInfo;
      }

      data.callerContext = Ember.get(infoObj, "context");
      data.callerDescription = Ember.get(infoObj, "description") || Ember.get(dagInfo, "blob") || dagInfo;
    }

    return resourceHash;
  },

  extractAttributes: function (modelClass, resourceHash) {
    return this._super(modelClass, this.normalizeResourceHash(resourceHash));
  },

});
