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

import { get } from '@ember/object';
import MoreObject from '../utils/more-object';

import DAGInfoSerializer from './dag-info';

function getStatus(source) {
  var status = get(source, 'otherinfo.status') || get(source, 'primaryfilters.status.0'),
      event = source.events;

  if(!status && event) {
    if(event.find(({eventtype}) => eventtype === 'DAG_STARTED')) {
      status = 'RUNNING';
    }
  }

  return status;
}

function getStartTime(source) {
  var time = get(source, 'otherinfo.startTime'),
      event = source.events;

  if(!time && event) {
    event = event.find(({eventtype}) => eventtype === 'DAG_STARTED');
    if(event) {
      time = event.timestamp;
    }
  }

  return time;
}

function getEndTime(source) {
  var time = get(source, 'otherinfo.endTime'),
      event = source.events;

  if(!time && event) {
    event = event.find(({eventtype}) => eventtype === 'DAG_FINISHED');
    if(event) {
      time = event.timestamp;
    }
  }

  return time;
}

function getContainerLogs(source) {
  var containerLogs = [],
      otherinfo = source.otherinfo;

  if(!otherinfo) {
    return undefined;
  }

  for (var key in otherinfo) {
    if (key.indexOf('inProgressLogsURL_') === 0) {
      let logs = get(source, 'otherinfo.' + key);
      if (logs.indexOf("://") === -1) {
        let yarnProtocol = this.get('env.app.yarnProtocol');
        logs = yarnProtocol + '://' + logs;
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
  var nameIdMap = get(source, 'otherinfo.vertexNameIdMapping'),
      idNameMap = {};

  if(nameIdMap) {
    MoreObject.forEach(nameIdMap, function (name, id) {
      idNameMap[id] = name;
    });
  }

  return idNameMap;
}

export default DAGInfoSerializer.extend({
  maps: {
    name: 'primaryfilters.dagName.0',

    submitter: 'primaryfilters.user.0',

    callerID: 'primaryfilters.callerId.0',

    atsStatus: getStatus,
    // progress

    startTime: getStartTime,
    endTime: getEndTime,
    // duration

    // appID
    domain: 'domain',

    queueName: 'otherinfo.queueName',

    containerLogs: getContainerLogs,

    vertexIdNameMap: getIdNameMap,
    vertexNameIdMap: 'otherinfo.vertexNameIdMapping',

    amWsVersion: 'otherinfo.amWebServiceVersion',
    failedTaskAttempts: 'otherinfo.numFailedTaskAttempts',
  }
});
