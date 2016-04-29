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

function getProcessorClass(source) {
  var name = Ember.get(source, 'otherinfo.processorClassName') || "";
  return name.substr(name.lastIndexOf('.') + 1);
}

export default TimelineSerializer.extend({
  maps: {
    name: 'otherinfo.vertexName',

    _initTime: 'otherinfo.initTime',
    _startTime: 'otherinfo.startTime',
    _endTime: 'otherinfo.endTime',
    _firstTaskStartTime: 'otherinfo.stats.firstTaskStartTime',
    _lastTaskFinishTime: 'otherinfo.stats.lastTaskFinishTime',

    totalTasks: 'otherinfo.numTasks',
    _failedTasks: 'otherinfo.numFailedTasks',
    _succeededTasks: 'otherinfo.numSucceededTasks',
    _killedTasks: 'otherinfo.numKilledTasks',

    _failedTaskAttempts: 'otherinfo.numFailedTaskAttempts',
    _killedTaskAttempts: 'otherinfo.numKilledTaskAttempts',

    minDuration:  'otherinfo.stats.minTaskDuration',
    maxDuration:  'otherinfo.stats.maxTaskDuration',
    avgDuration:  'otherinfo.stats.avgTaskDuration',

    firstTasksToStart:  'otherinfo.stats.firstTasksToStart',
    lastTasksToFinish:  'otherinfo.stats.lastTasksToFinish',
    shortestDurationTasks:  'otherinfo.stats.shortestDurationTasks',
    longestDurationTasks:  'otherinfo.stats.longestDurationTasks',

    processorClassName: getProcessorClass,

    dagID: 'primaryfilters.TEZ_DAG_ID.0',

    servicePlugin: 'otherinfo.servicePlugin',
  }
});
