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

    firstTaskStartTime: 'otherinfo.stats.firstTaskStartTime',

    numTasks: 'otherinfo.numTasks',
    failedTasks: 'otherinfo.numFailedTasks',
    sucessfulTasks: 'otherinfo.numSucceededTasks',
    killedTasks: 'otherinfo.numKilledTasks',

    failedTaskAttempts: 'otherinfo.numFailedTaskAttempts',
    killedTaskAttempts: 'otherinfo.numKilledTaskAttempts',

    processorClassName: getProcessorClass,
  }
});
