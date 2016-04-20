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

function createLogURL(source) {
  var logURL = Ember.get(source, 'otherinfo.inProgressLogsURL'),
      attemptID = Ember.get(source, 'entity'),
      yarnProtocol = this.get('env.app.yarnProtocol');

  if(logURL) {
    return `${yarnProtocol}://${logURL}/syslog_${attemptID}`;
  }
}

export default TimelineSerializer.extend({
  maps: {
    taskID: 'primaryfilters.TEZ_TASK_ID.0',
    vertexID: 'primaryfilters.TEZ_VERTEX_ID.0',
    dagID: 'primaryfilters.TEZ_DAG_ID.0',

    containerID: 'otherinfo.containerId',
    nodeID: 'otherinfo.nodeId',

    logURL: createLogURL
  }
});
