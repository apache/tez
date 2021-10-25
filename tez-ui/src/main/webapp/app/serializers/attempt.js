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

import TimelineSerializer from './timeline';

function createContainerLogURL(source) {
  var logURL = get(source, 'otherinfo.inProgressLogsURL'),
      yarnProtocol = this.get('env.app.yarnProtocol');

  if(logURL && logURL.indexOf("://") === -1) {
    return `${yarnProtocol}://${logURL}`;
  }
}

export default TimelineSerializer.extend({
  maps: {
    taskID: 'primaryfilters.TEZ_TASK_ID.0',
    vertexID: 'primaryfilters.TEZ_VERTEX_ID.0',
    dagID: 'primaryfilters.TEZ_DAG_ID.0',

    containerID: 'otherinfo.containerId',
    nodeID: 'otherinfo.nodeId',

    inProgressLogsURL: "otherinfo.inProgressLogsURL",
    completedLogsURL: "otherinfo.completedLogsURL",

    containerLogURL: createContainerLogURL
  }
});
