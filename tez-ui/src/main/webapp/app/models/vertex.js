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

import { computed } from '@ember/object';
import { attr } from '@ember-data/model';

import AMTimelineModel from './am-timeline';

function valueComputerFactory(path1, path2, path3) {
  return function () {
    var value = this.get(path1);
    if(path2 && (value === undefined || value === null)){
       value = this.get(path2);
    }
    if(path3 && (value === undefined || value === null)){
       value = this.get(path3);
    }
    return value;
  };
}

export default AMTimelineModel.extend({
  needs: {
    dag: {
      type: "dag",
      idKey: "dagID",
      silent: true
    },
    am: {
      type: "vertexAm",
      idKey: "entityID",
      loadType: "demand",
      queryParams: function (model) {
        return {
          vertexID: parseInt(model.get("index")),
          dagID: parseInt(model.get("dag.index")),
          counters: "*"
        };
      },
      urlParams: function (model) {
        return {
          app_id: model.get("appID"),
          version: model.get("dag.amWsVersion") || "1"
        };
      }
    }
  },

  name: attr('string'),

  _initTime: attr('number'),
  _startTime: attr('number'),
  _endTime: attr('number'),
  _firstTaskStartTime: attr('number'),
  _lastTaskFinishTime: attr('number'),

  initTime: computed("am.initTime", "_initTime",
    valueComputerFactory("am.initTime", "_initTime")
  ),
  startTime: computed("firstTaskStartTime", "am.startTime", "_startTime",
    valueComputerFactory("firstTaskStartTime", "am.startTime", "_startTime")
  ),
  endTime: computed("am.endTime", "_endTime",
    valueComputerFactory("am.endTime", "_endTime")
  ),
  firstTaskStartTime: computed("am.firstTaskStartTime", "_firstTaskStartTime",
    valueComputerFactory("am.firstTaskStartTime", "_firstTaskStartTime")
  ),
  lastTaskFinishTime: computed("am.lastTaskFinishTime", "_lastTaskFinishTime",
    valueComputerFactory("am.lastTaskFinishTime", "_lastTaskFinishTime")
  ),

  totalTasks: attr('number'),
  _failedTasks: attr('number'),
  _succeededTasks: attr('number'),
  _killedTasks: attr('number'),

  failedTasks: computed("am.failedTasks", "_failedTasks",
    valueComputerFactory("am.failedTasks", "_failedTasks")
  ),
  succeededTasks: computed("am.succeededTasks", "_succeededTasks",
    valueComputerFactory("am.succeededTasks", "_succeededTasks")
  ),
  killedTasks: computed("am.killedTasks", "_killedTasks",
    valueComputerFactory("am.killedTasks", "_killedTasks")
  ),

  finalStatus: computed("status", "failedTaskAttempts", function () {
    var status = this.status;
    if(status === "SUCCEEDED" && this.failedTaskAttempts) {
      status = "SUCCEEDED_WITH_FAILURES";
    }
    return status;
  }),

  runningTasks: computed("am.runningTasks", "status", function () {
    var runningTasks = this.get("am.runningTasks");
    if(runningTasks === undefined) {
      runningTasks = this.status === 'SUCCEEDED' ? 0 : null;
    }
    return  runningTasks;
  }),
  pendingTasks: computed("totalTasks", "succeededTasks", "runningTasks", function () {
    var pendingTasks = null,
        runningTasks = this.runningTasks,
        totalTasks = this.totalTasks;
    if(totalTasks!== null && runningTasks !== null) {
      pendingTasks = totalTasks - this.succeededTasks - runningTasks;
    }
    return pendingTasks;
  }),

  _failedTaskAttempts: attr('number'),
  _killedTaskAttempts: attr('number'),
  failedTaskAttempts: computed("am.failedTaskAttempts", "_failedTaskAttempts",
    valueComputerFactory("am.failedTaskAttempts", "_failedTaskAttempts")
  ),
  killedTaskAttempts: computed("am.killedTaskAttempts", "_killedTaskAttempts",
    valueComputerFactory("am.killedTaskAttempts", "_killedTaskAttempts")
  ),

  minDuration: attr('number'),
  maxDuration: attr('number'),
  avgDuration: attr('number'),

  firstTasksToStart: attr("object"),
  lastTasksToFinish: attr("object"),
  shortestDurationTasks: attr("object"),
  longestDurationTasks: attr("object"),

  processorClassName: attr('string'),

  dagID: attr('string'),
  dag: attr('object'), // Auto-loaded by need

  description: computed("dag.vertices", "name", function () {
    try {
      let vertex = this.get("dag.vertices").findBy("vertexName", this.name);
      return JSON.parse(vertex.userPayloadAsText).desc;
    }catch(e) {}
  }),

  servicePlugin: attr('object'),
});
