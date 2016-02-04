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
import DS from 'ember-data';

import AMTimelineModel from './am-timeline';

function valueComputerFactory(path1, path2) {
  return function () {
    var value = this.get(path1);
    if(value === undefined || value === null){
      value = this.get(path2);
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
          app_id: model.get("appID")
        };
      }
    }
  },

  name: DS.attr('string'),

  firstTaskStartTime: DS.attr('number'),
  lastTaskFinishTime: DS.attr('number'),

  totalTasks: DS.attr('number'),
  _failedTasks: DS.attr('number'),
  _succeededTasks: DS.attr('number'),
  _killedTasks: DS.attr('number'),
  failedTasks: Ember.computed("am.failedTasks", "_failedTasks",
    valueComputerFactory("am.failedTasks", "_failedTasks")
  ),
  succeededTasks: Ember.computed("am.succeededTasks", "_succeededTasks",
    valueComputerFactory("am.succeededTasks", "_succeededTasks")
  ),
  killedTasks: Ember.computed("am.killedTasks", "_killedTasks",
    valueComputerFactory("am.killedTasks", "_killedTasks")
  ),

  runningTasks: Ember.computed("am.runningTasks", "status", function () {
    var runningTasks = this.get("am.runningTasks");
    if(runningTasks === undefined) {
      runningTasks = this.get("status") === 'SUCCEEDED' ? 0 : null;
    }
    return  runningTasks;
  }),
  pendingTasks: Ember.computed("totalTasks", "succeededTasks", "runningTasks", function () {
    var pendingTasks = null,
        runningTasks = this.get("runningTasks"),
        totalTasks = this.get("totalTasks");
    if(totalTasks!== null && runningTasks !== null) {
      pendingTasks = totalTasks - this.get("succeededTasks") - runningTasks;
    }
    return pendingTasks;
  }),

  _failedTaskAttempts: DS.attr('number'),
  _killedTaskAttempts: DS.attr('number'),
  failedTaskAttempts: Ember.computed("am.failedTaskAttempts", "_failedTaskAttempts",
    valueComputerFactory("am.failedTaskAttempts", "_failedTaskAttempts")
  ),
  killedTaskAttempts: Ember.computed("am.killedTaskAttempts", "_killedTaskAttempts",
    valueComputerFactory("am.killedTaskAttempts", "_killedTaskAttempts")
  ),

  minDuration: DS.attr('number'),
  maxDuration: DS.attr('number'),
  avgDuration: DS.attr('number'),

  firstTasksToStart: DS.attr("object"),
  lastTasksToFinish: DS.attr("object"),
  shortestDurationTasks: DS.attr("object"),
  longestDurationTasks: DS.attr("object"),

  processorClassName: DS.attr('string'),

  dagID: DS.attr('string'),
  dag: DS.attr('object'), // Auto-loaded by need
});
