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

export default AMTimelineModel.extend({
  needs: {
    dag: {
      type: "dag",
      idKey: "dagID",
      silent: true
    },
    am: {
      type: "attemptAm",
      idKey: "entityID",
      loadType: "demand",
      queryParams: function (model) {
        var vertexIndex = parseInt(model.get("vertexIndex")),
            taskIndex = parseInt(model.get("taskIndex")),
            attemptIndex = parseInt(model.get("index"));
        return {
          attemptID: `${vertexIndex}_${taskIndex}_${attemptIndex}`,
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

  taskID: DS.attr('string'),
  taskIndex: Ember.computed("taskID", function () {
    var id = this.get("taskID") || "";
    return id.substr(id.lastIndexOf('_') + 1);
  }),

  vertexID: DS.attr('string'),
  vertexIndex: Ember.computed("vertexID", function () {
    var id = this.get("vertexID") || "";
    return id.substr(id.lastIndexOf('_') + 1);
  }),
  vertexName: Ember.computed("vertexID", "dag", function () {
    var vertexID = this.get("vertexID");
    return this.get(`dag.vertexIdNameMap.${vertexID}`);
  }),

  dagID: DS.attr('string'),
  dag: DS.attr('object'), // Auto-loaded by need

  containerID: DS.attr('string'),
  nodeID: DS.attr('string'),

  inProgressLogsURL: DS.attr('string'),
  completedLogsURL: DS.attr('string'),
  logURL: Ember.computed("entityID", "inProgressLogsURL", "completedLogsURL", "dag.isComplete", function () {
    var logURL = this.get("inProgressLogsURL");

    if(logURL) {
      if(logURL.indexOf("://") === -1) {
        let attemptID = this.get("entityID"),
            yarnProtocol = this.get('env.app.yarnProtocol');
        return `${yarnProtocol}://${logURL}/syslog_${attemptID}`;
      }
      else { // LLAP log link
        return this.get("dag.isComplete") ? this.get("completedLogsURL") : logURL;
      }
    }
  }),

  containerLogURL: DS.attr('string'),
});
