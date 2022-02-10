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

export default AMTimelineModel.extend({
  needs: {
    dag: {
      type: "dag",
      idKey: "dagID",
      silent: true
    },
    am: {
      type: "taskAm",
      idKey: "entityID",
      loadType: "demand",
      queryParams: function (model) {
        var vertexIndex = parseInt(model.get("vertexIndex")),
            taskIndex = parseInt(model.get("index"));
        return {
          taskID: `${vertexIndex}_${taskIndex}`,
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

  vertexID: attr('string'),
  vertexIndex: computed("vertexID", function () {
    var id = this.vertexID || "";
    return id.substr(id.lastIndexOf('_') + 1);
  }),
  vertexName: computed("vertexID", "dag", function () {
    var vertexID = this.vertexID;
    return this.get(`dag.vertexIdNameMap.${vertexID}`);
  }),

  dagID: attr('string'),
  dag: attr('object'), // Auto-loaded by need

  failedTaskAttempts: attr('number'),

  successfulAttemptID: attr('string'),
  attemptIDs: attr("object"),
});
