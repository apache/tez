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
import { or } from '@ember/object/computed';
import { assign } from '@ember/polyfills';
import { attr } from '@ember-data/model';

import DAGInfoModel from './dag-info';

export default DAGInfoModel.extend({
  needs: {
    am: {
      type: "dagAm",
      idKey: "entityID",
      loadType: "demand",
      queryParams: function (model) {
        return {
          dagID: parseInt(model.get("index")),
          counters: "*"
        };
      },
      urlParams: function (model) {
        return {
          app_id: model.get("appID"),
          version: model.get("amWsVersion") || "1"
        };
      }
    },
    app: {
      type: ["AhsApp", "appRm"],
      idKey: "appID",
      loadType: function (record) {
        if(record.get("queueName") && record.get("atsStatus") !== "RUNNING") {
          return "demand";
        }
      },
      silent: true
    },
    info: {
      type: "dagInfo",
      idKey: "entityID",
      loadType: "demand",
      silent: true
    }
  },

  name: attr("string"),

  submitter: attr("string"),

  // Serialize when required
  vertices: or("dagPlan.vertices", "info.dagPlan.vertices"),
  edges: or("dagPlan.edges", "info.dagPlan.edges"),
  vertexGroups: or("dagPlan.vertexGroups", "info.dagPlan.vertexGroups"),

  domain: attr("string"),
  containerLogs: attr("object"),
  queueName: attr("string"),
  queue: or('queueName', 'app.queue'),

  vertexIdNameMap: attr("object"),
  vertexNameIdMap: attr("object"),

  callerID: attr("string"),
  callerContext: or("callerData.callerContext", "info.callerData.callerContext"),
  callerDescription: or("callerData.callerDescription", "info.callerData.callerDescription"),
  callerType: or("callerData.callerType", "info.callerData.callerType"),

  amWsVersion: attr("string"),
  failedTaskAttempts: attr("number"),

  finalStatus: computed("status", "failedTaskAttempts", function () {
    var status = this.status;
    if(status === "SUCCEEDED" && this.failedTaskAttempts) {
      status = "SUCCEEDED_WITH_FAILURES";
    }
    return status;
  }),

  info: attr("object"),

  counterGroupsHash: computed("am.counterGroupsHash", "_counterGroups", "info.counterGroupsHash", function () {
    var amCounters = this.get("am.counterGroupsHash"),
        atsCounters = this.get("info.counterGroupsHash") || this._super();
    return amCounters ? assign({}, atsCounters, amCounters) : atsCounters;
  })
});
