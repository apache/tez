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

  name: DS.attr("string"),

  submitter: DS.attr("string"),

  // Serialize when required
  vertices: Ember.computed.or("dagPlan.vertices", "info.dagPlan.vertices"),
  edges: Ember.computed.or("dagPlan.edges", "info.dagPlan.edges"),
  vertexGroups: Ember.computed.or("dagPlan.vertexGroups", "info.dagPlan.vertexGroups"),

  domain: DS.attr("string"),
  containerLogs: DS.attr("object"),
  queueName: DS.attr("string"),
  queue: Ember.computed("queueName", "app", function () {
    return this.get("queueName") || this.get("app.queue");
  }),

  vertexIdNameMap: DS.attr("object"),
  vertexNameIdMap: DS.attr("object"),

  callerID: DS.attr("string"),
  callerContext: Ember.computed.or("callerData.callerContext", "info.callerData.callerContext"),
  callerDescription: Ember.computed.or("callerData.callerDescription", "info.callerData.callerDescription"),
  callerType: Ember.computed.or("callerData.callerType", "info.callerData.callerType"),

  amWsVersion: DS.attr("string"),

  info: DS.attr("object"),

  counterGroupsHash: Ember.computed("am.counterGroupsHash", "_counterGroups", "info.counterGroupsHash", function () {
    var amCounters = this.get("am.counterGroupsHash"),
        atsCounters = this.get("info.counterGroupsHash") || this._super();
    return amCounters ? Ember.$.extend({}, atsCounters, amCounters) : atsCounters;
  })
});
