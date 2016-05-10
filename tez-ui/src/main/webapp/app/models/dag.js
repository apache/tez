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
      silent: true
    }
  },

  name: DS.attr("string"),

  submitter: DS.attr("string"),

  // Serialize when required
  vertices: DS.attr('object'),
  edges: DS.attr('object'),
  vertexGroups: DS.attr('object'),

  domain: DS.attr("string"),
  containerLogs: DS.attr("object"),
  queue: Ember.computed("app", function () {
    return this.get("app.queue");
  }),

  vertexIdNameMap: DS.attr("object"),

  callerID: DS.attr("string"),
  callerType: DS.attr("string"),
  callerInfo: DS.attr("string"),

  amWsVersion: DS.attr("string"),
});
