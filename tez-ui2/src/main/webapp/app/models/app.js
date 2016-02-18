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

import TimelineModel from './timeline';

export default TimelineModel.extend({
  needs:{
    app: {
      type: ["AhsApp", "appRm"],
      idKey: "appID",
      silent: true
    }
  },

  appID: Ember.computed("entityID", function () {
    return this.get("entityID").substr(4);
  }),

  domain: DS.attr("string"),

  user: DS.attr("string"),

  buildTime: DS.attr("string"),
  tezRevision: DS.attr("string"),
  tezVersion: DS.attr("string"),

  configs: DS.attr("object"),
});
