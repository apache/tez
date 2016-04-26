/*global more*/
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

var MoreObject = more.Object;

// For all AM related entities that can be updated from AM
export default TimelineModel.extend({

  am: DS.attr("object"), // Represents data from am

  status: Ember.computed("am.status", "atsStatus", "app.status", "app.finalStatus", function () {
    return this.get("am.status") || this._super();
  }),

  progress: Ember.computed("am.progress", "status", function () {
    var progress = this.get("am.progress");
    return MoreObject.isNumber(progress) ? progress : this._super();
  }),

  counterGroupsHash: Ember.computed("am.counterGroupsHash", "_counterGroups", function () {
    var amCounters = this.get("am.counterGroupsHash"),
        atsCounters = this._super();
    return amCounters ? Ember.$.extend({}, atsCounters, amCounters) : atsCounters;
  })
});
