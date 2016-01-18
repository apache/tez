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
/*
  Inherited properties

  entityID - String
  appID - Computed from entityID

  status - String
  progress - Computed from status

  startTime - Number
  endTime - Number
  duration - Computed from start & end times

  counterGroups - Array
  counterHash - Computed from counterGroups
*/

export default TimelineModel.extend({
  needs: {
    dag: {
      type: "dag",
      idKey: "dagID",
      silent: true
    }
  },

  name: DS.attr('string'),

  firstTaskStartTime: DS.attr('number'),
  lastTaskFinishTime: DS.attr('number'),

  totalTasks: DS.attr('number'),
  failedTasks: DS.attr('number'),
  successfulTasks: DS.attr('number'),
  killedTasks: DS.attr('number'),

  runningTasks: Ember.computed("status", function () {
    return this.get("status") === 'SUCCEEDED' ? 0 : null;
  }),
  pendingTasks: Ember.computed("status", function () {
    return this.get("status") === 'SUCCEEDED' ? 0 : null;
  }),

  failedTaskAttempts: DS.attr('number'),
  killedTaskAttempts: DS.attr('number'),

  minDuration: DS.attr('number'),
  maxDuration: DS.attr('number'),
  avgDuration: DS.attr('number'),

  processorClassName: DS.attr('string'),

  dagID: DS.attr('string'),
  dag: DS.attr('object'), // Auto-loaded by need
});
