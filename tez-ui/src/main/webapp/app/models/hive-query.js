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

import AbstractModel from './abstract';

export default AbstractModel.extend({

  needs: {
    dag: {
      type: "dag",
      silent: true,
      queryParams: function (model) {
        return {
          callerId: model.get("entityID")
        };
      },
    }
  },

  queryName: DS.attr('string'),

  queryText: DS.attr("string"),

  dag: DS.attr('object'),

  sessionID: DS.attr('string'),
  operationID: DS.attr('string'),

  instanceType: DS.attr('string'),
  executionMode: DS.attr('string'), // Would be ideally TEZ

  domain: DS.attr('string'),
  threadName: DS.attr('string'),
  queue: DS.attr('string'),
  version: DS.attr('string'),

  hiveAddress: DS.attr('string'),
  clientAddress: DS.attr('string'),

  user: DS.attr('string'),
  requestUser: DS.attr('string'),

  tablesRead: DS.attr('object'),
  tablesWritten: DS.attr('object'),

  status: DS.attr('string'),

  configsJSON: DS.attr("string"),

  startTime: DS.attr("number"),
  endTime: DS.attr("number"),
  duration: Ember.computed("startTime", "endTime", function () {
    var duration = this.get("endTime") - this.get("startTime");
    return duration > 0 ? duration : null;
  }),

});
