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
      loadType: function (record) {
        if(record.get("dagID")) {
          return "demand";
        }
      },
    }
  },

  queryText: attr("string"),

  dag: attr('object'),

  dagID: attr('string'),
  appID: attr('string'),
  sessionID: attr('string'),
  operationID: attr('string'),
  llapAppID: attr('string'),

  instanceType: attr('string'),
  executionMode: attr('string'), // Would be ideally TEZ

  domain: attr('string'),
  threadName: attr('string'),
  queue: attr('string'),
  version: attr('string'),

  hiveAddress: attr('string'),
  clientAddress: attr('string'),

  user: attr('string'),
  requestUser: attr('string'),

  tablesRead: attr('object'),
  tablesWritten: attr('object'),

  status: attr('string'),

  configsJSON: attr("string"),

  startTime: attr("number"),
  endTime: attr("number"),
  duration: computed("startTime", "endTime", function () {
    var duration = this.endTime - this.startTime;
    return duration > 0 ? duration : null;
  }),

  perf: attr("Object"),

});
