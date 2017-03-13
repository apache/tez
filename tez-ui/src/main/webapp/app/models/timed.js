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

  startTime: DS.attr('number'),
  endTime: DS.attr('number'),

  duration: Ember.computed("startTime", "endTime", function () {
    var startTime = this.get("startTime"),
        endTime = this.get("endTime");

    if(startTime > 0 && endTime > 0) {
      if(startTime > endTime) {
        let delta = startTime - endTime;
        return new Error(`Start time is greater than end time by ${delta} msecs!`);
      }

      return endTime - startTime;
    }
  }),

});
