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

import DS from 'ember-data';

import AMModel from './am';

export default AMModel.extend({

  totalTasks: DS.attr("number"),
  succeededTasks: DS.attr("number"),
  runningTasks: DS.attr("number"),
  pendingTasks: DS.attr("number"),
  failedTaskAttempts: DS.attr("number"),
  killedTaskAttempts: DS.attr("number"),

  initTime: DS.attr('number'),
  startTime: DS.attr('number'),
  endTime: DS.attr('number'),
  firstTaskStartTime: DS.attr('number'),
  lastTaskFinishTime: DS.attr('number'),

});
