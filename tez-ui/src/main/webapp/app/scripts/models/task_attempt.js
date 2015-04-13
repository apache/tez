/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

App.TaskAttempt = App.AbstractEntity.extend({
  index: function () {
    var id = this.get('id'),
        idPrefix = 'attempt_%@_'.fmt(this.get('dagID').substr(4));
    return id.indexOf(idPrefix) == 0 ? id.substr(idPrefix.length) : id;
  }.property('id'),

  // start time of the entity
  startTime: DS.attr('number'),

  // end time of the entity
  endTime: DS.attr('number'),

  duration: function () {
    return App.Helpers.date.duration(this.get('startTime'), this.get('endTime'))
  }.property('startTime', 'endTime'),

  entityType: App.EntityType.TASK_ATTEMPT,

  // container
  containerId: DS.attr('string'),
  nodeId: DS.attr('string'),

  // status of the task attempt
  status: DS.attr('string'),

  task: DS.belongsTo('task'),
  taskID: DS.attr('string'),
  vertexID: DS.attr('string'),
  dagID: DS.attr('string'),

  inProgressLog: DS.attr('string'),
  completedLog: DS.attr('string'),

  diagnostics: DS.attr('string'),

  counterGroups: DS.attr('array'),
});
App.DagTaskAttempt = App.TaskAttempt.extend({});
App.VertexTaskAttempt = App.TaskAttempt.extend({});
App.TaskTaskAttempt = App.TaskAttempt.extend({});
