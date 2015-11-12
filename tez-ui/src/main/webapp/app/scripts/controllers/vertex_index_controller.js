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

App.VertexIndexController = App.PollingController.extend(App.ModelRefreshMixin, {
  controllerName: 'VertexIndexController',

  needs: 'vertex',

  load: function () {
    var vertex = this.get('controllers.vertex.model'),
        controller = this.get('controllers.vertex'),
        t = this;
    vertex.reload().then(function () {
      return controller.loadAdditional(vertex);
    }).catch(function(error){
      Em.Logger.error(error);
      var err = App.Helpers.misc.formatError(error, defaultErrMsg);
      var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
      App.Helpers.ErrorBar.getInstance().show(msg, err.details);
    });
  },

  //TODO: TEZ-1705 : Create a parent class and move this function there to avoid duplication.
  iconStatus: function() {
    return App.Helpers.misc.getStatusClassForEntity(this.get('model.status'),
      this.get('model.hasFailedTaskAttempts'));
  }.property('id', 'model.status', 'model.hasFailedTaskAttempts'),

  progressStr: function() {
    var pct;
    if (Ember.typeOf(this.get('progress')) === 'number') {
      pct = App.Helpers.number.fractionToPercentage(this.get('progress'));
    }
    return pct;
  }.property('id', 'status', 'progress', 'model.status'),

  hasFailedTasks: function() {
    return this.get('failedTasks') > 0;
  }.property('failedTasks'),

  failedTasksLink: function() {
    return '#/vertex/%@/tasks?searchText=Status%3AFAILED'.fmt(this.get('id'));
  }.property('id'),

  failedTaskAttemptsLink: function() {
    return '#/vertex/%@/taskAttempts?searchText=Status%3AFAILED'.fmt(this.get('id'));
  }.property('id'),

  hasFirstTaskStarted: function() {
    return !!this.get('firstTaskStartTime') && !!this.get('firstTasksToStart');
  }.property('firstTaskStartTime', 'firstTasksToStart'),

  hasLastTaskFinished: function() {
    return !!this.get('lastTaskFinishTime') && !!this.get('lastTasksToFinish');
  }.property('lastTaskFinishTime', 'lastTasksToFinish'),

  hasStats: function() {
    return (this.get('numTasks') || 0) > 0 ||
           (this.get('sucessfulTasks') || 0) > 0 ||
           (this.get('failedTasks') || 0 ) > 0 ||
           (this.get('killedTasks') || 0) > 0 ||
           this.get('showAvgTaskDuration') ||
           this.get('showMinTaskDuration') ||
           this.get('showMaxTaskDuration');
  }.property('numTasks', 'sucessfulTasks', 'failedTasks', 'killedTasks', 'showAvgTaskDuration',
    'showMinTaskDuration', 'showMaxTaskDuration'),

  showAvgTaskDuration: function() {
    return (this.get('avgTaskDuration') || 0) > 0;
  }.property('avgTaskDuration'),

  showMinTaskDuration: function() {
    return (this.get('minTaskDuration') || 0) > 0;
  }.property('minTaskDuration'),

  showMaxTaskDuration: function() {
    return (this.get('maxTaskDuration') || 0) > 0;
  }.property('maxTaskDuration'),
});