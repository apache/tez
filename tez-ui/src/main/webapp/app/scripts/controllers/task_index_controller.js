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

 //TODO: watch individual counters.
App.TaskIndexController = App.PollingController.extend(App.ModelRefreshMixin, {
  controllerName: 'TaskIndexController',

  needs: "task",

  taskStatus: function() {
    return App.Helpers.misc.getFixedupDisplayStatus(this.get('model.status'));
  }.property('id', 'model.status'),

  taskIconStatus: function() {
    return App.Helpers.misc.getStatusClassForEntity(this.get('taskStatus'),
      this.get('hasFailedTaskAttempts'));
  }.property('id', 'taskStatus', 'hasFailedTaskAttempts'),

  load: function () {
    var model = this.get('content');
    if(model && $.isFunction(model.reload)) {
      model.reload().then(function(record) {
        if(record.get('isDirty')) {
          record.rollback();
        }
      });
    }
  },

  logsLink: null,

  logsLinkObserver: function() {

    var model = this.get('content');
    var taskAttemptId = model.get('successfulAttemptId') || model.get('attempts.lastObject');
    var store = this.get('store');
    var that = this;

    if (taskAttemptId) {
      store.find('taskAttempt', taskAttemptId).then(function(attempt) {
          var cellContent = App.Helpers.misc.constructLogLinks(
              attempt,
              that.get('controllers.task.appDetail.status'),
              that.get('controllers.task.tezApp.user')
              );

          cellContent.notAvailable = cellContent.viewUrl || cellContent.downloadUrl;
          that.set('logsLink', cellContent);
        });
    }
  }.observes('id', 'successfulAttemptId'),

});
