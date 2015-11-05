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

App.TaskAttemptController = App.BaseController.extend(App.Helpers.DisplayHelper, {
  controllerName: 'TaskAttemptController',

  pageTitle: 'TaskAttempt',

  loading: true,

  pollster: App.Helpers.EntityArrayPollster.create(),

  init: function () {
    this._super();
    this.get('pollster').setProperties({
      entityType: 'attemptInfo',
      mergeProperties: ['status', 'progress'],
      store: this.get('store')
    });
  },

  pollsterControl: function () {
    if(this.get('task.vertex.dag.status') == 'RUNNING' &&
        this.get('task.vertex.dag.amWebServiceVersion') != '1' &&
        this.get('pollingEnabled') &&
        this.get('isActive')) {
      this.get('pollster').start();
    }
    else {
      this.get('pollster').stop();
    }
  }.observes('task.vertex.dag.status', 'task.vertex.dag.amWebServiceVersion', 'isActive', 'pollingEnabled'),

  pollsterOptionsObserver: function () {
    var model = this.get('model');

    this.get('pollster').setProperties( (model && model.get('status') != 'SUCCEEDED') ? {
      targetRecords: [model],
      options: {
        appID: this.get('task.vertex.dag.applicationId'),
        dagID: App.Helpers.misc.getIndexFromId(this.get('dagID')),
        attemptID: '%@_%@_%@'.fmt(
          App.Helpers.misc.getIndexFromId(this.get('vertexID')),
          App.Helpers.misc.getIndexFromId(this.get('taskID')),
          App.Helpers.misc.getIndexFromId(this.get('id'))
        )
      }
    } : {
      targetRecords: [],
      options: null
    });
  }.observes('task.vertex.dag.applicationId', 'status', 'dagID', 'vertexID', 'id'),

  loadAdditional: function(attempt) {
    var that = this;

    var dagLoader = this.store.find('dag', attempt.get('dagID'));
    var vertexLoader = this.store.find('vertex', attempt.get('vertexID'));
    var taskLoader = this.store.find('task', attempt.get('taskID'));

    var allLoaders = Em.RSVP.hash({
      dag: dagLoader,
      vertex: vertexLoader,
      task: taskLoader
    });
    allLoaders.then(function(results) {
      attempt.set('task', results.task);
      attempt.set('task.vertex', results.vertex);
      attempt.set('task.vertex.dag', results.dag);
    }).finally(function() {
      that.set('loading', false);
    });

    return allLoaders;
  },

  taskIndex: function() {
    return App.Helpers.misc.getTaskIndex(this.get('dagID'), this.get('taskID'));
  }.property('taskID', 'dagID'),

  vertexName: function() {
    return this.get('task.vertex.name') || this.get('vertexID');
  }.property('task.vertex.name', 'vertexID'),

  dagName: function() {
    return this.get('task.vertex.dag.name') || this.get('dagID');
  }.property('task.vertex.dag.name', 'dagID'),

  childDisplayViews: [
    Ember.Object.create({title: 'TaskAttempt Details', linkTo: 'taskAttempt.index'}),
    Ember.Object.create({title: 'TaskAttempt Counters', linkTo: 'taskAttempt.counters'}),
  ],

});
