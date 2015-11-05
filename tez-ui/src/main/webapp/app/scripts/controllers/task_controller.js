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

App.TaskController = App.PollingController.extend(App.Helpers.DisplayHelper, App.ModelRefreshMixin, {
  controllerName: 'TaskController',

  pageTitle: 'Task',

  loading: true,

  isActive: false,

  pollingType: 'taskInfo',

  setup: function () {
    this.set('isActive', true);
  },
  reset: function () {
    this.set('isActive', false);
  },

  pollsterControl: function () {
    if(this.get('vertex.dag.status') == 'RUNNING' &&
        this.get('vertex.dag.amWebServiceVersion') != '1' &&
        this.get('isActive')) {
      this.get('pollster').start();
    }
    else {
      this.get('pollster').stop();
    }
  }.observes('vertex.dag.status', 'vertex.dag.amWebServiceVersion', 'isActive'),

  pollsterOptionsObserver: function () {
    var model = this.get('model');

    this.get('pollster').setProperties( (model && model.get('status') != 'SUCCEEDED') ? {
      targetRecords: [model],
      options: {
        appID: this.get('vertex.dag.applicationId'),
        dagID: App.Helpers.misc.getIndexFromId(this.get('dagID')),
        taskID: '%@_%@'.fmt(
          App.Helpers.misc.getIndexFromId(this.get('vertexID')),
          App.Helpers.misc.getIndexFromId(this.get('id'))
        )
      }
    } : {
      targetRecords: [],
      options: null
    });
  }.observes('vertex.dag.applicationId', 'status', 'dagID', 'vertexID', 'id'),

  loadAdditional: function(task) {
    var that = this;
    var applicationId = App.Helpers.misc.getAppIdFromVertexId(task.get('vertexID'));

    var dagLoader = this.store.find('dag', task.get('dagID'));
    var vertexLoader = this.store.find('vertex', task.get('vertexID'));
    var tezAppLoader = this.store.find('tezApp', 'tez_' + applicationId);

    task.set('progress', undefined);
    var allLoaders = Em.RSVP.hash({
      dag: dagLoader,
      vertex: vertexLoader,
      tezApp: tezAppLoader
    });

    allLoaders.then(function(results) {
      task.set('vertex', results.vertex);
      task.set('vertex.dag', results.dag);
      task.set('tezApp', results.tezApp);
    }).finally(function() {
      that.set('loading', false);
    });

    return allLoaders;
  },

  vertexName: function() {
    return this.get('vertex.name') || this.get('vertexID');
  }.property('vertex.name', 'vertexID'),

  dagName: function() {
    return this.get('vertex.dag.name') || this.get('dagID');
  }.property('vertex.dag.name', 'dagID'),

  childDisplayViews: [
    Ember.Object.create({title: 'Task Details', linkTo: 'task.index'}),
    Ember.Object.create({title: 'Task Counters', linkTo: 'task.counters'}),
    Ember.Object.create({title: 'Task Attempts', linkTo: 'task.attempts'}),
  ],

});
