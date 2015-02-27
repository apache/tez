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

App.TaskController = Em.ObjectController.extend(App.Helpers.DisplayHelper, App.ModelRefreshMixin, {
  controllerName: 'TaskController',

  pageTitle: 'Task',

  loading: true,

  loadAdditional: function(task) {
    var that = this;

    var dagLoader = this.store.find('dag', task.get('dagID'));
    var vertexLoader = this.store.find('vertex', task.get('vertexID'));

    var allLoaders = Em.RSVP.hash({
      dag: dagLoader,
      vertex: vertexLoader
    });

    allLoaders.then(function(results) {
      task.set('vertex', results.vertex);
      task.set('vertex.dag', results.dag);
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
