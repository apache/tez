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

App.DagController = Em.ObjectController.extend(App.Helpers.DisplayHelper, {
  controllerName: 'DagController',
  pageTitle: 'Dag',
  loading: true,

  loadAdditional: function(dag) {
    var that = this;
    var loaders = [];
    var applicationId = dag.get('applicationId');

    if (dag.get('status') === 'RUNNING') {
      // update the progress info if available. this need not block the UI
      App.Helpers.misc.removeRecord(this.store, 'dagProgress', dag.get('id'));
      var aminfoLoader = that.store.find('dagProgress', dag.get('id'), {
        appId: applicationId,
        dagIdx: dag.get('idx')
      }).then(function(dagProgressInfo) {
        dag.set('progress', dagProgressInfo.get('progress'));
      }).catch(function (error) {
        Em.Logger.error("Failed to fetch dagProgress")
      });
      loaders.push(aminfoLoader);
    }
    App.Helpers.misc.removeRecord(this.store, 'appDetail', applicationId);
    var appDetailLoader = this.store.find('appDetail', applicationId)
      .then(function(app){
        dag.set('appDetail', app);
        var appState = app.get('appState');
        if (appState) {
          dag.set('yarnAppState', appState);
        }
        dag.set('status', App.Helpers.misc.getRealStatus(dag.get('status'), app.get('appState'), app.get('finalAppStatus')));
      }).catch(function(){});
    App.Helpers.misc.removeRecord(this.store, 'tezApp', 'tez_' + applicationId);
    var tezAppLoader = this.store.find('tezApp', 'tez_' + applicationId)
      .then(function(app){
        dag.set('tezApp', app);
      }).catch(function(){});

    loaders.push(appDetailLoader);

    Em.RSVP.allSettled(loaders).then(function(){
      that.set('loading', false);
    });

    return Em.RSVP.all(loaders);
  },

  enableAppIdLink: function() {
    return !!(this.get('tezApp') && this.get('appDetail'));
  }.property('applicationId', 'appDetail', 'tezApp'),

  childDisplayViews: [
    Ember.Object.create({title: 'Details', linkTo: 'dag.index'}),
    Ember.Object.create({title: 'View', linkTo: 'dag.view'}),
    Ember.Object.create({title: 'Vertices', linkTo: 'dag.vertices'}),
    Ember.Object.create({title: 'Tasks', linkTo: 'dag.tasks'}),
    Ember.Object.create({title: 'Task Attempts', linkTo: 'dag.taskAttempts'}),
    Ember.Object.create({title: 'Counters', linkTo: 'dag.counters'}),
    Ember.Object.create({title: 'Swimlane', linkTo: 'dag.swimlane'})
  ],

});
