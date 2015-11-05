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

App.DagController = App.PollingController.extend(App.Helpers.DisplayHelper, {
  controllerName: 'DagController',
  pageTitle: 'Dag',

  loading: true,
  isActive: false,

  pollingType: 'dagInfo',

  setup: function () {
    this.set('isActive', true);
  },
  reset: function () {
    this.set('isActive', false);
  },

  pollsterControl: function () {
    if(this.get('status') == 'RUNNING' &&
        this.get('amWebServiceVersion') != '1' &&
        this.get('isActive')) {
      this.get('pollster').start();
    }
    else {
      this.get('pollster').stop();
    }
  }.observes('status', 'amWebServiceVersion', 'isActive'),

  pollsterOptionsObserver: function () {
    var model = this.get('model');

    this.get('pollster').setProperties( (model && model.get('status') != 'SUCCEEDED') ? {
      targetRecords: [model],
      options: {
        appID: this.get('applicationId'),
        dagID: App.Helpers.misc.getIndexFromId(this.get('id')),
      }
    } : {
      targetRecords: [],
      options: null
    });
  }.observes('applicationId', 'model', 'model.status', 'id'),

  loadAdditional: function(dag) {
    var that = this;
    var loaders = [];
    var applicationId = dag.get('applicationId');

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
    loaders.push(tezAppLoader);

    Em.RSVP.allSettled(loaders).then(function(){
      that.set('loading', false);
    });

    if (!dag.get('appContextInfo.info') && App.get('env.compatibilityMode')) {
      var dagName = dag.getWithDefault('name', '');
      var hiveQueryId = dagName.replace(/([^:]*):.*/, '$1');
      if (dagName !=  hiveQueryId && !!hiveQueryId) {
        this.store.find('hiveQuery', hiveQueryId).then(function (hiveQueryData) {
          var queryInfoStr = Em.get(hiveQueryData || {}, 'query') || '{}';
          var queryInfo = $.parseJSON(queryInfoStr);
          dag.set('appContextInfo', {
            appType: 'Hive',
            info: queryInfo['queryText']
          });
        }).catch(function (e) {
          // ignore.
        });
      }
    }

    var allLoaders = Em.RSVP.all(loaders);
    allLoaders.then(function(){
      if (dag.get('status') === 'RUNNING') {
        // update the progress info if available. this need not block the UI
        if (dag.get('amWebServiceVersion') == '1') {
          that.updateInfoFromAM(dag);
        }
      }
    });

    return allLoaders;
  },

  // called only for v1 version of am api.
  updateInfoFromAM: function(dag) {
    var that = this;
    App.Helpers.misc.removeRecord(this.get('store'), 'dagProgress', dag.get('id'));
    var aminfoLoader = this.store.find('dagProgress', dag.get('id'), {
      appId: dag.get('applicationId'),
      dagIdx: dag.get('idx')
    }).then(function(dagProgressInfo) {
      that.set('dag.progress', dagProgressInfo.get('progress'));
    }).catch(function (error) {
      Em.Logger.error("Failed to fetch dagProgress" + e);
    });
  },

  enableAppIdLink: function() {
    return !!this.get('tezApp');
  }.property('applicationId', 'tezApp'),

  childDisplayViews: [
    Ember.Object.create({title: 'DAG Details', linkTo: 'dag.index'}),
    Ember.Object.create({title: 'DAG Counters', linkTo: 'dag.counters'}),
    Ember.Object.create({title: 'Graphical View', linkTo: 'dag.view'}),
    Ember.Object.create({title: 'All Vertices', linkTo: 'dag.vertices'}),
    Ember.Object.create({title: 'All Tasks', linkTo: 'dag.tasks'}),
    Ember.Object.create({title: 'All TaskAttempts', linkTo: 'dag.taskAttempts'}),
    Ember.Object.create({title: 'Swimlane', linkTo: 'dag.swimlane'})
  ],

});
