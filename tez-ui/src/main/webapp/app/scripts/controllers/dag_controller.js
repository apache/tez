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
      ['dagProgress', 'dagInfo', 'vertexInfo'].forEach(function(itemType){
        that.store.unloadAll(itemType);
      });
      if (dag.get('status') === 'RUNNING') {
        // update the progress info if available. this need not block the UI
        if (dag.get('amWebServiceVersion') == 'v1') {
          that.updateInfoFromAM(dag);
        } else {
          // if AM version is v2 we keep updating the status, progress etc live.
          ["loading", "id", "model.status"].forEach(function(item) {
            Em.addObserver(that, item, that.startAMInfoUpdateService);
          });
          that.startAMInfoUpdateService();
        }
      }
    });

    return allLoaders;
  },

  updateAMDagInfo: function() {
    var dagId = this.get('id')
        that = this,
        dagInfoLoader = null;

    if (!dagId) return;

    if (this.store.recordIsLoaded("dagInfo", dagId)) {
      var dagInfoRecord = this.store.recordForId("dagInfo", dagId);
      if (dagInfoRecord.get('isLoading')) return;
      dagInfoLoader = dagInfoRecord.reload();
    } else {
      dagInfoLoader = this.store.find("dagInfo", dagId, {
        appId: that.get('applicationId'),
        dagIdx: that.get('idx')
      })
    }

    dagInfoLoader.then(function(dagInfo){
      that.set('amDagInfo', dagInfo);
      //TODO: find another way to trigger notification
      that.set('amDagInfo._amInfoLastUpdatedTime', moment());
    }).catch(function(e){
      // do nothing.
    });
  },

  updateAMVerticesInfo: function() {
    var dagId = this.get('id')
        that = this,
        verticesInfoLoader = null;

    if (!dagId) return;

    verticesInfoLoader = this.store.findQuery('vertexInfo', {
      metadata: {
        appID: that.get('applicationId'),
        dagID: that.get('idx'),
        counters: App.get('vertexCounters')
      }
    });

    verticesInfoLoader.then(function(verticesInfo) {
      that.set('amVertexInfo', verticesInfo);
    }).catch(function(e){
      // do nothing
    });

  },

  startAMInfoUpdateService: function() {
    if (this.get('loading') || !this.get('model.id') || this.get('model.status') != 'RUNNING') {
      return;
    }

    var amInfoUpdateService = this.get('amInfoUpdateService')
        that = this;

    if (Em.isNone(amInfoUpdateService)) {
      amInfoUpdateService = App.Helpers.Pollster.create({
        onPoll: function() {
          that.updateAMDagInfo();
          that.updateAMVerticesInfo();
        }
      });
      that.set('amInfoUpdateService', amInfoUpdateService);
      amInfoUpdateService.start(true);

      ["loading", "id", "model.status"].forEach(function(item) {
        Em.addObserver(that, item, that.stopAMInfoUpdateService);
      });
    }
    else {
      that.updateAMDagInfo();
      that.updateAMVerticesInfo();
    }
  },

  dostopAMInfoUpdateService: function() {
      var amInfoUpdateService = this.get('amInfoUpdateService');
      if (!Em.isNone(amInfoUpdateService)) {
        amInfoUpdateService.stop();
        this.set('amInfoUpdateService', undefined);
      }
  },

  // stop the update service if the status changes. see startAMInfoUpdateService
  stopAMInfoUpdateService: function() {
    if (this.get('loading') || this.get('model.status') != 'RUNNING') {
      this.dostopAMInfoUpdateService();
    }
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
