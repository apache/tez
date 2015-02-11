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

App.VertexController = Em.ObjectController.extend(App.Helpers.DisplayHelper, {
  controllerName: 'VertexController',

  pageTitle: 'Vertex',

  loading: true,

  updateLoading: function() {
    var loaders = [],
      that = this,
      applicationId = this.get('applicationId');

    if (this.get('status') == 'RUNNING') {
      var vertexIdx = that.get('id').split('_').splice(-1).pop();
      var progressLoader = this.store.find('vertexProgress', vertexIdx, {
        appId: applicationId,
        dagIdx: that.get('dagIdx')
      }).then(function(vertexProgressInfo) {
        if (vertexProgressInfo) {
          that.set('progress', vertexProgressInfo.get('progress'));
        }
      }).catch(function(error) {
        Em.Logger.error("Failed to fetch vertexProgress" + error)
      });
      loaders.push(progressLoader);
    }

    var appDetailFetcher = that.store.find('appDetail', applicationId).then(function(appDetail) {
      var appState = appDetail.get('appState');
      if (appState) {
        that.set('yarnAppState', appState);
      }
    });
    loaders.push(appDetailFetcher);
    Em.RSVP.allSettled(loaders).then(function(){
      that.set('loading', false);
    });
  }.observes('content'),

  childDisplayViews: [
    Ember.Object.create({title: 'Details', linkTo: 'vertex.index'}),
    Ember.Object.create({title: 'Tasks', linkTo: 'vertex.tasks'}),
    Ember.Object.create({title: 'Task Attempts', linkTo: 'vertex.taskAttempts'}),
    Ember.Object.create({title: 'Counters', linkTo: 'vertex.counters'}),
    Ember.Object.create({title: 'Swimlane', linkTo: 'vertex.swimlane'}),
    Ember.Object.create({title: 'Sources & Sinks', linkTo: 'vertex.additionals'}),
  ],
});
