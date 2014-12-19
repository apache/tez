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

App.TezAppController = Em.ObjectController.extend(App.Helpers.DisplayHelper, {
  controllerName: 'AppController',

  pageTitle: 'App',

  loading: true,

  rmTrackingURL: function() {
    return App.env.RMWebUrl + '/cluster/app/' + this.get('appId');
  }.property('appId'),

  updateLoading: function() {
    this.set('loading', false);
  }.observes('content'),

  childDisplayViews: [
    Ember.Object.create({title: 'Details', linkTo: 'tez-app.index'}),
    Ember.Object.create({title: 'Dags', linkTo: 'tez-app.dags'}),
    Ember.Object.create({title: 'Configuration', linkTo: 'tez-app.configs'}),
  ],
});