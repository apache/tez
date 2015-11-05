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

App.TezAppController = App.BaseController.extend(App.Helpers.DisplayHelper, App.ModelRefreshMixin, {
  controllerName: 'AppController',

  pageTitle: 'App',

  loading: true,

  updateLoading: function() {
    this.set('loading', false);
  }.observes('content'),

  pollster: App.Helpers.Pollster.create(),

  init: function () {
    this._super();
    this.get('pollster').setProperties({
      onPoll: this.load.bind(this)
    });
  },

  pollsterControl: function () {
    if(this.get('appDetail.finalAppStatus') == 'UNDEFINED' &&
        this.get('pollingEnabled') &&
        this.get('isActive')) {
      this.get('pollster').start();
    }
    else {
      this.get('pollster').stop();
    }
  }.observes('appDetail.finalAppStatus', 'isActive', 'pollingEnabled'),

  load: function () {
    var tezApp = this.get('content'),
        store  = this.get('store');

      tezApp.reload().then(function (tezApp) {
        var appId = tezApp.get('appId');
        if(!appId) return tezApp;
        App.Helpers.misc.removeRecord(store, 'appDetail', appId);
        return store.find('appDetail', appId).then(function (appDetails){
          tezApp.set('appDetail', appDetails);
          return tezApp;
        });
      }).catch(function (error) {
        Em.Logger.error(error);
        var err = App.Helpers.misc.formatError(error, defaultErrMsg);
        var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
        App.Helpers.ErrorBar.getInstance().show(msg, err.details);
      });
  },

  childDisplayViews: [
    Ember.Object.create({title: 'App Details', linkTo: 'tez-app.index'}),
    Ember.Object.create({title: 'DAGs', linkTo: 'tez-app.dags'}),
    Ember.Object.create({title: 'App Configuration', linkTo: 'tez-app.configs'}),
  ],
});
