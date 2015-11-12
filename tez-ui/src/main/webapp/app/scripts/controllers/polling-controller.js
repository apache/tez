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

var DEFAULT_MERGE_PROPS = ['status', 'progress'];

App.PollingController = App.BaseController.extend({

  pollster: null,
  pollingEnabled: null,
  showAutoUpdate: true,

  persistConfigs: true,

  pollingType: null,
  pollingOptions: null,

  init: function () {
    var pollingEnabled;

    this._super();
    this.set('pollster', App.Helpers.EntityArrayPollster.create({
      store: this.get('store'),

      mergeProperties: DEFAULT_MERGE_PROPS,
      entityType: this.get('pollingType'),
      options: this.get('pollingOptions'),

      onFailure: this.onPollingFailure.bind(this)
    }));

    if(this.get('persistConfigs')) {
      pollingEnabled = this.fetchConfig('pollingEnabled');
      if(pollingEnabled == undefined) {
        pollingEnabled = true;
      }
      Ember.run.later(this, this.set, 'pollingEnabled', pollingEnabled, 100);
    }
  },

  setup: function () {
    this._super();
    Ember.run.later(this, this.send, 'pollingEnabledChanged', this.get('pollingEnabled'));
  },

  pollingEnabledObserver: function () {
    var pollingEnabled = this.get('pollingEnabled');

    if(this.get('persistConfigs')) {
      this.storeConfig('pollingEnabled', pollingEnabled);
    }

    this.send('pollingEnabledChanged', pollingEnabled);

    if(!pollingEnabled && this.get('pollster.isRunning')) {
      this.get('pollster').stop();
      this.set('pollster.polledRecords', null);
      this.applicationComplete();
    }
  }.observes('pollingEnabled'),

  onPollingFailure: function (error) {
    var appID = this.get('pollster.options.appID'),
        that = this;

    App.Helpers.misc.removeRecord(this.get('store'), 'clusterApp', appID);
    this.get('store').find('clusterApp', appID).then(function (app) {
      if(app.get('isComplete')) {
        that.get('pollster').stop();
        that.applicationComplete();
      }
      else {
        error.message = "Application Master (AM) is out of reach. Either it's down, or CORS is not enabled.";
        that.applicationFailed(error);
      }
    }).catch(function (error) {
      that.get('pollster').stop();
      error.message = "Resource Manager (RM) is out of reach. Either it's down, or CORS is not enabled.";
      that.applicationFailed(error);
    });
  },

  applicationComplete: function () {
    this.get('pollster').stop();
    this.set('pollster.polledRecords', null);
    if(this.load) {
      this.load();
    }
  },

  applicationFailed: function (error) {
    // TODO: TEZ-2877 - #1
    Em.Logger.error(error);
    var err = App.Helpers.misc.formatError(error, error.message);
    var msg = 'Error code: %@, message: %@'.fmt(err.errCode, err.msg);
    App.Helpers.ErrorBar.getInstance().show(msg, err.details);
  }

});
