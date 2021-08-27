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

import EmberObject from '@ember/object';
import SingleAmPollsterRoute from '../single-am-pollster';

import Model from '@ember-data/model';

export default SingleAmPollsterRoute.extend({
  get title() {
    var app = this.modelFor("app"),
      entityID = app.get("entityID");
    return `Application: ${entityID}`;
  },

  loaderNamespace: "app",

  setupController: function () {
    this._super(...arguments);
    this.startCrumbBubble();
  },

  onRecordPoll: function () {
    this.reload();
  },

  load: function (value, query, options) {
    var appModel = this.modelFor("app"),
      // this.get below is needed for loader injection during testing
      loader = this.get("loader"),
      appID = appModel.get("entityID");

    // If it's a dummy object then reset, we have already taken appID from it
    if(!(appModel instanceof Model)) {
      appModel = null;
    }

    return loader.queryRecord('app', "tez_" + appID, options).catch(function (appErr) {
      return loader.query('dag', {
        appID: appID,
        limit: 1
      }, options).then(function (dags) {
        // If DAG details or application history is available for the app, then don't throw error
        if(dags.get("length") || appModel) {
          return EmberObject.create({
            app: appModel,
            appID: appID
          });
        }
        throw(appErr);
      });
    });
  }
});
