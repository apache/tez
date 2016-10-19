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

import AbstractRoute from './abstract';
import Ember from 'ember';

export default AbstractRoute.extend({
  title: "Application",

  loaderQueryParams: {
    id: "app_id"
  },

  model: function (params) {
    var loader = this.get("loader"),
        appID = this.queryFromParams(params).id;
    return loader.queryRecord('AhsApp', appID).catch(function () {
      return loader.queryRecord('appRm', appID).catch(function () {
        // Sending back a dummy object presuming app details might be behind ACL.
        // Not throwing error bar at this level as we need to display DAG tab if
        // DAG details are available.
        return Ember.Object.create({
          entityID: appID
        });
      });
    });
  },

  actions: {
    setLoadTime: function (time) {
      this.set("controller.loadTime", time);
    }
  }
});
