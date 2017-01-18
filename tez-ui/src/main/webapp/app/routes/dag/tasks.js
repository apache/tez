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

import Ember from 'ember';
import MultiAmPollsterRoute from '../multi-am-pollster';

import virtualAnchor from '../../utils/virtual-anchor';

export default MultiAmPollsterRoute.extend({
  title: "All Tasks",

  loaderNamespace: "dag",

  setupController: function (controller, model) {
    this._super(controller, model);
    Ember.run.later(this, "startCrumbBubble");
  },

  load: function (value, query, options) {
    return this.get("loader").query('task', {
      dagID: this.modelFor("dag").get("id")
    }, options);
  },

  actions: {
    logCellClicked: function (attemptID, download) {
      var that = this;
      return this.get("loader").queryRecord('attempt', attemptID).then(function (attempt) {
        var logURL = attempt.get("logURL");
        if(logURL) {
          return virtualAnchor(logURL, download ? attempt.get("entityID") : undefined);
        }
        else {
          that.send("openModal", {
            title: "Log Link Not Available!",
            content: `Log is missing for task attempt : ${attemptID}!`
          });
        }
      });
    }
  }
});
