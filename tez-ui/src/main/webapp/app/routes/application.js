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

export default Ember.Route.extend({
  title: "Application",

  pageReset: function () {
    this.send("resetTooltip");
  },

  actions: {
    didTransition: function(/* transition */) {
      this.pageReset();
    },
    bubbleBreadcrumbs: function (breadcrumbs) {
      this.set("controller.breadcrumbs", breadcrumbs);
    },

    error: function (error) {
      this.set("controller.appError", error);
      Ember.Logger.error(error);
    },

    resetTooltip: function () {
      Ember.$(document).tooltip("destroy");
      Ember.$(document).tooltip({
        show: {
          delay: 500
        },
        tooltipClass: 'generic-tooltip'
      });
    },

    // Modal window actions
    openModal: function (componentName, options) {
      options = options || {};

      if(typeof componentName === "object") {
        options = componentName;
        componentName = null;
      }

      this.render(options.modalName || "simple-modal", {
        into: 'application',
        outlet: 'modal',
        model: {
          title: options.title,
          componentName: componentName,
          content: options.content,
          targetObject: options.targetObject
        }
      });
      Ember.run.later(function () {
        Ember.$(".simple-modal").modal();
      });
    },
    closeModal: function () {
      Ember.$(".simple-modal").modal("hide");
    },
    destroyModal: function () {
      Ember.run.later(this, function () {
        this.disconnectOutlet({
          outlet: 'modal',
          parentView: 'application'
        });
      });
    }
  }
});
