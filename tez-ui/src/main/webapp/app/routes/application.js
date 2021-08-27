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

import { action } from '@ember/object';
import Route from '@ember/routing/route';
import { later } from '@ember/runloop';

export default Route.extend({
  title: "Application",

  bubbleBreadcrumbs: action(function (breadcrumbs) {
    this.set("controller.breadcrumbs", breadcrumbs);
  }),

  error: action(function (error) {
    console.error(error);
    this.set("controller.appError", error);
  }),

  // Modal window actions
  openModal: action(function (componentName, options) {
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
    later(function() {
      // new render isn't available until next run loop
      var modal = document.querySelector('.simple-modal');
      modal.style.display="block";
    });
  }),
  closeModal: action(function () {
    var modal = document.querySelector('.simple-modal');
    if (modal) {
      modal.style.display="none";
    }
  }),
  destroyModal: action(function () {
    later(this, function () {
      this.disconnectOutlet({
        outlet: 'modal',
        parentView: 'application'
      });
    });
  })
});
