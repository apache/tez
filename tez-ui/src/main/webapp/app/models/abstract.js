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

import { computed } from '@ember/object';
import Model, { attr } from '@ember-data/model';

export default Model.extend({
  loadTime: null,

  mergedProperties: ["needs"],

  refreshLoadTime: function () {
    this.set('loadTime', new Date());
  },

  //TODO - Find a better alternative to detect property change in a model
  _notifyProperties: function (keys) {
    this.refreshLoadTime();
    return this._super(keys);
  },

  dataReady: computed("isLoaded", function () {
    return this.refreshLoadTime();
  }),

  entityID: attr("string"),

  index: computed("entityID", function () {
    var id = this.entityID || "";
    return id.substr(id.lastIndexOf('_') + 1);
  }),

  status: attr("string"),
  isComplete: computed("status", function () {
    switch(this.status) {
      case "SUCCEEDED":
      case "FINISHED":
      case "FAILED":
      case "KILLED":
      case "ERROR":
        return true;
    }
    return false;
  }),

});
