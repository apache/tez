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

import { observer } from '@ember/object';
import { assign } from '@ember/polyfills';
import MultiAmPollsterRoute from '../multi-am-pollster';

export default MultiAmPollsterRoute.extend({
  get title() {
    var dag = this.modelFor("dag"),
      name = dag.get("name"),
      entityID = dag.get("entityID");
    return `Graphical View: ${name} (${entityID})`;
  },

  loaderNamespace: "dag",

  setupController: function () {
    this._super(...arguments);
    this.startCrumbBubble();
  },

  load: function (value, query, options) {
    options = assign({
      demandNeeds: ["info", "dag"]
    }, options);
    return this.loader.query('vertex', {
      dagID: this.modelFor("dag").get("id")
    }, options);
  },

  _loadedValueObserver: observer("loadedValue", function () {
    var loadedValue = this.loadedValue,
        records = [];

    if(loadedValue) {
      loadedValue.forEach(function (record) {
        records.push(record);
      });

      this.set("polledRecords", records);
    }
  }),
});
