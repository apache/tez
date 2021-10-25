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

import SingleAmPollsterRoute from '../single-am-pollster';

export default SingleAmPollsterRoute.extend({
  get title() {
    var vertex = this.modelFor("vertex"),
      name = vertex.get("name"),
      entityID = vertex.get("entityID");
    return `Vertex Counters: ${name} (${entityID})`;
  },

  loaderNamespace: "vertex",

  setupController: function () {
    this._super(...arguments);
    this.startCrumbBubble();
  },

  load: function (value, query, options) {
    return this.loader.queryRecord('vertex', this.modelFor("vertex").get("id"), options);
  }
});
