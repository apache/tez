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

import ParentController from './parent';

export default ParentController.extend({
  breadcrumbs: Ember.computed("model.appID", "model.app.name", function () {
    var name = this.get("model.app.name") || this.get("model.appID");

    return [{
      text: `Application [ ${name} ]`,
      routeName: "app.index",
      model: this.get("model.appID")
    }];
  }),

  tabs: [{
    text: "Application Details",
    routeName: "app.index"
  }, {
    text: "DAGs",
    routeName: "app.dags"
  }, {
    text: "Configurations",
    routeName: "app.configs"
  }]
});
