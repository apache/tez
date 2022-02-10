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

import ParentController from './parent';

export default ParentController.extend({
  breadcrumbs: computed('model.{entityID,name}', function () {
    var name = this.get("model.name");

    return [{
      text: `DAG [ ${name} ]`,
      routeName: "dag.index.index",
      model: this.get("model.entityID")
    }];
  }),

  tabs: computed(function() {return [{
    text: "DAG Details",
    routeName: "dag.index.index"
  }, {
    text: "DAG Counters",
    routeName: "dag.counters"
  }, {
    text: "Graphical View",
    routeName: "dag.graphical"
  }, {
    text: "All Vertices",
    routeName: "dag.vertices"
  }, {
    text: "All Tasks",
    routeName: "dag.tasks"
  }, {
    text: "All Task Attempts",
    routeName: "dag.attempts"
  }, {
    text: "Vertex Swimlane",
    routeName: "dag.swimlane"
  }]})
});
