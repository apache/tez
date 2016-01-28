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
  breadcrumbs: Ember.computed("model.dag", function () {
    var dagName = this.get("model.dag.name"),
        vertexName = this.get("model.vertexName"),
        taskIndex = this.get("model.taskIndex"),
        attemptNo = this.get("model.index");

    return [{
      text: `DAG [ ${dagName} ]`,
      routeName: "dag.index",
      model: this.get("model.dagID")
    },{
      text: `Vertex [ ${vertexName} ]`,
      routeName: "vertex.index",
      model: this.get("model.vertexID")
    },{
      text: `Task [ ${taskIndex} ]`,
      routeName: "task.index",
      model: this.get("model.taskID")
    },{
      text: `Attempt [ ${attemptNo} ]`,
      routeName: "attempt.index",
      model: this.get("model.entityID")
    }];
  }),

  tabs: [{
    text: "Attempt Details",
    routeName: "attempt.index"
  },{
    text: "Attempt Counters",
    routeName: "attempt.counters"
  }]
});
