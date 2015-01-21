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

App.DagSwimlaneController = Em.ArrayController.extend({
  needs: "dag",
  controllerName: "DagSwimlaneController",
  pageTitle: "Task Attempts",
  pageSubTitle: "All Task Attempts",
  dag_id: Em.computed.alias('controllers.dag.id'),

  getFilterParams: function(params) {
    var dag_id = this.get('dag_id');
    var filterParams = {};
    if (dag_id) {
      filterParams['primaryFilter'] = 'TEZ_DAG_ID:' + dag_id;
    }

    return filterParams;
  },

  actions: {
    taskAttemptClicked: function (id) {
      this.transitionToRoute('taskAttempt', id);
    },
  },
});
