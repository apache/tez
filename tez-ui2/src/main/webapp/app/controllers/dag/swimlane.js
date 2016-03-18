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

import MultiTableController from '../multi-table';
import ColumnDefinition from 'em-table/utils/column-definition';
import VertexProcess from '../../utils/vertex-process';

import fullscreen from 'em-tgraph/utils/fullscreen';

export default MultiTableController.extend({
  breadcrumbs: [{
    text: "Vertex Swimlane",
    routeName: "dag.swimlane",
  }],

  columns: ColumnDefinition.make([]),

  actions: {
    toggleFullscreen: function () {
      var swimlaneElement = Ember.$(".swimlane-page")[0];
      if(swimlaneElement){
        fullscreen.toggle(swimlaneElement);
      }
    }
  },

  processes: Ember.computed("model", function () {
    var processes = [],
        processHash = {},

        dagPlanEdges = this.get("model.firstObject.dag.edges");

    // Create process instances for each vertices
    this.get("model").forEach(function (vertex) {
      var process = VertexProcess.create({
        vertex: vertex,
        blockers: Ember.A()
      });
      processHash[vertex.get("name")] = process;
      processes.push(process);
    });

    // Add process(vertex) dependencies based on dagPlan
    dagPlanEdges.forEach(function (edge) {
      var process = processHash[edge.outputVertexName];
      if(process) {
        process.blockers.push(processHash[edge.inputVertexName]);
      }
    });

    return Ember.A(processes);
  }),

  eventBars: [{
    fromEvent: "VERTEX_TASK_START",
    toEvent: "VERTEX_TASK_FINISH",
  }, {
    fromEvent: "BLOCKING_VERTICES_COMPLETE",
    toEvent: "VERTEX_TASK_FINISH",
  }]
});
