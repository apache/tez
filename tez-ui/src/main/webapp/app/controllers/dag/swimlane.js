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

import { A } from '@ember/array';
import { action, computed } from '@ember/object';

import MultiTableController from '../multi-table';
import ColumnDefinition from '../../utils/column-definition';
import VertexProcess from '../../utils/vertex-process';

import fullscreen from '../../utils/fullscreen';

export default MultiTableController.extend({

  zoom: 100,

  columnSelectorTitle: 'Customize vertex tooltip',

  breadcrumbs: computed(function() {return [{
    text: "Vertex Swimlane",
    routeName: "dag.swimlane",
  }]}),

  columns: ColumnDefinition.make([{
    id: 'entityID',
    headerTitle: 'Vertex Id',
    contentPath: 'entityID'
  },{
    id: 'status',
    headerTitle: 'Status',
    contentPath: 'finalStatus',
  },{
    id: 'progress',
    headerTitle: 'Progress',
    contentPath: 'progress',
    cellDefinition: {
      type: 'number',
      format: '0%'
    }
  },{
    id: 'startTime',
    headerTitle: 'Start Time',
    contentPath: 'startTime',
    cellComponentName: 'date-formatter',
  },{
    id: 'endTime',
    headerTitle: 'End Time',
    contentPath: 'endTime',
    cellComponentName: 'date-formatter',
  },{
    id: 'duration',
    headerTitle: 'Duration',
    contentPath: 'duration',
    cellDefinition: {
      type: 'duration'
    }
  },{
    id: 'description',
    headerTitle: 'Description',
    contentPath: 'description',
  },{
    id: 'firstTaskStartTime',
    headerTitle: 'First Task Start Time',
    contentPath: 'firstTaskStartTime',
    cellComponentName: 'date-formatter',
  },{
    id: 'totalTasks',
    headerTitle: 'Tasks',
    contentPath: 'totalTasks',
  },{
    id: 'succeededTasks',
    headerTitle: 'Succeeded Tasks',
    contentPath: 'succeededTasks',
  },{
    id: 'runningTasks',
    headerTitle: 'Running Tasks',
    contentPath: 'runningTasks',
  },{
    id: 'pendingTasks',
    headerTitle: 'Pending Tasks',
    contentPath: 'pendingTasks',
  },{
    id: 'processorClassName',
    headerTitle: 'Processor Class',
    contentPath: 'processorClassName',
  }]),

  dataAvailable: computed("model.firstObject.dag.amWsVersion",
      "model.firstObject.dag.isComplete",
      "model.firstObject.am.initTime", function () {
    var vertex = this.get("model.firstObject"),
        dag = this.get("model.firstObject.dag"),
        dataAvailable = true;

    if(vertex && dag && !dag.get("isComplete")) {
      let amWsVersion = dag.get("amWsVersion");
      // amWsVersion = undefined or 1
      if(!amWsVersion || amWsVersion === 1) {
        dataAvailable = false;
      }
      // amWsVersion >= 2, but without event/time data
      if(vertex.get("am") && !vertex.get("am.initTime")) {
        dataAvailable = false;
      }
    }

    return dataAvailable;
  }),

  processes: computed('model.firstObject.dag.edges', function () {
    var processes = [],
        processHash = {},

        dagPlanEdges = this.get("model.firstObject.dag.edges"),

        that = this,
        getVisibleProps = function () {
          return that.get("visibleColumns");
        };

    // Create process instances for each vertices
    this.model.forEach(function (vertex) {
      var process = VertexProcess.create({
        vertex: vertex,
        getVisibleProps: getVisibleProps,
        blockers: A()
      });
      processHash[vertex.get("name")] = process;
      processes.push(process);
    });

    // Add process(vertex) dependencies based on dagPlan
    if(dagPlanEdges) {
      dagPlanEdges.forEach(function (edge) {
        var process = processHash[edge.outputVertexName];
        if(process && processHash[edge.inputVertexName]) {
          process.blockers.push(processHash[edge.inputVertexName]);
          process.edgeHash.set(edge.inputVertexName, edge);
        }
      });
    }

    return A(processes);
  }),

  toggleFullscreen: action(function () {
    var swimlaneElement = document.querySelector('.swimlane-page');
    if(swimlaneElement){
      fullscreen.toggle(swimlaneElement);
    }
  }),

  routeToVertex: action(function (entityID) {
    this.transitionToRoute('vertex.index', entityID);
  })
});
