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

export default MultiTableController.extend({

  columnSelectorTitle: 'Customize vertex tooltip',

  breadcrumbs: [{
    text: "Graphical View",
    routeName: "dag.graphical",
  }],

  columns: ColumnDefinition.make([{
    id: 'name',
    headerTitle: 'Vertex Name',
    contentPath: 'name',
    cellComponentName: 'em-table-linked-cell',
    getCellContent: function (row) {
      return {
        routeName: "vertex",
        model: row.get("entityID"),
        text: row.get("name")
      };
    }
  },{
    id: 'entityID',
    headerTitle: 'Vertex Id',
    contentPath: 'entityID'
  },{
    id: 'status',
    headerTitle: 'Status',
    contentPath: 'finalStatus',
    cellComponentName: 'em-table-status-cell',
    observePath: true
  },{
    id: 'progress',
    headerTitle: 'Progress',
    contentPath: 'progress',
    cellComponentName: 'em-table-progress-cell',
    observePath: true
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
    observePath: true
  },{
    id: 'runningTasks',
    headerTitle: 'Running Tasks',
    contentPath: 'runningTasks',
    observePath: true
  },{
    id: 'pendingTasks',
    headerTitle: 'Pending Tasks',
    contentPath: 'pendingTasks',
    observePath: true
  },{
    id: 'processorClassName',
    headerTitle: 'Processor Class',
    contentPath: 'processorClassName',
  }]),

  redirect: function (details) {
    switch(details.type) {
      case 'vertex':
        this.transitionToRoute('vertex.index', details.d.get('data.entityID'));
      break;
      case 'task':
        this.transitionToRoute('vertex.tasks', details.d.get('data.entityID'));
      break;
      case 'io':
      break;
      case 'input':
      break;
      case 'output':
      break;
    }
  },

  actions: {
    entityClicked: function (details) {

      /**
       * In IE 11 under Windows 7, mouse events are not delivered to the page
       * anymore at all after a SVG use element that was under the mouse is
       * removed from the DOM in the event listener in response to a mouse click.
       * See https://connect.microsoft.com/IE/feedback/details/796745
       *
       * This condition and related actions must be removed once the bug is fixed
       * in all supported IE versions
       */
      if(this.get("env.ENV.isIE")) {
        var pageType = details.type === "io" ? "additionals" : details.type,
            message = `You will be redirected to ${pageType} page`;

        alert(message);
      }
      this.redirect(details);
    }
  },

  viewData: Ember.computed("model", function () {
    var model = this.get("model"),
        dag, vertices, entities;

    if(!model) {
      return {};
    }

    dag = this.get('model.firstObject.dag');
    vertices = this.get('model.firstObject.dag.vertices') || [];
    entities = {};

    model.forEach(function (vertexData) {
      entities[vertexData.get('name')] = vertexData;
    });

    vertices.forEach(function (vertex) {
      vertex.data = entities[vertex.vertexName];
    });

    return {
      vertices: vertices,
      edges: dag.get('edges'),
      vertexGroups: dag.get('vertexGroups')
    };
  })

});
