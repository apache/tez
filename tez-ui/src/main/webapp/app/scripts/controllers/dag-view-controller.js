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

App.DagViewController = Em.ObjectController.extend(App.PaginatedContentMixin, App.ColumnSelectorMixin, {
  controllerName: 'DagViewController',

  childEntityType: 'vertex',

  needs: ["dag", "dagVertices"],
  pageTitle: 'Dag View',

  columnSelectorTitle: 'Customize vertex tooltip',

  count: Number.MAX_SAFE_INTEGER - 1,

  loadData: function() {
    var filters = {
      primary: {
        TEZ_DAG_ID: this.get('controllers.dag.id')
      }
    }
    this.setFiltersAndLoadEntities(filters);
  },

  actions: {
    entityClicked: function (details) {
      switch(details.type) {
        case 'vertex':
          this.transitionToRoute('vertex', details.d.get('data.id'));
        break;
        case 'task':
          this.transitionToRoute('vertex.tasks', details.d.get('data.id'));
        break;
        case 'io':
          this.transitionToRoute('vertex.additionals', details.d.get('data.id'));
        break;
        case 'input':
          this.transitionToRoute('input.configs', details.d.get('parent.data.id'), details.d.entity);
        break;
        case 'output':
          this.transitionToRoute('output.configs', details.d.get('vertex.data.id'), details.d.entity);
        break;
      }
    }
  },

  defaultColumnConfigs: function() {
    return this.get('controllers.dagVertices.defaultColumnConfigs');
  }.property(),

  columnConfigs: function() {
    var configs = this.get('controllers.dagVertices.columnConfigs');
    return configs.filter(function (config) {
      return (config.contentPath) ||
          (config.getCellContent && !config.tableCellViewClass);
    });
  }.property(),

  viewData: function () {
    var vertices = this.get('controllers.dag.vertices'),
        entities = this.get('entities'),
        finalVertex;

    entities = entities.reduce(function (obj, vertexData) {
      obj[vertexData.get('name')] = vertexData;
      return obj;
    }, {});

    vertices.forEach(function (vertex) {
      vertex.data = entities[vertex.vertexName];
    });

    return {
      vertices: vertices,
      edges: this.get('controllers.dag.edges'),
      vertexGroups: this.get('controllers.dag.vertexGroups')
    };
  }.property('entities')
});
