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

App.DagViewController = App.TablePageController.extend({
  controllerName: 'DagViewController',
  needs: ["dag", "dagVertices"],

  entityType: 'dagVertex',
  filterEntityType: 'dag',
  filterEntityId: Ember.computed.alias('controllers.dag.id'),

  cacheDomain: Ember.computed.alias('controllers.dag.id'),

  columnSelectorTitle: 'Customize vertex tooltip',

  beforeLoad: function () {
    var dagController = this.get('controllers.dag'),
        model = dagController.get('model');
    return model.reload().then(function () {
      return dagController.loadAdditional(model);
    });
  },

  afterLoad: function () {
    var data = this.get('data'),
        runningVerticesIdx,
        isUnsuccessfulDag = App.Helpers.misc.isStatusInUnsuccessful(
          this.get('controllers.dag.status')
        );

    if(isUnsuccessfulDag) {
      data.filterBy('status', 'RUNNING').forEach(function (vertex) {
        vertex.set('status', 'KILLED');
      });
    }

    return this._super();
  },

  redirect: function (details) {
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
  },

  actions: {
    modalConfirmed: function () {
      this.redirect(this.get('redirectionDetails'));
    },
    modalCanceled: function () {
    },
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
      if(App.env.isIE) {
        this.set('redirectionDetails', details);
        Bootstrap.ModalManager.confirm(
          this,
          'Confirmation Required!',
          'You will be redirected to %@ page'.fmt(
            details.type == "io" ? "additionals" : details.type
          )
        );
      }
      else {
        this.redirect(details);
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
          (config.getCellContent && config.searchAndSortable != false);
    });
  }.property(),

  viewData: function () {
    var vertices = this.get('controllers.dag.vertices') || [],
        entities = this.get('data') || [],
        finalVertex,
        dagStatus = this.get('controllers.dag.status'),
        needsStatusFixup = App.Helpers.misc.isStatusInUnsuccessful(dagStatus);

    entities = entities.reduce(function (obj, vertexData) {
      obj[vertexData.get('name')] = vertexData;
      return obj;
    }, {});

    vertices.forEach(function (vertex) {
      vertex.data = entities[vertex.vertexName];
      if (needsStatusFixup && vertex.data && vertex.data.get('status') == 'RUNNING') {
        vertex.data.set('status', 'KILLED');
      }
    });

    return {
      vertices: vertices,
      edges: this.get('controllers.dag.edges'),
      vertexGroups: this.get('controllers.dag.vertexGroups')
    };
  }.property('data', 'controllers.dag.vertices', 'controllers.dag')
});
