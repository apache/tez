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

App.DagVerticesController = Em.ObjectController.extend(App.PaginatedContentMixin, App.ColumnSelectorMixin, {
  needs: "dag",

  controllerName: 'DagVerticesController',

  // required by the PaginatedContentMixin
  childEntityType: 'vertex',

  count: 50,

  queryParams: {
    status_filter: 'status'
  },

  status_filter: null,

  loadData: function() {
    var filters = {
      primary: {
        TEZ_DAG_ID: this.get('controllers.dag.id')
      },
      secondary: {
        status: this.status_filter
      }
    }
    this.setFiltersAndLoadEntities(filters);
  },

  loadAdditional: function() {
    var defer = Em.RSVP.defer();

    var that = this;
        vertices = this.get('entities');

    var dagStatus = this.get('controllers.dag.status');
    if (App.Helpers.misc.isStatusInUnsuccessful(dagStatus)) {
      vertices.filterBy('status', 'RUNNING')
        .forEach(function(item) {
          item.set('status', 'KILLED');
        });
    }

    var runningVerticesIdx = vertices
      .filterBy('status', 'RUNNING')
      .map(function(item) {
        return item.get('id').split('_').splice(-1).pop();
      });
    if (runningVerticesIdx.length > 0) {
      this.store.findQuery('vertexProgress', {
        metadata: {
          appId: that.get('applicationId'),
          dagIdx: that.get('idx'),
          vertexIds: runningVerticesIdx.join(',')
        }
      }).then(function(vertexProgressInfo) {
        vertexProgressInfo.forEach(function(item) {
          var model = vertices.findBy('id', item.get('id')) || Em.Object.create();
          model.set('progress', item.get('progress'));
        });
      }).catch(function(error) {
        Em.Logger.debug("failed to fetch vertex progress")
      }).finally(function(){
        defer.resolve();
      })
    } else {
      defer.resolve();
    }

    return defer.promise;
  },

  actions : {
    filterUpdated: function(filterID, value) {
      // any validations required goes here.
      if (!!value) {
        this.set(filterID, value);
      } else {
        this.set(filterID, null);
      }
      this.loadData();
    }
  },

  defaultColumnConfigs: function() {
    return [
      {
        id: 'vertexName',
        headerCellName: 'Vertex Name',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'vertex' view.cellContent.id class='ember-table-content'}}{{view.cellContent.name}}{{/link-to}}")
        }),
        getCellContent: function(row) {
          return {
            id: row.get('id'),
            name: row.get('name')
          };
        }
      },
      {
        id: 'id',
        headerCellName: 'Vertex ID',
        contentPath: 'id',
      },
      {
        id: 'startTime',
        headerCellName: 'Start Time',
        getCellContent: function(row) {
          return App.Helpers.date.dateFormat(row.get('startTime'));
        }
      },
      {
        id: 'endTime',
        headerCellName: 'End Time',
        getCellContent: function(row) {
          return App.Helpers.date.dateFormat(row.get('endTime'));
        }
      },
      {
        id: 'duration',
        headerCellName: 'Duration',
        getCellContent: function(row) {
          var st = row.get('startTime');
          var et = row.get('endTime');
          if (st && et) {
            return App.Helpers.date.durationSummary(st, et);
          }
        }
      },
      {
        id: 'firstTaskStartTime',
        headerCellName: 'First Task Start Time',
        getCellContent: function(row) {
          return App.Helpers.date.dateFormat(row.get('firstTaskStartTime'));
        }
      },
      {
        id: 'tasks',
        headerCellName: 'Tasks',
        contentPath: 'numTasks'
      },
      {
        id: 'processorClass',
        headerCellName: 'Processor Class',
        contentPath: 'processorClassName'
      },
      {
        id: 'status',
        headerCellName: 'Status',
        filterID: 'status_filter',
        filterType: 'dropdown',
        dropdownValues: App.Helpers.misc.vertexStatusUIOptions,
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            '<span class="ember-table-content">&nbsp;\
            <i {{bind-attr class=":task-status view.cellContent.statusIcon"}}></i>\
            &nbsp;&nbsp;{{view.cellContent.status}}\
            {{#if view.cellContent.progress}} {{bs-badge content=view.cellContent.progress}}{{/if}}</span>')
        }),
        getCellContent: function(row) {
          var pct,
              vertexStatus = row.get('status');
          if (Ember.typeOf(row.get('progress')) === 'number') {
            pct = App.Helpers.number.fractionToPercentage(row.get('progress'));
          }

          return {
            status: vertexStatus,
            statusIcon: App.Helpers.misc.getStatusClassForEntity(vertexStatus),
            progress: pct
          };
        }
      },
      {
        id: 'configurations',
        headerCellName: 'Source/Sink Configs',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            " {{#if view.cellContent.linkToAdditionals}}\
                {{#link-to 'vertex.additionals' view.cellContent.vertexId class='ember-table-content'}}View sources & sinks{{/link-to}}\
              {{else}}{{#if view.cellContent.inputId}}\
                {{#link-to 'input.configs' view.cellContent.vertexId view.cellContent.inputId class='ember-table-content'}}View source configs{{/link-to}}\
              {{else}}{{#if view.cellContent.outputId}}\
                {{#link-to 'output.configs' view.cellContent.vertexId view.cellContent.outputId class='ember-table-content'}}View sink configs{{/link-to}}\
              {{else}}\
                <span class='ember-table-content'>No source or sink</span>\
              {{/if}}{{/if}}{{/if}}")
        }),
        getCellContent: function(row) {
          var firstInputId = row.get('inputs.content.0.id'),
              firstOutputId = row.get('outputs.content.0.id');
          return {
            linkToAdditionals: row.get('inputs.content.length') > 1 ||
                row.get('outputs.content.length') > 1 ||
                (firstInputId != undefined && firstOutputId != undefined),
            inputId: firstInputId,
            outputId: firstOutputId,
            vertexId: row.get('id')
          };
        }
      }
    ];
  }.property(),

  columnConfigs: function() {
    return this.get('defaultColumnConfigs').concat(
      App.Helpers.misc.normalizeCounterConfigs(
        App.get('Configs.defaultCounters').concat(
          App.get('Configs.tables.entity.vertex') || [],
          App.get('Configs.tables.sharedColumns') || []
        )
      )
    );
  }.property(),

});
