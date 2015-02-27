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

App.TasksController = Em.ObjectController.extend(App.PaginatedContentMixin, App.ColumnSelectorMixin, {
  // Required by the PaginatedContentMixin
  childEntityType: 'task',

  controllerName: 'TasksController',

  pageTitle: 'Tasks',

  pageSubTitle: 'All Tasks',

  queryParams: {
    parentType: true,
    parentID: true,
    status_filter: 'status'
  },

  parentName: 'Loading...', // So that a proper message is displayed
  parentType: null,
  parentID: null,
  status_filter: null,

  loadData: function() {
    var filters = {
      primary: {},
      secondary: {
        status: this.status_filter
      }
    }
    filters.primary[this.parentType] = this.parentID;
    this.setFiltersAndLoadEntities(filters);
  },

  loadAdditional: function (loader) {
    var that = this;
    return this.store.find('dag', this.get('parentID')).
      then(function (parent) {
        that.set('parentName', parent.get('name'));
      });
  },

  defaultColumnConfigs: function() {

    return [
      {
        id: 'taskId',
        headerCellName: 'Task Id',
        contentPath: 'id',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'task' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}")
        })
      },
      {
        id: 'vertexId',
        headerCellName: 'Vertex ID',
        contentPath: 'vertexID'
      },
      {
        id: 'submissionTime',
        headerCellName: 'Submission Time',
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
        id: 'status',
        headerCellName: 'Status',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            '<span class="ember-table-content">&nbsp;\
            <i {{bind-attr class=":task-status view.cellContent.statusIcon"}}></i>\
            &nbsp;&nbsp;{{view.cellContent.status}}</span>')
        }),
        getCellContent: function(row) {
          var taskStatus = row.get('status');
          return {
            status: taskStatus,
            statusIcon: App.Helpers.misc.getStatusClassForEntity(taskStatus)
          };
        }
      }
    ];
    
  }.property(),

  columnConfigs: function() {
    return this.get('defaultColumnConfigs').concat(
      App.Helpers.misc.normalizeCounterConfigs(
        App.get('Configs.defaultCounters').concat(
          App.get('Configs.tables.entity.task') || [],
          App.get('Configs.tables.sharedColumns') || []
        )
      )
    );
  }.property(),

});