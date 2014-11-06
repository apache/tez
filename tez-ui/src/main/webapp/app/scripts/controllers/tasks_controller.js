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

App.TasksController = Em.ObjectController.extend(App.PaginatedContentMixin, {
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

	/* table view for tasks */
  columns: function() {
    var store = this.get('store');
    var columnHelper = function(columnName, valName) {
      return Em.Table.ColumnDefinition.create({
        textAlign: 'text-align-left',
        headerCellName: columnName,
        getCellContent: function(row) {
          return row.get(valName);
        }
      });
    }

    var idColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Task Id',
      tableCellViewClass: Em.Table.TableCell.extend({
      	template: Em.Handlebars.compile(
          "{{#link-to 'task' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}")
      }),
      getCellContent: function(row) {
      	return row.get('id');
      }
    });

    var vertexColumn = columnHelper('Vertex ID', 'vertexID');

    var startTimeColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Submission Time',
      getCellContent: function(row) {
      	return App.Helpers.date.dateFormat(row.get('startTime'));
      }
    });

    var endTimeColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'End Time',
      getCellContent: function(row) {
        return App.Helpers.date.dateFormat(row.get('endTime'));
      }
    });

    var statusColumn = Em.Table.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Status',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          '<span class="ember-table-content">&nbsp;\
          <i {{bind-attr class=":task-status view.cellContent.statusIcon"}}></i>\
          &nbsp;&nbsp;{{view.cellContent.status}}</span>')
      }),
      getCellContent: function(row) {
      	return { 
          status: row.get('status'),
          statusIcon: App.Helpers.misc.getStatusClassForEntity(row)
        };
      }
    });
    
    return [idColumn, vertexColumn, startTimeColumn, endTimeColumn, statusColumn];
  }.property(),
});