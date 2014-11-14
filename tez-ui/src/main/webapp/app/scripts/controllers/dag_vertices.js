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

App.DagVerticesController = Em.ObjectController.extend(App.PaginatedContentMixin, {
  needs: "dag",

  // required by the PaginatedContentMixin
  childEntityType: 'vertex',

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

  columns: function() {
    var idCol = App.ExTable.ColumnDefinition.create({
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
    });

    var nameCol = App.ExTable.ColumnDefinition.create({
      headerCellName: 'Vertex ID',
      contentPath: 'id',
    });

    var startTimeCol = App.ExTable.ColumnDefinition.create({
      headerCellName: 'Start Time',
      getCellContent: function(row) {
      	return App.Helpers.date.dateFormat(row.get('startTime'));
      }
    });

    var endTimeCol = App.ExTable.ColumnDefinition.create({
      headerCellName: 'End Time',
      getCellContent: function(row) {
        return App.Helpers.date.dateFormat(row.get('endTime'));
      }
    });

    var firstTaskStartTime = App.ExTable.ColumnDefinition.create({
      headerCellName: 'First Task Start Time',
      getCellContent: function(row) {
        return App.Helpers.date.dateFormat(row.get('firstTaskStartTime'));
      }
    });

    var numTasksCol = App.ExTable.ColumnDefinition.create({
      headerCellName: 'Tasks',
      contentPath: 'numTasks'
    });

    var statusCol = App.ExTable.ColumnDefinition.createWithMixins(App.ExTable.FilterColumnMixin,{
      headerCellName: 'Status',
      filterID: 'status_filter',
      filterType: 'dropdown',
      dropdownValues: App.Helpers.misc.vertexStatusUIOptions,
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

		return [idCol, nameCol, startTimeCol, endTimeCol, firstTaskStartTime, statusCol, numTasksCol];
	}.property(),
});