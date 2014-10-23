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
	reloadData: function() {
		this.loadEntities();
	}.observes('id'),

	// required by the PaginatedContentMixin
	parentEntityType: 'dag',
	childEntityType: 'vertex',

	filterValues: {},

  /*
  reloadWhenFiltersChange: function() {
    var filters = {
      id: null,
      primary: {},
      secondary: {}
    };
    var fv = this.filterValues;
    filters.primary.TEZ_VERTEX_ID = fv.vertex_id;
    filters.secondary.status = fv.status;
    this.setFiltersAndLoadEntities(filters);
  }.observes('filterValues.task_id', 'filterValues.vertex_id', 'filterValues.status'),
*/
	columns: function() {
		var idCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Vertex ID',
      tableCellViewClass: Em.Table.TableCell.extend({
      	template: Em.Handlebars.compile(
      		"{{#link-to 'vertex' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}")
      }),
      //filterID: 'task_id',
      contentPath: 'id',
    });

    var nameCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Name',
      contentPath: 'name',
    });

    var startTimeCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Submission Time',
      getCellContent: function(row) {
      	return App.Helpers.date.dateFormat(row.get('startTime'));
      }
    });

    var endTimeCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'End Time',
      getCellContent: function(row) {
        return App.Helpers.date.dateFormat(row.get('endTime'));
      }
    });

    var numTasksCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Tasks',
      contentPath: 'numTasks'
    });

    var statusCol = App.ExTable.ColumnDefinition.createWithMixins(App.ExTable.FilterColumnMixin,{
      textAlign: 'text-align-left',
      headerCellName: 'Status',
      filterID: 'status',
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

		return [idCol, nameCol, startTimeCol, endTimeCol, numTasksCol, statusCol];
	}.property(),
});