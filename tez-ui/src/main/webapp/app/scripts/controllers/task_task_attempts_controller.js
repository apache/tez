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

App.TaskAttemptsController = Em.ObjectController.extend(App.PaginatedContentMixin, App.ColumnSelectorMixin, {
  // required by the PaginatedContentMixin
  childEntityType: 'task_attempt',

  controllerName: 'TaskAttemptsController',

  needs: 'task',

  queryParams: {
    status_filter: 'status'
  },
  status_filter: null,

  loadData: function() {
    var filters = {
      primary: {
        TEZ_TASK_ID: this.get('controllers.task.id')
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

  defaultColumnConfigs: function() {
    return [
      {
        id: 'taskId',
        headerCellName: 'Task ID',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'taskAttempt' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}")
        }),
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
        id: 'status',
        headerCellName: 'Status',
        filterID: 'status_filter',
        filterType: 'dropdown',
        dropdownValues: App.Helpers.misc.taskStatusUIOptions,
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
      },
      {
        id: 'nodeId',
        headerCellName: 'Node ID',
        contentPath: 'nodeId'
      },
      {
        id: 'containerId',
        headerCellName: 'Container ID',
        contentPath: 'containerId'
      },
      {
        id: 'logs',
        headerCellName: 'Logs',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            '<span class="ember-table-content">\
              {{#unless view.cellContent}}\
                Not Available\
              {{else}}\
                <a href="//{{unbound view.cellContent}}">View</a>\
                &nbsp;\
                <a href="//{{unbound view.cellContent}}?start=0" download target="_blank" type="application/octet-stream">Download</a>\
              {{/unless}}\
            </span>')
        }),
        getCellContent: function(row) {
          var logFile = row.get('inProgressLog') || row.get('completedLog');
          if(logFile) logFile += "/syslog_" + row.get('id');
          return logFile;
        }
      }
    ];

  }.property(),

  columnConfigs: function() {
    return this.get('defaultColumnConfigs').concat(
      App.Helpers.misc.normalizeCounterConfigs(App.get('Configs.table.commonColumns.counters')),
      App.get('Configs.table.entitieSpecificColumns.taskAttempt') || []
    );
  }.property(),

});


App.TaskAttemptIndexController = Em.ObjectController.extend({
  controllerName: 'TaskAttemptIndexController',

  taskIconStatus: function() {
    return App.Helpers.misc.getStatusClassForEntity(this.get('model'));
  }.property('id', 'status', 'counterGroups'),

});
