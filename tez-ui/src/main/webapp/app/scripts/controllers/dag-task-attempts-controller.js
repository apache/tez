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

App.DagTaskAttemptsController = Em.ObjectController.extend(App.PaginatedContentMixin, App.ColumnSelectorMixin, {
  // Required by the PaginatedContentMixin
  needs: 'dag',
  childEntityType: 'taskAttempt',

  controllerName: 'DagTaskAttemptsController',

  queryParams: {
    status_filter: 'status',
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

  defaultColumnConfigs: function() {
    return [
      {
        id: 'taskId',
        headerCellName: 'Task Index',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'task' view.cellContent.taskId class='ember-table-content'}}{{view.cellContent.displayId}}{{/link-to}}")
        }),
        getCellContent: function (row) {
          var taskId = row.get('taskID'),
              idPrefix = 'task_%@_'.fmt(row.get('dagID').substr(4));
          return {
            taskId: taskId,
            displayId: taskId.indexOf(idPrefix) == 0 ? taskId.substr(idPrefix.length) : taskId
          };
        }
      },
      {
        id: 'attemptNo',
        headerCellName: 'Attempt No',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'taskAttempt' view.cellContent.attemptID class='ember-table-content'}}{{view.cellContent.attemptNo}}{{/link-to}}")
        }),
        getCellContent: function(row) {
          var attemptID = row.get('id') || '',
              attemptNo = attemptID.split(/[_]+/).pop();
          return {
            attemptNo: attemptNo,
            attemptID: attemptID
          };
        }
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
        id: 'containerId',
        headerCellName: 'Container',
        contentPath: 'containerId'
      },
      {
        id: 'nodeId',
        headerCellName: 'Node',
        contentPath: 'nodeId'
      },
      {
        id: 'actions',
        headerCellName: 'Actions',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            '<span class="ember-table-content">\
            {{#link-to "taskAttempt.counters" view.cellContent}}counters{{/link-to}}&nbsp;\
            </span>'
            )
        }),
        contentPath: 'id'
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
                <a target="_blank" href="//{{unbound view.cellContent}}">View</a>\
                &nbsp;\
                <a target="_blank" href="//{{unbound view.cellContent}}?start=0" download target="_blank" type="application/octet-stream">Download</a>\
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
      App.Helpers.misc.normalizeCounterConfigs(
        App.get('Configs.defaultCounters').concat(
          App.get('Configs.tables.entity.taskAttempt') || [],
          App.get('Configs.tables.sharedColumns') || []
        )
      )
    );
  }.property(),

});
