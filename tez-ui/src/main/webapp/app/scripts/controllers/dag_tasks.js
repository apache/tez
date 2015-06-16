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

App.DagTasksController = App.TablePageController.extend({

  controllerName: 'DagTasksController',
  needs: "dag",

  entityType: 'dagTask',
  filterEntityType: 'dag',
  filterEntityId: Ember.computed.alias('controllers.dag.id'),

  cacheDomain: Ember.computed.alias('controllers.dag.id'),

  beforeLoad: function () {
    var dagController = this.get('controllers.dag'),
        model = dagController.get('model');
    return model.reload().then(function () {
      return dagController.loadAdditional(model);
    });
  },

  afterLoad: function () {
    var data = this.get('data'),
        isUnsuccessfulDag = App.Helpers.misc.isStatusInUnsuccessful(
          this.get('controllers.dag.status')
        );

    data.forEach(function (task) {
      var taskStatus = App.Helpers.misc.getFixedupDisplayStatus(task.get('status'));

      if (taskStatus == 'RUNNING' && isUnsuccessfulDag) {
        taskStatus = 'KILLED'
      }
      if (taskStatus != task.get('status')) {
        task.set('status', taskStatus);
      }
    });

    return this._super();
  },

  defaultColumnConfigs: function() {
    var that = this,
        vertexIdToNameMap = this.get('controllers.dag.vertexIdToNameMap') || {};

    function getLogContent(attempt) {
      var cellContent = App.Helpers.misc.constructLogLinks(
            attempt,
            that.get('controllers.dag.yarnAppState'),
            that.get('controllers.dag.tezApp.user')
          );

      cellContent.notAvailable = cellContent.viewUrl || cellContent.downloadUrl;
      return cellContent;
    }

    return [
      {
        id: 'id',
        headerCellName: 'Task Index',
        templateName: 'components/basic-table/linked-cell',
        contentPath: 'id',
        getCellContent: function (row) {
          var id = row.get('id'),
              idPrefix = 'task_%@_'.fmt(row.get('dagID').substr(4));
          return {
            linkTo: 'task',
            entityId: id,
            displayText: id.indexOf(idPrefix) == 0 ? id.substr(idPrefix.length) : id
          };
        },
        getSearchValue: function (row) {
          var id = row.get('id'),
              idPrefix = 'task_%@_'.fmt(row.get('dagID').substr(4));
          return id.indexOf(idPrefix) == 0 ? id.substr(idPrefix.length) : id;
        }
      },
      {
        id: 'vertexName',
        headerCellName: 'Vertex Name',
        contentPath: 'vertexID',
        getCellContent: function(row) {
          var vertexId = row.get('vertexID');
          return vertexIdToNameMap[vertexId] || vertexId;
        },
        getSearchValue: function(row) {
          var vertexId = row.get('vertexID');
          return vertexIdToNameMap[vertexId] || vertexId;
        },
        getSortValue: function(row) {
          var vertexId = row.get('vertexID');
          return vertexIdToNameMap[vertexId] || vertexId;
        }
      },
      {
        id: 'status',
        headerCellName: 'Status',
        templateName: 'components/basic-table/status-cell',
        contentPath: 'status',
        getCellContent: function(row) {
          var status = row.get('status');
          return {
            status: status,
            statusIcon: App.Helpers.misc.getStatusClassForEntity(status,
              row.get('hasFailedTaskAttempts'))
          };
        }
      },
      {
        id: 'startTime',
        headerCellName: 'Start Time',
        contentPath: 'startTime',
        getCellContent: function(row) {
          return App.Helpers.date.dateFormat(row.get('startTime'));
        },
        getSearchValue: function(row) {
          return App.Helpers.date.dateFormat(row.get('startTime'));
        }
      },
      {
        id: 'endTime',
        headerCellName: 'End Time',
        contentPath: 'endTime',
        getCellContent: function(row) {
          return App.Helpers.date.dateFormat(row.get('endTime'));
        },
        getSearchValue: function(row) {
          return App.Helpers.date.dateFormat(row.get('endTime'));
        },
      },
      {
        id: 'duration',
        headerCellName: 'Duration',
        contentPath: 'duration',
        getCellContent: function(row) {
          return App.Helpers.date.timingFormat(row.get('duration'), 1);
        },
        getSearchValue: function(row) {
          return App.Helpers.date.timingFormat(row.get('duration'), 1);
        },
      },
      {
        id: 'actions',
        headerCellName: 'Actions',
        templateName: 'components/basic-table/task-actions-cell',
        contentPath: 'id',
        searchAndSortable: false
      },
      {
        id: 'logs',
        headerCellName: 'Logs',
        templateName: 'components/basic-table/logs-cell',
        searchAndSortable: false,
        getCellContent: function(row) {
          var taskAttemptId = row.get('successfulAttemptId') || row.get('attempts.lastObject'),
              store = that.get('store');

          if (taskAttemptId) {
            return store.find('taskAttempt', taskAttemptId).then(getLogContent);
          }
        }
      }
    ];
  }.property('id'),

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
