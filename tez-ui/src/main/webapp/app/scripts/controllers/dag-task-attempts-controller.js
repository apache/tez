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

App.DagTaskAttemptsController = App.TablePageController.extend({

  controllerName: 'DagTaskAttemptsController',
  needs: "dag",

  entityType: 'dagTaskAttempt',
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

    data.forEach(function (attempt) {
      var attemptStatus = App.Helpers.misc
        .getFixedupDisplayStatus(attempt.get('status'));
      if (attemptStatus == 'RUNNING' && isUnsuccessfulDag) {
        attemptStatus = 'KILLED'
      }
      if (attemptStatus != attempt.get('status')) {
        attempt.set('status', attemptStatus);
      }
    });

    return this._super();
  },

  defaultColumnConfigs: function() {
    var that = this,
        vertexIdToNameMap = this.get('controllers.dag.vertexIdToNameMap') || {};
    return [
      {
        id: 'taskId',
        headerCellName: 'Task Index',
        templateName: 'components/basic-table/linked-cell',
        contentPath: 'taskID',
        getCellContent: function (row) {
          var taskId = row.get('taskID'),
              idPrefix = 'task_%@_'.fmt(row.get('dagID').substr(4));
          return {
            linkTo: 'task',
            entityId: taskId,
            displayText: taskId.indexOf(idPrefix) == 0 ? taskId.substr(idPrefix.length) : taskId
          };
        },
        getSearchValue: function (row) {
          var id = row.get('taskID'),
              idPrefix = 'task_%@_'.fmt(row.get('dagID').substr(4));
          return id.indexOf(idPrefix) == 0 ? id.substr(idPrefix.length) : id;
        }
      },
      {
        id: 'attemptNo',
        headerCellName: 'Attempt No',
        templateName: 'components/basic-table/linked-cell',
        contentPath: 'id',
        getCellContent: function(row) {
          var attemptID = row.get('id') || '';
          return {
            linkTo: 'taskAttempt',
            displayText: attemptID.split(/[_]+/).pop(),
            entityId: attemptID
          };
        },
        getSearchValue: function (row) {
          var attemptID = row.get('id') || '';
          return attemptID.split(/[_]+/).pop();
        },
        getSortValue: function (row) {
          var attemptID = row.get('id') || '';
          return attemptID.split(/[_]+/).pop();
        }
      },
      {
        id: 'status',
        headerCellName: 'Status',
        templateName: 'components/basic-table/status-cell',
        contentPath: 'status',
        getCellContent: function(row) {
          var status = App.Helpers.misc.getFixedupDisplayStatus(row.get('status'));
          return {
            status: status,
            statusIcon: App.Helpers.misc.getStatusClassForEntity(status)
          };
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
        getSearchValue: function (row) {
          var vertexId = row.get('vertexID');
          return vertexIdToNameMap[vertexId] || vertexId;
        },
        getSortValue: function (row) {
          var vertexId = row.get('vertexID');
          return vertexIdToNameMap[vertexId] || vertexId;
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
        templateName: 'components/basic-table/linked-cell',
        searchAndSortable: false,
        contentPath: 'id',
        getCellContent: function(row) {
          var attemptID = row.get('id') || '';
          return {
            linkTo: 'taskAttempt.counters',
            displayText: 'counters',
            entityId: attemptID
          };
        }
      },
      {
        id: 'logs',
        headerCellName: 'Logs',
        templateName: 'components/basic-table/logs-cell',
        searchAndSortable: false,
        getCellContent: function(row) {
          var yarnAppState = that.get('controllers.dag.yarnAppState'),
              suffix = "/syslog_" + row.get('id'),
              link = row.get('inProgressLog') || row.get('completedLog'),
              cellContent = {};

          if(link) {
            cellContent.viewUrl = link + suffix;
          }
          link = row.get('completedLog');
          if (link && yarnAppState === 'FINISHED' || yarnAppState === 'KILLED' || yarnAppState === 'FAILED') {
            cellContent.downloadUrl = link + suffix;
          }

          cellContent.notAvailable = cellContent.viewUrl || cellContent.downloadUrl;

          return cellContent;
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
