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

App.VertexTasksController = App.TablePageController.extend(App.AutoCounterColumnMixin, {

  controllerName: 'VertexTasksController',
  needs: "vertex",

  entityType: 'vertexTask',
  baseEntityType: 'task',
  filterEntityType: 'vertex',
  filterEntityId: Ember.computed.alias('controllers.vertex.id'),

  cacheDomain: Ember.computed.alias('controllers.vertex.dagID'),

  beforeLoad: function () {
    var controller = this.get('controllers.vertex'),
        model = controller.get('model');
    return model.reload().then(function () {
      return controller.loadAdditional(model);
    });
  },

  afterLoad: function () {
    var data = this.get('data'),
        isUnsuccessfulVertex = App.Helpers.misc.isStatusInUnsuccessful(
          this.get('controllers.vertex.status')
        );

    data.forEach(function (task) {
      var taskStatus = App.Helpers.misc.getFixedupDisplayStatus(task.get('status'));

      if (taskStatus == 'RUNNING' && isUnsuccessfulVertex) {
        taskStatus = 'KILLED'
      }
      if (taskStatus != task.get('status')) {
        task.set('status', taskStatus);
      }
    });

    return this._super();
  },

  defaultColumnConfigs: function() {
    var that = this;

    function getLogContent(attempt) {
      var yarnAppState = that.get('controllers.vertex.yarnAppState'),
          suffix,
          link,
          cellContent = {};

      if(attempt) {
        suffix = "/syslog_" + attempt.get('id'),
        link = attempt.get('inProgressLog') || attempt.get('completedLog');

        if(link) {
          cellContent.viewUrl = link + suffix;
        }
        link = attempt.get('completedLog');
        if (link && yarnAppState === 'FINISHED' || yarnAppState === 'KILLED' || yarnAppState === 'FAILED') {
          cellContent.downloadUrl = link + suffix;
        }
      }

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
  }.property('filterEntityId')

});