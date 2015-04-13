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

App.TaskAttemptsController = App.TablePageController.extend({

  controllerName: 'TaskAttemptsController',
  needs: "task",

  entityType: 'taskTaskAttempt',
  filterEntityType: 'task',
  filterEntityId: Ember.computed.alias('controllers.task.id'),

  cacheDomain: Ember.computed.alias('controllers.task.dagID'),

  beforeLoad: function () {
    var taskController = this.get('controllers.task'),
        model = taskController.get('model');
    return model.reload().then(function () {
      return taskController.loadAdditional(model);
    });
  },

  afterLoad: function () {
    var loaders = [],
        that = this;

    App.Helpers.misc.removeRecord(that.store, 'dag', that.get('controllers.task.dagID'));

    var appDetailFetcher = that.store.find('dag', that.get('controllers.task.dagID')).
      then(function (dag) {
        App.Helpers.misc.removeRecord(that.store, 'appDetail', dag.get('applicationId'));
        return that.store.find('appDetail', dag.get('applicationId'));
      }).
      then(function(appDetail) {
        var appState = appDetail.get('appState');
        if (appState) {
          that.set('yarnAppState', appState);
        }
      });
    loaders.push(appDetailFetcher);

    return Em.RSVP.allSettled(loaders);
  },

  defaultColumnConfigs: function() {
    var that = this;
    return [
      {
        id: 'id',
        headerCellName: 'Attempt Index',
        templateName: 'components/basic-table/linked-cell',
        contentPath: 'id',
        getCellContent: function (row) {
          var id = row.get('id'),
              idPrefix = 'attempt_%@_'.fmt(row.get('dagID').substr(4));
          return {
            linkTo: 'taskAttempt',
            entityId: id,
            displayText: id.indexOf(idPrefix) == 0 ? id.substr(idPrefix.length) : id
          };
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
          var status = App.Helpers.misc.getFixedupDisplayStatus(row.get('status'));
          return {
            status: status,
            statusIcon: App.Helpers.misc.getStatusClassForEntity(status)
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
          var yarnAppState = that.get('yarnAppState'),
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


App.TaskAttemptIndexController = Em.ObjectController.extend(App.ModelRefreshMixin, {
  controllerName: 'TaskAttemptIndexController',

  taskAttemptStatus: function() {
    return App.Helpers.misc.getFixedupDisplayStatus(this.get('status'));
  }.property('id', 'status'),

  taskAttemptIconStatus: function() {
    return App.Helpers.misc.getStatusClassForEntity(this.get('taskAttemptStatus'));
  }.property('id', 'status', 'counterGroups'),

});
