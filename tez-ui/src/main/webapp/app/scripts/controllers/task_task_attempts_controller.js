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

App.TaskAttemptsController = App.TablePageController.extend(App.AutoCounterColumnMixin, {

  controllerName: 'TaskAttemptsController',
  needs: "task",

  entityType: 'taskTaskAttempt',
  baseEntityType: 'taskAttempt',
  filterEntityType: 'task',
  filterEntityId: Ember.computed.alias('controllers.task.id'),

  cacheDomain: Ember.computed.alias('controllers.task.dagID'),

  pollingType: 'attemptInfo',

  pollsterControl: function () {
    if(this.get('vertex.dag.status') == 'RUNNING' &&
        this.get('vertex.dag.amWebServiceVersion') != '1' &&
        !this.get('loading') &&
        this.get('isActive') &&
        this.get('pollingEnabled') &&
        this. get('rowsDisplayed.length') > 0) {
      this.get('pollster').start();
    }
    else {
      this.get('pollster').stop();
    }
  }.observes('vertex.dag.status',
    'vertex.dag.amWebServiceVersion', 'rowsDisplayed', 'loading', 'isActive', 'pollingEnabled'),

  pollsterOptionsObserver: function () {
    this.set('pollster.options', {
      appID: this.get('vertex.dag.applicationId'),
      dagID: this.get('vertex.dag.idx'),
      counters: this.get('countersDisplayed'),
      attemptID: this.get('rowsDisplayed').map(function (row) {
          var attemptIndex = App.Helpers.misc.getIndexFromId(row.get('id')),
              taskIndex = App.Helpers.misc.getIndexFromId(row.get('taskID')),
              vertexIndex = App.Helpers.misc.getIndexFromId(row.get('vertexID'));
          return '%@_%@_%@'.fmt(vertexIndex, taskIndex, attemptIndex);
        }).join(',')
    });
  }.observes('vertex.dag.applicationId', 'vertex.dag.idx', 'rowsDisplayed'),

  countersDisplayed: function () {
    return App.Helpers.misc.getCounterQueryParam(this.get('columns'));
  }.property('columns'),

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
        return App.Helpers.misc.loadApp(that.store, dag.get('applicationId'));
      }).
      then(function(appDetail) {
        var status = appDetail.get('status');
        if (status) {
          that.set('yarnAppState', status);
        }
      });
    loaders.push(appDetailFetcher);

    return Em.RSVP.allSettled(loaders);
  },

  defaultColumnConfigs: function() {
    var that = this;
    return [
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
        observePath: true,
        onSort: this.onInProgressColumnSort.bind(this),
        getCellContent: function(row) {
          var status = App.Helpers.misc.getFixedupDisplayStatus(row.get('status'));
          return {
            status: status,
            statusIcon: App.Helpers.misc.getStatusClassForEntity(status)
          };
        }
      },
      {
        id: 'progress',
        headerCellName: 'Progress',
        contentPath: 'progress',
        observePath: true,
        onSort: this.onInProgressColumnSort.bind(this),
        templateName: 'components/basic-table/progress-cell'
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
          var cellContent = App.Helpers.misc.constructLogLinks(
                row,
                that.get('yarnAppState'),
                that.get('controllers.task.tezApp.user')
              );

          cellContent.notAvailable = cellContent.viewUrl || cellContent.downloadUrl;
          return cellContent;
        }
      }
    ];
  }.property('yarnAppState', 'controllers.task.tezApp.user'),

});


App.TaskAttemptIndexController = Em.ObjectController.extend(App.ModelRefreshMixin, {
  controllerName: 'TaskAttemptIndexController',

  needs: "taskAttempt",

  taskAttemptStatus: function() {
    return App.Helpers.misc.getFixedupDisplayStatus(this.get('status'));
  }.property('id', 'status'),

  taskAttemptIconStatus: function() {
    return App.Helpers.misc.getStatusClassForEntity(this.get('taskAttemptStatus'));
  }.property('id', 'status', 'counterGroups'),

  load: function () {
    var model = this.get('content');
    if(model && $.isFunction(model.reload)) {
      model.reload().then(function(record) {
        if(record.get('isDirty')) {
          record.rollback();
        }
      });
    }
  },

  logsLink: function() {
    var cellContent = App.Helpers.misc.constructLogLinks(
      this.get('content'),
      this.get('controllers.taskAttempt.appDetail.status'),
      this.get('controllers.taskAttempt.tezApp.user')
    );

    cellContent.notAvailable = cellContent.viewUrl || cellContent.downloadUrl;
    return cellContent;
  }.property('id', 'controllers.taskAttempt'),

});
