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

App.DagsController = Em.ObjectController.extend(App.PaginatedContentMixin, App.ColumnSelectorMixin, {
  childEntityType: 'dag',

	controllerName: 'DagsController',

	pageTitle: 'Tez DAGs',

	pageSubTitle: 'All Tez DAGs',

	columnSelectorMessage: 'Logs and counter columns needs more time to load.',

  // query parameters supported through url. The same named variables in this controller get
  // bound automatically to the ones defined in the route.
  queryParams: {
    status_filter: 'status',
    user_filter: 'user',
    appId_filter: 'appid',
    id_filter: 'id',
    dagName_filter: 'dag_name',
    callerId_filter: 'caller_id'
  },

  _loadedAllData: false,

  fromID: null,

  status_filter: null,
  user_filter: null,
  appId_filter: null,
  id_filter: null,
  dagName_filter: null,
  callerId_filter: null,

  boundFilterValues: Em.Object.create({
    status: null
  }),
  visibleFilters: null,

  init: function () {
    this._super();
    this._filterVisiblilityObserver();
  },

  _paramObserver: function () {
    this.set('boundFilterValues', Em.Object.create({
      status: this.get('status_filter'),
      user: this.get('user_filter'),
      appId: this.get('appId_filter'),
      id: this.get('id_filter'),
      dagName: this.get('dagName_filter'),
      callerId: this.get('callerId_filter')
    }));
  }.observes('status_filter', 'user_filter', 'appId_filter', 'dagName_filter', 'id_filter',
      'callerId_filter'),

  _otherInfoFieldsVisible: function () {
    var visibleColumns = this.get('visibleColumnIds') || {},
        columnIds;

    if(visibleColumns['logs']) {
      return true;
    }

    columnIds = Object.keys(visibleColumns);
    for(var i = 0, length = columnIds.length, id; i < length; i++) {
      id = columnIds[i];
      if(visibleColumns[id] && id.indexOf('/') != -1) {
        return true;
      }
    }

    return false;
  }.property('visibleColumnIds'),

  _filterVisiblilityObserver: function () {
    var visibleFilters = Em.Object.create();
    this.get('columns').forEach(function (column) {
      if(column.get('enableFilter')) {
        visibleFilters.set(column.get('id'), true);
      }
    });
    this.set('visibleFilters', visibleFilters);
  }.observes('columns'),

  loadData: function() {
    var filters = {
      primary: {
        dagName: this.dagName_filter,
        applicationId: this.appId_filter,
        user: this.user_filter,
        callerId: this.callerId_filter
      },
      secondary: {
      }
    }
    if (App.Helpers.misc.isFinalDagStatus(this.status_filter)) {
      filters.primary['status'] = this.status_filter;
    } else {
      filters.secondary['status'] = this.status_filter;
    }
    this.setFiltersAndLoadEntities(filters);
  },

  loadEntities: function() {
    var that = this,
    store = this.get('store'),
    childEntityType = this.get('childEntityType'),
    finder,
    record;
    var defaultErrMsg = 'Error while loading DAGs. Either Timeline Server is down, or CORS might not be enabled.';

    that.set('loading', true);
    store.unloadAll(childEntityType);
    store.unloadAll('dagProgress');

    that.set('_loadedAllData', false);
    function loadAllData() {
      return store.findQuery(childEntityType, that.getFilterProperties()).then(function (entities) {
        that.set('_loadedAllData', true);
        return entities;
      });
    }

    function setEntities(entities) {
      that.set('entities', entities);
      that.set('loading', false);

      return entities;
    }

    if(that.id_filter) {
      finder = store.find(childEntityType, that.id_filter).then(function (entity) {
        var entities = (
          (that.dagName_filter && entity.get('name') != that.dagName_filter) ||
          (that.appId_filter && entity.get('applicationId') != that.appId_filter) ||
          (that.user_filter && entity.get('user') != that.user_filter) ||
          (that.status_filter && entity.get('status') != that.status_filter) ||
          (that.callerId_filter && entity.get('callerId') != that.callerId_filter)
        ) ? [] : [entity];

        return setEntities(entities);
      });
    }
    else {
      // Query just basic data
      finder = store.findQuery(childEntityType, that.getFilterProperties('events,primaryfilters'));
      finder = finder.then(setEntities).then(function (entities) {

        // If countersVisible lazy load counters
        return that.get('_otherInfoFieldsVisible') ? new Promise(function (fullfill) {
          setTimeout(fullfill, 100); // Lazyload delay
        }).then(loadAllData) : entities;

      }).catch(function () {
        // Basic data query failed, probably YARN-3530 fix is not in ATS. Load all data.
        return loadAllData().then(setEntities);
      });
    }

    finder = finder.then(function(entities){

      entities.forEach(function (dag) {
        var appId = dag.get('applicationId');
        if(appId && dag.get('status') === 'RUNNING') {
          App.Helpers.misc.loadApp(store, appId).then(function (app) {
            dag.set('appDetail', app);
            dag.set('status', App.Helpers.misc.getRealStatus(
              dag.get('status'),
              app.get('status'),
              app.get('finalStatus')
            ));
          }).finally(function (entities) {
            if(dag.get('status') === 'RUNNING') {
              App.Helpers.misc.removeRecord(store, 'dagProgress', dag.get('id'));
              store.find('dagProgress', dag.get('id'), {
                appId: dag.get('applicationId'),
                dagIdx: dag.get('idx')
              })
              .then(function(dagProgressInfo) {
                dag.set('progress', dagProgressInfo.get('progress'));
              })
              .catch(function(error) {
                error.message = "Failed to fetch dagProgress. Application Master (AM) is out of reach. Either it's down, or CORS is not enabled for YARN ResourceManager.";
                Em.Logger.error(error);
                var err = App.Helpers.misc.formatError(error);
                var msg = 'Error code: %@, message: %@'.fmt(err.errCode, err.msg);
                App.Helpers.ErrorBar.getInstance().show(msg, err.details);
              });
            }
          });
        }
      });

    }).catch(function(error){
      Em.Logger.error(error);
      var err = App.Helpers.misc.formatError(error, defaultErrMsg);
      var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
      App.Helpers.ErrorBar.getInstance().show(msg, err.details);
    });
  },

  _onCountersVisible: function () {
    if(this.get('_otherInfoFieldsVisible') && !this.get('_loadedAllData')) {
      Em.run.once(this, this.loadEntities);
    }
  }.observes('_otherInfoFieldsVisible'),

  actions : {
    filterUpdated: function() {
      Em.run.later();
      var filterValues = this.get('boundFilterValues');
      this.setProperties({
        status_filter: filterValues.get('status') || null,
        user_filter: filterValues.get('user') || null,
        appId_filter: filterValues.get('appId') || null,
        id_filter: filterValues.get('id') || null,
        dagName_filter: filterValues.get('dagName') || null,
        callerId_filter: filterValues.get('callerId') || null
      });
      this.loadData();
    }
  },

  /*
   * Columns that would be displayed by default
   * @return Array of column configs
   */
  defaultColumnConfigs: function () {
    var store = this.get('store');

    return [
      {
        id: 'dagName',
        headerCellName: 'Dag Name',
        templateName: 'components/basic-table/linked-cell',
        enableFilter: true,
        getCellContent: function(row) {
          return {
            linkTo: 'dag.index',
            entityId: row.get('id'),
            displayText: row.get('name')
          };
        }
      },
      {
        id: 'id',
        headerCellName: 'Id',
        enableFilter: true,
        contentPath: 'id'
      },
      {
        id: 'user',
        headerCellName: 'Submitter',
        contentPath: 'user',
        enableFilter: true
      },
      {
        id: 'status',
        headerCellName: 'Status',
        templateName: 'components/basic-table/status-cell',
        enableFilter: true,
        contentPath: 'status',
        observePath: true,
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
        id: 'progress',
        headerCellName: 'Progress',
        contentPath: 'progress',
        enableFilter: true,
        observePath: true,
        templateName: 'components/basic-table/progress-cell'
      },
      {
        id: 'startTime',
        headerCellName: 'Start Time',
        contentPath: 'startTime',
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
          return App.Helpers.date.timingFormat(row.get('duration'), 1);
        }
      },
      {
        id: 'appId',
        headerCellName: 'Application ID',
        templateName: 'components/basic-table/linked-cell',
        enableFilter: true,
        getCellContent: function(row) {
          return {
            linkTo: 'tez-app',
            entityId: row.get('applicationId'),
            displayText: row.get('applicationId')
          };
        }
      },
      {
        id: 'queue',
        headerCellName: 'Queue',
        templateName: 'components/basic-table/bounded-basic-cell',
        getCellContent: function(row) {
          var appId = row.get('applicationId');
          if(appId) {
            return App.Helpers.misc.loadApp(store, appId, true).then(function (app) {
              return app.get('queue');
            }).catch(function(error) {});
          }
        }
      },
      {
        id: 'callerId',
        headerCellName: 'Context ID',
        enableFilter: true,
        contentPath: 'callerId'
      },
      {
        id: 'logs',
        headerCellName: 'Logs',
        templateName: 'components/basic-table/multi-logs-cell',
        contentPath: 'containerLogs',
        observePath: true,
        getCellContent: function(row) {
          var containerLogs = row.get('containerLogs');
          return containerLogs ? {
            logs: containerLogs
          } : {
            isPending: true
          };
        }
      }
    ];
  }.property(),

  columnConfigs: function() {
    return this.get('defaultColumnConfigs').concat(
      App.Helpers.misc.normalizeCounterConfigs(
        App.get('Configs.defaultCounters').concat(
          App.get('Configs.tables.entity.dag') || [],
          App.get('Configs.tables.sharedColumns') || []
        )
      )
    );
  }.property('defaultColumnConfigs'),

});
