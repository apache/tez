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

  // query parameters supported through url. The same named variables in this controller get
  // bound automatically to the ones defined in the route.
  queryParams: {
    status_filter: 'status',
    user_filter: 'user',
    appId_filter: 'appid',
    id_filter: 'id',
    dagName_filter: 'dag_name'
  },

  fromID: null,

  status_filter: null,
  user_filter: null,
  appId_filter: null,
  id_filter: null,
  dagName_filter: null,

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
      dagName: this.get('dagName_filter')
    }));
  }.observes('status_filter', 'user_filter', 'appId_filter', 'dagName_filter', 'id_filter'),

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
        user: this.user_filter
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
    var defaultErrMsg = 'Error while loading dag info.';

    that.set('loading', true);
    store.unloadAll(childEntityType);
    store.unloadAll('dagProgress');

    if(this.id_filter) {
      finder = store.find(childEntityType, this.id_filter).then(function (entity) {
        return (
          (that.dagName_filter && entity.get('name') != that.dagName_filter) ||
          (that.appId_filter && entity.get('applicationId') != that.appId_filter) ||
          (that.user_filter && entity.get('user') != that.user_filter) ||
          (that.status_filter && entity.get('status') != that.status_filter)
        ) ? [] : [entity];
      }).catch(function () {
        return [];
      });
    }
    else {
      finder = store.findQuery(childEntityType, this.getFilterProperties());
    }

    finder.then(function(entities){
      that.set('entities', entities);
      that.set('loading', false);

      entities.forEach(function (dag) {
        var appId = dag.get('applicationId');
        if(appId) {
          record = store.getById('appDetail', appId);
          if(record && !App.Helpers.misc.isStatusInUnsuccessful(record.get('appState'))) {
            store.unloadRecord(record);
          }
        }
      });
      entities.forEach(function (dag) {
        var appId = dag.get('applicationId');
        if(appId) {
          store.find('appDetail', appId).then(function (app) {
            dag.set('appDetail', app);
            if (dag.get('status') === 'RUNNING') {
              dag.set('status', App.Helpers.misc.getRealStatus(
                dag.get('status'),
                app.get('appState'),
                app.get('finalAppStatus')
              ));
            }
          })
          .catch(function(error) {
            Em.Logger.error('Failed to fetch appDetail' + error);
          });

          if (dag.get('status') === 'RUNNING') {
            App.Helpers.misc.removeRecord(store, 'dagProgress', dag.get('id'));
            store.find('dagProgress', dag.get('id'), {
              appId: dag.get('applicationId'),
              dagIdx: dag.get('idx')
            })
            .then(function(dagProgressInfo) {
              dag.set('progress', dagProgressInfo.get('progress'));
            })
            .catch(function(error) {
              Em.Logger.error('Failed to fetch dagProgress' + error);
            });
          }
        }
      });
    }).catch(function(error){
      Em.Logger.error(error);
      var err = App.Helpers.misc.formatError(error, defaultErrMsg);
      var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
      App.Helpers.ErrorBar.getInstance().show(msg, err.details);
    });
  }.observes('fields'),

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

    function onProgressChange() {
      var progress = this.get('dag.progress'),
          pct;
      if (Ember.typeOf(progress) === 'number') {
        pct = App.Helpers.number.fractionToPercentage(progress);
        this.set('progress', pct);
      }
    }

    function onStatusChange() {
      var status = this.get('dag.status');
      this.setProperties({
        status: status,
        statusIcon: App.Helpers.misc.getStatusClassForEntity(status,
          this.get('dag.hasFailedTaskAttempts'))
      });
    }

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
        getCellContent: function(row) {
          var status = row.get('status'),
              content = Ember.Object.create({
                dag: row,
                status: status,
                statusIcon: App.Helpers.misc.getStatusClassForEntity(status,
                  row.get('hasFailedTaskAttempts'))
              });

          if(status == 'RUNNING') {
            row.addObserver('progress', content, onProgressChange);
            row.addObserver('status', content, onStatusChange);
          }

          return content;
        }
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
          var appId = row.get('applicationId');
          if(appId) {
            return Em.RSVP.allSettled([
              store.find('appDetail', appId),
              store.find('tezApp', 'tez_' + appId)
            ]).then(function (response) {
              var content = {
                displayText: row.get('applicationId'),
                entityId: row.get('applicationId')
              };
              if(response.get('0.value') && response.get('1.value')) {
                content.linkTo = 'tez-app';
              }
              return content;
            });
          }
        }
      },
      {
        id: 'queue',
        headerCellName: 'Queue',
        templateName: 'components/basic-table/bounded-basic-cell',
        getCellContent: function(row) {
          var appId = row.get('applicationId');
          if(appId) {
            return store.find('appDetail', appId).then(function (app) {
              return app.get('queue');
            });
          }
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
