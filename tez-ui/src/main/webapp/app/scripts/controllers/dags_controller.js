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
    fromID: true,
    status_filter: 'status',
    user_filter: 'user',
    appId_filter: 'appid',
    dagName_filter: 'dag_name'
  },

  fromID: null,

  status_filter: null,

  user_filter: null,

  appId_filter: null,

  dagName_filter: null,

  fields: 'events,primaryfilters,otherinfo',

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
    fetcher,
    record;
    var defaultErrMsg = 'Error while loading dag info.';

    that.set('loading', true);
    store.unloadAll(childEntityType);
    store.unloadAll('dagProgress');

    store.findQuery(childEntityType, this.getFilterProperties()).then(function(entities){
      var loaders = [];
      entities.forEach(function (dag) {
        var appId = dag.get('applicationId');
        if(appId) {
          // Pivot attempt selection logic
          record = store.getById('appDetail', appId);
          if(record && !App.Helpers.misc.isStatusInUnsuccessful(record.get('appState'))) {
            store.unloadRecord(record);
          }
          fetcher = store.find('appDetail', appId).then(function (app) {
            dag.set('appDetail', app);
            if (dag.get('status') === 'RUNNING') {
              dag.set('status', App.Helpers.misc.getRealStatus(
                dag.get('status'),
                app.get('appState'),
                app.get('finalAppStatus')
              ));
              App.Helpers.misc.removeRecord(store, 'tezApp', 'tez_' + appId);
            }
            return store.find('tezApp', 'tez_' + appId).then(function (app) {
              dag.set('tezApp', app);
            });
          });
          loaders.push(fetcher);
          //Load tezApp details
          if (dag.get('status') === 'RUNNING') {
            App.Helpers.misc.removeRecord(store, 'dagProgress', dag.get('id'));
            amInfoFetcher = store.find('dagProgress', dag.get('id'), {
              appId: dag.get('applicationId'),
              dagIdx: dag.get('idx')
            })
            .then(function(dagProgressInfo) {
              dag.set('progress', dagProgressInfo.get('progress'));
            })
            .catch(function(error) {
              Em.Logger.error('Failed to fetch dagProgress' + error);
            });
            loaders.push(amInfoFetcher);
          }
        }
      });
      Em.RSVP.allSettled(loaders).then(function(){
        that.set('entities', entities);
        that.set('loading', false);
      });
    }).catch(function(error){
      Em.Logger.error(error);
      var err = App.Helpers.misc.formatError(error, defaultErrMsg);
      var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
      App.Helpers.ErrorBar.getInstance().show(msg, err.details);
    });
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
    },
  },

  /*
   * Columns that would be displayed by default
   * @return Array of column configs
   */
  defaultColumnConfigs: function () {
    return [
      {
        id: 'dagName',
        headerCellName: 'Dag Name',
        filterID: 'dagName_filter',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'dag.index' view.cellContent.id class='ember-table-content'}}{{view.cellContent.name}}{{/link-to}}")
        }),
        getCellContent: function(row) {
          return {
            id: row.get('id'),
            name: row.get('name')
          };
        }
      },
      {
        id: 'id',
        headerCellName: 'Id',
        contentPath: 'id'
      },
      {
        id: 'user',
        headerCellName: 'Submitter',
        filterID: 'user_filter',
        contentPath: 'user'
      },
      {
        id: 'status',
        headerCellName: 'Status',
        filterID: 'status_filter',
        filterType: 'dropdown',
        dropdownValues: App.Helpers.misc.dagStatusUIOptions,
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            '<span class="ember-table-content">&nbsp;\
            <i {{bind-attr class=":task-status view.cellContent.statusIcon"}}></i>\
            &nbsp;&nbsp;{{view.cellContent.status}}\
            {{#if view.cellContent.progress}} {{bs-badge content=view.cellContent.progress}}{{/if}}</span>')
        }),
        getCellContent: function(row) {
          var pct;
          if (Ember.typeOf(row.get('progress')) === 'number') {
            pct = App.Helpers.number.fractionToPercentage(row.get('progress'));
          }
          var dagStatus = row.get('status');
          return {
            status: dagStatus,
            statusIcon: App.Helpers.misc.getStatusClassForEntity(dagStatus),
            progress: pct
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
        id: 'appId',
        headerCellName: 'Application ID',
        filterID: 'appId_filter',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#if view.cellContent.enableLink}}\
               {{#link-to 'tez-app' view.cellContent.appId class='ember-table-content'}}{{view.cellContent.appId}}{{/link-to}}\
             {{else}}\
               <span class='ember-table-content'>{{view.cellContent.appId}}</span>\
             {{/if}}")
        }),
        getCellContent: function(row) {
          return  {
            enableLink: row.get('tezApp') && row.get('appDetail'),
            appId: row.get('applicationId')
          }
        }
      },
      {
        id: 'queue',
        headerCellName: 'Queue',
        getCellContent: function(row) {
          return row.get('appDetail.queue') || 'Not Available';
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
  }.property(),

  columns: function() {
    var visibleColumnConfigs = this.get('columnConfigs').filter(function (column) {
      return this.visibleColumnIds[column.id];
    }, this);

    return App.Helpers.misc.createColumnsFromConfigs(visibleColumnConfigs);
  }.property('visibleColumnIds'),

});
