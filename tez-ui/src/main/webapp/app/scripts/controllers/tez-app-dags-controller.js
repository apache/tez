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

App.TezAppDagsController = Em.ObjectController.extend(App.PaginatedContentMixin, App.ColumnSelectorMixin, {
  needs: "tezApp",

  controllerName: 'TezAppDagsController',

  // required by the PaginatedContentMixin
  childEntityType: 'dag',

  queryParams: {
    dagName_filter: 'dagName',
    status_filter: 'status',
    user_filter: 'user'
  },
  dagName_filter: null,
  status_filter: null,
  user_filter: null,

  loadData: function() {
    var filters = {
      primary: {
        applicationId: this.get('appId'),
      },
      secondary: {
        user: this.user_filter,
        status: this.status_filter,
        dagName: this.dagName_filter
      }
    };
    this.setFiltersAndLoadEntities(filters);
  },

  loadEntities: function() {
    var that = this;
    var childEntityType = this.get('childEntityType');
    var defaultErrMsg = 'Error while loading %@. could not connect to %@'
      .fmt(childEntityType, App.env.timelineBaseUrl);


    that.set('loading', true);

    this.get('store').unloadAll(childEntityType);
    this.get('store').findQuery(childEntityType, this.getFilterProperties()).then(function(entities){
      that.set('entities', entities);

      var loaders = [];
      entities.forEach(function (dag) {
        if (dag.get('status') === 'RUNNING') {
          amInfoFetcher = that.store.find('dagProgress', dag.get('id'), {
            appId: dag.get('applicationId'),
            dagIdx: dag.get('idx')
          }).then(function(dagProgressInfo) {
              dag.set('progress', dagProgressInfo.get('progress'));
          }).catch(function(error) {
            Em.Logger.error('Failed to fetch dagProgress' + error);
          });
          loaders.push(amInfoFetcher);
        }
      });
      Em.RSVP.allSettled(loaders).then(function(){
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
    }
  },

  defaultColumnConfigs: function() {
    return [
      {
        id: 'dagName',
        headerCellName: 'Dag Name',
        filterID: 'dagName_filter',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'dag' view.cellContent.id class='ember-table-content'}}{{view.cellContent.name}}{{/link-to}}")
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
          return {
            status: row.get('status'),
            statusIcon: App.Helpers.misc.getStatusClassForEntity(row),
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

});
