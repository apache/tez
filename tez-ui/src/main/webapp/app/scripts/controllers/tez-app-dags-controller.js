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

App.TezAppDagsController = App.TablePageController.extend({

  controllerName: 'TezAppDagsController',
  needs: "tezApp",

  entityType: 'dag',
  filterEntityType: 'tezApp',
  filterEntityId: Ember.computed.alias('appId'),

  afterLoad: function () {
    var data = this.get('data'),
        loaders = [],
        store = this.get('store'),
        record,
        fetcher;

    data.forEach(function (dag) {

      var appId = dag.get('applicationId');
      if(appId) {
        //Load tezApp details
        if (dag.get('status') === 'RUNNING') {
          App.Helpers.misc.removeRecord(store, 'dagProgress', dag.get('id'));
          fetcher = store.find('dagProgress', dag.get('id'), {
            appId: dag.get('applicationId'),
            dagIdx: dag.get('idx')
          })
          .then(function(dagProgressInfo) {
            dag.set('progress', dagProgressInfo.get('progress'));
          })
          .catch(function(error) {
            Em.Logger.error('Failed to fetch dagProgress' + error);
          });
          loaders.push(fetcher);
        }
      }

    });

    return Em.RSVP.allSettled(loaders);
  },

  defaultColumnConfigs: function() {
    var store = this.get('store');
    return [
      {
        id: 'dagName',
        headerCellName: 'Dag Name',
        filterID: 'dagName_filter',
        templateName: 'components/basic-table/linked-cell',
        contentPath: 'name',
        getCellContent: function(row) {
          return {
            linkTo: 'dag',
            entityId: row.get('id'),
            displayText: row.get('name')
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
        contentPath: 'user'
      },
      {
        id: 'status',
        headerCellName: 'Status',
        templateName: 'components/basic-table/status-cell',
        contentPath: 'status',
        getCellContent: function(row) {
          var status = row.get('status'),
              content = Ember.Object.create({
                status: status,
                statusIcon: App.Helpers.misc.getStatusClassForEntity(status)
              });

          if(status == 'RUNNING') {
            App.Helpers.misc.removeRecord(store, 'dagProgress', row.get('id'));

            store.find('dagProgress', row.get('id'), {
              appId: row.get('applicationId'),
              dagIdx: row.get('idx')
            })
            .then(function(dagProgressInfo) {
              content.set('progress', dagProgressInfo.get('progress'));
            })
            .catch(function(error) {
              Em.Logger.error('Failed to fetch dagProgress' + error);
            });
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
        }
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
