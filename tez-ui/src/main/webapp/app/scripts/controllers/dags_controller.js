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

App.DagsController = Em.ObjectController.extend(App.PaginatedContentMixin, {
  childEntityType: 'dag',

	controllerName: 'DagsController',

	pageTitle: 'Dags',

	pageSubTitle: 'All Dags',

  // query parameters supported through url. The same named variables in this controller get
  // bound automatically to the ones defined in the route.
  queryParams: {
    count: true,
    fromID: true,
    status_filter: 'status',
    user_filter: 'user'
  },

  // paging related values. These are bound automatically to the values in url. via the queryParams
  // defined in the route. 
  count: 10,

  fromID: null,

  status_filter: null,

  user_filter: null,

  fields: 'events,primaryfilters,otherinfo',

  // The dropdown contents for number of items to show.
  countOptions: [5, 10, 25, 50, 100],

  loadData: function() {
    var filters = {
      primary: {
        user: this.user_filter
      },
      secondary: {
        status: this.status_filter
      }
    }
    this.setFiltersAndLoadEntities(filters);
  },

  loadEntities: function() {
    var that = this,
    store = this.get('store'),
    childEntityType = this.get('childEntityType'),
    fetcher;

    store.unloadAll(childEntityType);
    store.findQuery(childEntityType, this.getFilterProperties()).then(function(entities){
      var loaders = [];
      that.set('entities', entities);
      entities.forEach(function (dag) {
        // Pivot attempt selection logic
        fetcher = store.find('appDetail', dag.get('applicationId') );
        fetcher.then(function (app) {
          dag.set('app', app);
        });
        loaders.push(fetcher);
      });
      Em.RSVP.allSettled(loaders).then(function(){
        that.set('loading', false);
      });
    }).catch(function(jqXHR){
      alert('failed');
    });
  },

  countUpdated: function() {
    this.loadData();
  }.observes('count'),

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

	/* table view for dags */
  columns: function() {
    var store = this.get('store');
    var columnHelper = function(columnName, valName) {
      return App.ExTable.ColumnDefinition.create({
        textAlign: 'text-align-left',
        headerCellName: columnName,
        contentPath: valName
      });
    }

    var nameCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Dag Name',
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
    });
    var idCol = columnHelper('Dag ID', 'id');
    var userCol = App.ExTable.ColumnDefinition.createWithMixins(App.ExTable.FilterColumnMixin, {
      textAlign: 'text-align-left',
      headerCellName: 'Submitter',
      filterID: 'user_filter',
      contentPath: 'user'
    }); 
    var statusCol = App.ExTable.ColumnDefinition.createWithMixins(App.ExTable.FilterColumnMixin,{
      textAlign: 'text-align-left',
      headerCellName: 'Status',
      filterID: 'status_filter',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          '<span class="ember-table-content">&nbsp;\
          <i {{bind-attr class=":task-status view.cellContent.statusIcon"}}></i>\
          &nbsp;&nbsp;{{view.cellContent.status}}</span>')
      }),
      getCellContent: function(row) {
      	return { 
          status: row.get('status'),
          statusIcon: App.Helpers.misc.getStatusClassForEntity(row)
        };
      }
    });
    var submittedTimeCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Submitted Time',
      getCellContent: function(row) {
        return App.Helpers.date.dateFormat(row.get('submittedTime'));
      }
    });
    var runTimeCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Run Time',
      getCellContent: function(row) {
        var st = row.get('startTime');
        var et = row.get('endTime');
        if (st && et) {
          return App.Helpers.date.durationSummary(st, et);
        }
      }
    });
    var appIdCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Application ID',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          "{{#link-to 'tez-app' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}")
      }),
      getCellContent: function(row) {
        return  row.get('applicationId')
      }
    });
    var queue = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Queue',
      getCellContent: function(row) {
        return (row.get('app') && row.get('app').get('queue')) || 'Not Available';
      }
    });
    return [nameCol, idCol, userCol, statusCol, submittedTimeCol, runTimeCol, appIdCol, queue];
  }.property(),


});