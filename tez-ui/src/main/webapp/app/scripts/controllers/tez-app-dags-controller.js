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

App.TezAppDagsController = Em.ObjectController.extend(App.PaginatedContentMixin, {
  needs: "tezApp",

  // required by the PaginatedContentMixin
  childEntityType: 'dag',

  queryParams: {
    status_filter: 'status',
    user_filter: 'user'
  },
  status_filter: null,
  user_filter: null,

  loadData: function() {
    console.log(new Error().stack);
    var filters = {
      primary: {
        applicationId: this.get('appId'),
        user: this.user_filter
      },
      secondary: {
        status: this.status_filter
      }
    };
    this.setFiltersAndLoadEntities(filters);
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

  /* table view for dags */
  columns: function() {
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
    var idCol = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Dag ID',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          "{{#link-to 'dag' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}")
      }),
      getCellContent: function(row) {
        return row.get('id')
      }
    });
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
        console.log(st, et);
        if (st && et) {
          return App.Helpers.date.durationSummary(st, et);
        }
      }
    });
    return [nameCol, idCol, userCol, statusCol, submittedTimeCol, runTimeCol];
  }.property(),

});