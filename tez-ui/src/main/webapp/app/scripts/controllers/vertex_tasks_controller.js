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

App.VertexTasksController = Em.ObjectController.extend(App.PaginatedContentMixin, {
  // Required by the PaginatedContentMixin
  childEntityType: 'task',

  needs: 'vertex',

  queryParams: {
    status_filter: 'status',
  },
  status_filter: null,

  loadData: function() {
    var filters = {
      primary: {
        TEZ_VERTEX_ID: this.get('controllers.vertex.id')
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
    fetcher;
    childEntityType = this.get('childEntityType');

    store.unloadAll(childEntityType);
    store.findQuery(childEntityType, this.getFilterProperties()).then(function(entities){
      that.set('entities', entities);
      var pivotLoaders = [];
      entities.forEach(function (task) {
        var taskAttemptId = task.get('successfulAttemptId') || task.get('attempts').lastObject;
        if (!!taskAttemptId){
          // Pivot attempt selection logic
          fetcher = store.find('taskAttempt',  taskAttemptId);
          fetcher.then(function (attempt) {
            task.set('pivotAttempt', attempt);
          });
          pivotLoaders.push(fetcher);
        }
      });
      Em.RSVP.allSettled(pivotLoaders).then(function(){
        that.set('loading', false);
      });
    }).catch(function(jqXHR){
      if(console) console.log(jqXHR);
      alert('failed');
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

  columns: function() {
    var idCol = App.ExTable.ColumnDefinition.create({
      headerCellName: 'Task ID',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          "{{#link-to 'task' view.cellContent class='ember-table-content'}}{{view.cellContent}}{{/link-to}}")
      }),
      contentPath: 'id',
    });

    var startTimeCol = App.ExTable.ColumnDefinition.create({
      headerCellName: 'Start Time',
      getCellContent: function(row) {
        return App.Helpers.date.dateFormat(row.get('startTime'));
      }
    });

    var endTimeCol = App.ExTable.ColumnDefinition.create({
      headerCellName: 'End Time',
      getCellContent: function(row) {
        return App.Helpers.date.dateFormat(row.get('endTime'));
      }
    });

    var durationCol = App.ExTable.ColumnDefinition.create({
      headerCellName: 'duration',
      getCellContent: function(row) {
        var st = row.get('startTime');
        var et = row.get('endTime');
        if (st && et) {
          return App.Helpers.date.durationSummary(st, et);
        }
      }
    });

    var statusCol = App.ExTable.ColumnDefinition.createWithMixins(App.ExTable.FilterColumnMixin,{
      headerCellName: 'Status',
      filterID: 'status_filter',
      filterType: 'dropdown',
      dropdownValues: App.Helpers.misc.taskStatusUIOptions,
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

    var actionsCol = App.ExTable.ColumnDefinition.create({
      headerCellName: 'Actions',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          '<span class="ember-table-content">\
          {{#link-to "task.counters" view.cellContent}}counters{{/link-to}}&nbsp;\
          {{#link-to "task.attempts" view.cellContent}}attempts{{/link-to}}\
          </span>'
          )
      }),
      contentPath: 'id'
    });

    var logs = App.ExTable.ColumnDefinition.create({
      textAlign: 'text-align-left',
      headerCellName: 'Logs',
      tableCellViewClass: Em.Table.TableCell.extend({
        template: Em.Handlebars.compile(
          '<span class="ember-table-content">\
            {{#unless view.cellContent}}\
              Not Available\
            {{else}}\
              <a href="//{{unbound view.cellContent}}">View</a>\
              &nbsp;\
              <a href="//{{unbound view.cellContent}}?start=0" download target="_blank" type="application/octet-stream">Download</a>\
            {{/unless}}\
          </span>')
      }),
      getCellContent: function(row) {
        var attempt = row.get('pivotAttempt');
        var logFile = attempt && (attempt.get('inProgressLog') || attempt.get('completedLog'));
        if(logFile) logFile += "/syslog_" + attempt.get('id');
        return logFile;
      }
    });

    return [idCol, startTimeCol, endTimeCol, durationCol, statusCol, actionsCol, logs];
  }.property(),
});