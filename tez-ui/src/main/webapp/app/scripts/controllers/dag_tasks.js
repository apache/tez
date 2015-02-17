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

App.DagTasksController = Em.ObjectController.extend(App.PaginatedContentMixin, App.ColumnSelectorMixin, {
  needs: "dag",

  controllerName: 'DagTasksController',

  // required by the PaginatedContentMixin
  childEntityType: 'task',

  queryParams: {
    status_filter: 'status',
    vertex_id_filter: 'vertex_id',
  },
  status_filter: null,
  vertex_id_filter: null,

  loadData: function() {
    var primaryFilter;
    if (!!this.vertex_id_filter) {
      primaryFilter = { TEZ_VERTEX_ID : this.vertex_id_filter };
    } else {
      primaryFilter = { TEZ_DAG_ID : this.get('controllers.dag.id') };
    }

    var filters = {
      primary: primaryFilter,
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
    var defaultErrMsg = 'Error while loading tasks. could not connect to %@'
      .fmt(App.env.timelineBaseUrl);

    store.unloadAll(childEntityType);
    store.findQuery(childEntityType, this.getFilterProperties())
      .then(function(entities){

      var pivotLoaders = [];
      var dagStatus = that.get('controllers.dag.status');
      entities.forEach(function (task) {
        var taskStatus = App.Helpers.misc
          .getFixedupDisplayStatus(task.get('status'));
        if (taskStatus == 'RUNNING' &&
          App.Helpers.misc.isStatusInUnsuccessful(dagStatus)) {
          taskStatus = 'KILLED'
        }
        if (taskStatus != task.get('status')) {
          task.set('status', taskStatus);
        }
        var taskAttemptId = task.get('successfulAttemptId') ||
            task.get('attempts.lastObject');
        if (!!taskAttemptId) {
          // Pivot attempt selection logic
          fetcher = store.find('taskAttempt', taskAttemptId);
          fetcher.then(function (attempt) {
            task.set('pivotAttempt', attempt);
          });
          pivotLoaders.push(fetcher);
        }
      });
      Em.RSVP.allSettled(pivotLoaders).then(function(){
        that.set('entities', entities);
        that.set('loading', false);
      });
    }).catch(function(error){
      Em.Logger.error(error);
      var err = App.Helpers.misc.formatError(error, defaultErrMsg);
      var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
      App.Helpers.ErrorBar.getInstance().show(msg, error.details);
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
    var that = this;
    return [
      {
        id: 'id',
        headerCellName: 'Task Index',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'task' view.cellContent.id class='ember-table-content'}}{{view.cellContent.displayId}}{{/link-to}}")
        }),
        getCellContent: function (row) {
          var id = row.get('id'),
              idPrefix = 'task_%@_'.fmt(row.get('dagID').substr(4));
          return {
            id: id,
            displayId: id.indexOf(idPrefix) == 0 ? id.substr(idPrefix.length) : id
          };
        }
      },
      {
        id: 'vertexID',
        headerCellName: 'Vertex ID',
        filterID: 'vertex_id_filter',
        contentPath: 'vertexID'
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
        id: 'status',
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
          var status = row.get('status');
          return {
            status: status,
            statusIcon: App.Helpers.misc.getStatusClassForEntity(status)
          };
        }
      },
      {
        id: 'actions',
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
      },
      {
        id: 'logs',
        textAlign: 'text-align-left',
        headerCellName: 'Logs',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            '<span class="ember-table-content">\
              {{#unless view.cellContent.notAvailable}}\
                Not Available\
              {{else}}\
                {{#if view.cellContent.viewUrl}}\
                  <a target="_blank" href="//{{unbound view.cellContent.viewUrl}}">View</a>\
                  &nbsp;\
                {{/if}}\
                {{#if view.cellContent.downloadUrl}}\
                  <a target="_blank" href="{{unbound view.cellContent.downloadUrl}}?start=0" download type="application/octet-stream">Download</a>\
                {{/if}}\
              {{/unless}}\
            </span>')
        }),
        getCellContent: function(row) {
          var yarnAppState = that.get('controllers.dag.yarnAppState'),
              attempt = row.get('pivotAttempt'),
              suffix,
              link,
              cellContent = {};

          if(attempt) {
            suffix = "/syslog_" + attempt.get('id'),
            link = attempt.get('inProgressLog') || attempt.get('completedLog');

            if(link) {
              cellContent.viewUrl = link + suffix;
            }
            link = attempt.get('completedLog');
            if (link && yarnAppState === 'FINISHED' || yarnAppState === 'KILLED' || yarnAppState === 'FAILED') {
              cellContent.downloadUrl = link + suffix;
            }
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
          App.get('Configs.tables.entity.task') || [],
          App.get('Configs.tables.sharedColumns') || []
        )
      )
    );
  }.property(),

});
