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

App.TaskAttemptsController = Em.ObjectController.extend(App.PaginatedContentMixin, App.ColumnSelectorMixin, {
  // required by the PaginatedContentMixin
  childEntityType: 'task_attempt',

  controllerName: 'TaskAttemptsController',

  needs: 'task',

  queryParams: {
    status_filter: 'status'
  },
  status_filter: null,

  loadData: function() {
    var filters = {
      primary: {
        TEZ_TASK_ID: this.get('controllers.task.id')
      },
      secondary: {
        status: this.status_filter
      }
    }
    this.setFiltersAndLoadEntities(filters);
  },

  loadEntities: function () {
    var that = this;
    var childEntityType = this.get('childEntityType');
    var defaultErrMsg = 'Error while loading %@. could not connect to %@'
      .fmt(childEntityType, App.env.timelineBaseUrl);

    that.set('loading', true);

    this.get('store').unloadAll(childEntityType);
    this.get('store').findQuery(childEntityType, this.getFilterProperties()).then(function(entities){
      that.set('entities', entities);
      var loaders = [];
      try {
        var loader = Em.tryInvoke(that, 'loadAdditional');
        if (!!loader) {
          loaders.push(loader);
        }
      } catch(error) {
        Em.Logger.error("Exception invoking additional load", error);
      }

      var appDetailFetcher = that.store.find('dag', that.get('controllers.task.dagID')).
        then(function (dag) {
          return that.store.find('appDetail', dag.get('applicationId'));
        }).
        then(function(appDetail) {
          var appState = appDetail.get('appState');
          if (appState) {
            that.set('yarnAppState', appState);
          }
        });
      loaders.push(appDetailFetcher);
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
    var that = this;
    return [
      {
        id: 'id',
        headerCellName: 'Attempt Index',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'taskAttempt' view.cellContent.id class='ember-table-content'}}{{view.cellContent.displayId}}{{/link-to}}")
        }),
        getCellContent: function (row) {
          var id = row.get('id'),
              idPrefix = 'attempt_%@_'.fmt(row.get('dagID').substr(4));
          return {
            id: id,
            displayId: id.indexOf(idPrefix) == 0 ? id.substr(idPrefix.length) : id
          };
        }
      },
      {
        id: 'attemptNo',
        headerCellName: 'Attempt No',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#link-to 'taskAttempt' view.cellContent.attemptID class='ember-table-content'}}{{view.cellContent.attemptNo}}{{/link-to}}")
        }),
        getCellContent: function(row) {
          var attemptID = row.get('id') || '',
              attemptNo = attemptID.split(/[_]+/).pop();
          return {
            attemptNo: attemptNo,
            attemptID: attemptID
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
          return {
            status: row.get('status'),
            statusIcon: App.Helpers.misc.getStatusClassForEntity(row)
          };
        }
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
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            '<span class="ember-table-content">\
            {{#link-to "taskAttempt.counters" view.cellContent}}counters{{/link-to}}&nbsp;\
            </span>'
            )
        }),
        contentPath: 'id'
      },
      {
        id: 'logs',
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
          var yarnAppState = that.get('yarnAppState'),
              suffix = "/syslog_" + row.get('id'),
              link = row.get('inProgressLog') || row.get('completedLog'),
              cellContent = {};

          if(link) {
            cellContent.viewUrl = link + suffix;
          }
          link = row.get('completedLog');
          if (link && yarnAppState === 'FINISHED' || yarnAppState === 'KILLED' || yarnAppState === 'FAILED') {
            cellContent.downloadUrl = link + suffix;
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
          App.get('Configs.tables.entity.taskAttempt') || [],
          App.get('Configs.tables.sharedColumns') || []
        )
      )
    );
  }.property(),

});


App.TaskAttemptIndexController = Em.ObjectController.extend({
  controllerName: 'TaskAttemptIndexController',

  taskIconStatus: function() {
    return App.Helpers.misc.getStatusClassForEntity(this.get('model'));
  }.property('id', 'status', 'counterGroups'),

});
