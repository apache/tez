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

App.DagIndexController = App.TablePageController.extend({
  controllerName: 'DagIndexController',
  needs: "dag",

  entityType: 'dagVertex',
  filterEntityType: 'dag',
  filterEntityId: Ember.computed.alias('controllers.dag.id'),

  cacheDomain: Ember.computed.alias('controllers.dag.id'),

  pollingType: 'vertexInfo',

  _isRunning: false,
  _autoReloaded: false,

  init: function () {
    this._super();
    this.set('pollster.mergeProperties', ['progress', 'status', 'runningTasks', 'pendingTasks',
      'sucessfulTasks', 'failedTaskAttempts', 'killedTaskAttempts']);
  },

  reset: function () {
    this._super();
    this.set('data', null);
    this.set('_autoReloaded', false);
  },

  _statusObserver: function () {
    var rowsDisplayed,
        isRunning = false;

    if(this.get('status') == 'RUNNING') {
      isRunning = true;
    }
    else if(rowsDisplayed = this.get('rowsDisplayed')){
      rowsDisplayed.forEach(function (row) {
        if(row.get('status') == 'RUNNING') {
          isRunning = true;
        }
      });
    }

    this.set('_isRunning', isRunning);
  }.observes('status', 'rowsDisplayed.@each.status'),

  pollingObserver: function () {
    if(this.get('pollingEnabled')) {
      this.set('_autoReloaded', false);
    }
  }.observes('pollingEnabled'),

  pollsterControl: function () {
    if(this.get('_isRunning') &&
        this.get('amWebServiceVersion') != '1' &&
        !this.get('loading') &&
        this.get('isActive') &&
        this.get('pollingEnabled') &&
        this.get('rowsDisplayed.length') > 0) {
      this.get('pollster').start(!this.get('_autoReloaded'));
    }
    else {
      this.get('pollster').stop();
    }
  }.observes('_isRunning', 'amWebServiceVersion', 'loading', 'isActive', 'pollingEnabled', 'rowsDisplayed'),

  parentStatusObserver: function () {
    var parentStatus = this.get('status'),
        previousStatus = this.get('parentStatus');

    if(parentStatus != previousStatus && previousStatus == 'RUNNING' && this.get('pollingEnabled')) {
      this.get('pollster').stop();
      this.set('_autoReloaded', true);
      this.loadData(true);
    }
    this.set('parentStatus', parentStatus);
  }.observes('status'),

  applicationComplete: function () {
    this.set('_autoReloaded', true);
    this._super();
    if(this.get('controllers.dag.status') == 'SUCCEEDED') {
      this.set('controllers.dag.progress', 1);
    }
  },

  pollsterOptionsObserver: function () {
    this.set('pollster.options', {
      appID: this.get('applicationId'),
      dagID: this.get('idx')
    });
  }.observes('applicationId', 'idx').on('init'),

  progressDetails: null,

  progressObserver: function () {
    var vertexInfoContent = this.get('pollster.polledRecords.content'),
        progressDetails = null,
        succeededTasks = null,
        totalTasks = null,
        completedVertices = null;

    if(vertexInfoContent && vertexInfoContent.length) {
      liveData = vertexInfoContent,
      succeededTasks = 0,
      totalTasks = 0,
      completedVertices = 0;

      liveData.forEach(function (vertex) {
        succeededTasks += parseInt(vertex.get('sucessfulTasks'));
        totalTasks += parseInt(vertex.get('numTasks'));
        if(vertex.get('progress') >= 1) {
          completedVertices++;
        }
      });

      progressDetails = {
        succeededTasks: succeededTasks,
        completedVertices: completedVertices,
        totalTasks: totalTasks
      };
    }

    this.set('progressDetails', progressDetails);
  }.observes('pollster.polledRecords'),

  dataObserver: function () {
    var data = this.get('data.content');
    this.set('rowsDisplayed', data ? data.slice(0) : null);
  }.observes('data'),

  beforeLoad: function () {
    var dagController = this.get('controllers.dag'),
        model = dagController.get('model');
    return model.reload().then(function () {
      return dagController.loadAdditional(model);
    });
  },

  afterLoad: function () {
    var data = this.get('data'),
        runningVerticesIdx,
        isUnsuccessfulDag = App.Helpers.misc.isStatusInUnsuccessful(
          this.get('controllers.dag.status')
        );

    if(isUnsuccessfulDag) {
      data.filterBy('status', 'RUNNING').forEach(function (vertex) {
        vertex.set('status', 'KILLED');
      });
    }

    if (this.get('controllers.dag.amWebServiceVersion') == '1') {
      this._loadProgress(data);
    }

    return this._super();
  },

  // Load progress in parallel for v1 version of the api
  _loadProgress: function (vertices) {
    var that = this,
        runningVerticesIdx = vertices
      .filterBy('status', 'RUNNING')
      .map(function(item) {
        return item.get('id').split('_').splice(-1).pop();
      });

    if (runningVerticesIdx.length > 0) {
      this.store.unloadAll('vertexProgress');
      this.store.findQuery('vertexProgress', {
        metadata: {
          appId: that.get('applicationId'),
          dagIdx: that.get('idx'),
          vertexIds: runningVerticesIdx.join(',')
        }
      }).then(function(vertexProgressInfo) {
          App.Helpers.emData.mergeRecords(
            that.get('rowsDisplayed'),
            vertexProgressInfo,
            ['progress']
          );
      }).catch(function(error) {
        Em.Logger.debug("failed to fetch vertex progress")
      });
    }
  },

  defaultColumnConfigs: function() {
    var vertexIdToNameMap = this.get('vertexIdToNameMap');

    return App.Helpers.misc.createColumnDescription([
      {
        id: 'vertexName',
        headerCellName: 'Vertex Name',
        templateName: 'components/basic-table/linked-cell',
        contentPath: 'name',
        getCellContent: function(row) {
          return {
            linkTo: 'vertex',
            entityId: row.get('id'),
            displayText: vertexIdToNameMap[row.get('id')]
          };
        }
      },
      {
        id: 'status',
        headerCellName: 'Status',
        templateName: 'components/basic-table/status-cell',
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
        observePath: true,
        templateName: 'components/basic-table/progress-cell'
      },
      {
        id: 'totalTasks',
        headerCellName: 'Total Tasks',
        contentPath: 'numTasks',
        observePath: true,
      },
      {
        id: 'succeededTasks',
        headerCellName: 'Succeeded Tasks',
        contentPath: 'sucessfulTasks',
        observePath: true,
      },
      {
        id: 'runningTasks',
        headerCellName: 'Running Tasks',
        contentPath: 'runningTasks',
        observePath: true,
      },
      {
        id: 'pendingTasks',
        headerCellName: 'Pending Tasks',
        contentPath: 'pendingTasks',
        observePath: true,
      },
      {
        id: 'failedTasks',
        headerCellName: 'Failed Task Attempts',
        contentPath: 'failedTaskAttempts',
        observePath: true,
      },
      {
        id: 'killedTasks',
        headerCellName: 'Killed Task Attempts',
        contentPath: 'killedTaskAttempts',
        observePath: true,
      }
    ]);
  }.property('vertexIdToNameMap'),

  actions: {
    downloadDagJson: function() {
      var dagID = this.get('id');
      var downloader = App.Helpers.misc.downloadDAG(this.get('id'), {
        batchSize: 500,
        onSuccess: function() {
          Bootstrap.ModalManager.close('downloadModal');
        },
        onFailure: function() {
          $('#modalMessage').html('<i class="fa fa-lg fa-exclamation-circle margin-small-horizontal" ' +
          'style="color:red"></i>&nbsp;Error downloading data');
        }
      });
      this.set('tmpDownloader', downloader);
      var modalDialogView = Ember.View.extend({
        template: Em.Handlebars.compile(
          '<p id="modalMessage"><i class="fa fa-lg fa-spinner fa-spin margin-small-horizontal" ' +
          'style="color:green"></i>Downloading data for dag %@</p>'.fmt(dagID)
        )
      });
      var buttons = [
        Ember.Object.create({title: 'Cancel', dismiss: 'modal', clicked: 'cancelDownload'})
      ];
      Bootstrap.ModalManager.open('downloadModal', 'Download data',
        modalDialogView, buttons, this);
    },

    cancelDownload: function() {
      var currentDownloader = this.get('tmpDownloader');
      if (!!currentDownloader) {
        currentDownloader.cancel();
      }
      this.set('tmpDownloader', undefined);
    }
  },

  dagRunning: function () {
    var progress = this.get('dagProgress');
    return progress != null && progress < 1;
  }.property('dagProgress'),

  taskIconStatus: function() {
    return App.Helpers.misc.getStatusClassForEntity(this.get('model.status'),
      this.get('hasFailedTaskAttempts'));
  }.property('id', 'model.status', 'hasFailedTaskAttempts'),

  progressStr: function() {
    var pct;
    if (Ember.typeOf(this.get('progress')) === 'number' && this.get('status') == 'RUNNING') {
      pct = App.Helpers.number.fractionToPercentage(this.get('progress'));
    }
    return pct;
  }.property('id', 'status', 'progress'),

  totalTasks: function() {
    return App.Helpers.misc.getCounterValueForDag(this.get('counterGroups'),
      this.get('id'), 'org.apache.tez.common.counters.DAGCounter', 'TOTAL_LAUNCHED_TASKS')
  }.property('id', 'counterGroups'),

  sucessfulTasks: function() {
    return App.Helpers.misc.getCounterValueForDag(this.get('counterGroups'), this.get('id'),
      'org.apache.tez.common.counters.DAGCounter', 'NUM_SUCCEEDED_TASKS')
  }.property('id', 'counterGroups'),

  failedTasks: function() {
    return App.Helpers.misc.getCounterValueForDag(this.get('counterGroups'), this.get('id'),
      'org.apache.tez.common.counters.DAGCounter', 'NUM_FAILED_TASKS')
  }.property('id', 'counterGroups'),

  killedTasks: function() {
    return App.Helpers.misc.getCounterValueForDag(this.get('counterGroups'), this.get('id'),
      'org.apache.tez.common.counters.DAGCounter', 'NUM_KILLED_TASKS')
  }.property('id', 'counterGroups'),

  failedTasksLink: function() {
    return '#/dag/%@/tasks?searchText=Status%3AFAILED'.fmt(this.get('id'));
  }.property('id'),

  failedTaskAttemptsLink: function() {
    return '#/dag/%@/taskAttempts?searchText=Status%3AFAILED'.fmt(this.get('id'));
  }.property('id'),

  appContext: function() {
    return this.get('appContextInfo.info')
  }.property('appContextInfo.info'),

  appContextHeading: function() {
    var appContextType = this.get('appContextInfo.appType');
    return 'Additional Info' + (!!appContextType ? ' from ' + appContextType : '');
  }.property('appContextInfo.appType'),

  appInfoContextType: function() {
    switch (this.get('appContextInfo.appType')) {
      case 'Hive':
        return 'text/x-hive';
      case 'Pig':
        return 'text/x-pig';
      default:
        return 'text/x-sql';
    }
  }.property('appContextInfo.appType'),

});
