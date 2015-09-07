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

 //TODO: watch individual counters.
App.DagIndexController = Em.ObjectController.extend(App.ModelRefreshMixin, {
  controllerName: 'DagIndexController',

  needs: 'dag',

  liveData: null,

  succeededTasks: null,
  totalTasks: null,
  completedVertices: null,

  liveDataObserver: function () {
    var vertexInfoContent = this.get('amVertexInfo.content'),
        liveData = null,
        succeededTasks = null,
        totalTasks = null,
        completedVertices = null;

    if(vertexInfoContent && vertexInfoContent.length) {
      liveData = vertexInfoContent,
      succeededTasks = 0,
      totalTasks = 0,
      completedVertices = 0;

      liveData.forEach(function (vertex) {
        succeededTasks += parseInt(vertex.get('succeededTasks'));
        totalTasks += parseInt(vertex.get('totalTasks'));
        if(vertex.get('progress') >= 1) {
          completedVertices++;
        }
      });
    }

    this.setProperties({
      liveData: liveData,
      succeededTasks: succeededTasks,
      totalTasks: totalTasks,
      completedVertices: completedVertices
    });
  }.observes('amVertexInfo'),

  dagRunning: function () {
    var progress = this.get('dagProgress');
    return progress != null && progress < 1;
  }.property('dagProgress'),

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

  liveColumns: function () {
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
        id: 'progress',
        headerCellName: 'Progress',
        contentPath: 'progress',
        templateName: 'components/basic-table/progress-cell'
      },
      {
        id: 'status',
        headerCellName: 'Status',
        templateName: 'components/basic-table/status-cell',
        contentPath: 'status',
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
        id: 'totalTasks',
        headerCellName: 'Total Tasks',
        contentPath: 'totalTasks',
      },
      {
        id: 'succeededTasks',
        headerCellName: 'Succeeded Tasks',
        contentPath: 'succeededTasks',
      },
      {
        id: 'runningTasks',
        headerCellName: 'Running Tasks',
        contentPath: 'runningTasks',
      },
      {
        id: 'pendingTasks',
        headerCellName: 'Pending Tasks',
        contentPath: 'pendingTasks',
      },
      {
        id: 'failedTasks',
        headerCellName: 'Failed Task Attempts',
        contentPath: 'failedTaskAttempts',
      },
      {
        id: 'killedTasks',
        headerCellName: 'Killed Task Attempts',
        contentPath: 'killedTaskAttempts',
      }
    ]);
  }.property('id'),

  load: function () {
    var dag = this.get('controllers.dag.model'),
        controller = this.get('controllers.dag'),
        t = this;
    t.set('loading', true);
    dag.reload().then(function () {
      return controller.loadAdditional(dag);
    }).catch(function(error){
      Em.Logger.error(error);
      var err = App.Helpers.misc.formatError(error, defaultErrMsg);
      var msg = 'error code: %@, message: %@'.fmt(err.errCode, err.msg);
      App.Helpers.ErrorBar.getInstance().show(msg, err.details);
    });
  },

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

  updateAMInfo: function() {
    var status = this.get('amDagInfo.status');
    if (!Em.isNone(status)) {
      this.set('status', status);
      this.set('progress', this.get('amDagInfo.progress'));
    }
  }.observes('amDagInfo', 'amDagInfo._amInfoLastUpdatedTime')
});
