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
    if (Ember.typeOf(this.get('progress')) === 'number') {
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

  dagPlanContextInfoJson: function() {
    var dagInfo = {
          context: null,
          description: ''
        },
        dagInfoStr = this.get('dagPlanContextInfo');

    try {
      var tmpInfo = $.parseJSON(dagInfoStr);
      dagInfo.context = tmpInfo['context'];
      dagInfo.description = tmpInfo['description'];
    } catch (err) {
      Em.Logger.debug("error parsing daginfo " + err);
      dagInfo.description = dagInfoStr;     
    }

    return dagInfo;
  }.property('dagPlanContextInfo'),

  hasDagInfoContext: function() {
    var x = this.get('dagPlanContextInfoJson');
    return !!this.get('dagPlanContextInfoJson').description;
  }.property('dagPlanContextInfoJson'),

  additionalInfoHeading: function() {
    var contextName = this.get('dagPlanContextInfoJson').context;
    var heading = "Additional Info"
    if (!!contextName) {
      heading += " from " + contextName;
    }
    return heading;
  }.property('dagPlanContextInfoJson'),

  dagInfoContextType: function() {
    switch (this.get('dagPlanContextInfoJson').context) {
      case 'Hive':
        return 'text/x-hive';
      case 'Pig':
        return 'text/x-pig';
      default:
        return 'text/x-sql';
    }
  }.property('dagPlanContextInfoJson'),

  dagInfoContextDesc: function() {
    return this.get('dagPlanContextInfoJson').description || 'N/A'
  }.property('dagPlanContextInfoJson'),

});
