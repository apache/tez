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

App.Router.map(function() {
	this.resource('dags', { path: '/' });
	this.resource('dag', { path: '/dag/:dag_id'}, function() {
		this.route('vertices');
		this.route('tasks');
		this.route('counters');
		this.route('swimlane');
	});

  this.resource('tez-app', {path: '/tez-app/:app_id'}, function(){
    this.route('dags');
    this.route('configs');
  });

  this.resource('vertex', {path: '/vertex/:vertex_id'}, function(){
    this.route('tasks');
    this.route('inputs');
    this.resource('vertexInput', {path: '/input/:input_id'}, function(){
      this.route('configs');
    });
    this.route('counters');
    this.route('details');
    this.route('swimlane');
  });
	
	this.resource('tasks', {path: '/tasks'});
	this.resource('task', {path: '/task/:task_id'}, function(){
		this.route('attempts');
		this.route('counters');
	});
	this.resource('taskAttempt', {path: '/task_attempt/:task_attempt_id'}, function() {
		this.route('counters');
	});
	//this.resource('error', {path:$ '/error'});
});

/*
App.ApplicationRoute = Em.Route.extend({
	actions: {
		//TODO: handle error and show proper error message.
		error: function(error, transition) {
			return;
			this.transitionTo('error').then(function(newRoute) {
				newRoute.controller.set('err', {
					target: transition.targetName,
					statusText: error.message,
					responseText: error.stack
				});
				newRoute.controller.set('pageTitle', "Error");
			});
		}
	}
});*/

App.DagCountersRoute = App.VertexCountersRoute = 
  App.TaskCountersRoute = App.TaskAttemptCountersRoute = Em.Route.extend({
  renderTemplate: function() {
    this.render('common/counters');
  }
});

App.DagsRoute = Em.Route.extend({
  queryParams:  {
    count: App.Helpers.misc.defaultQueryParamsConfig,
    fromID: App.Helpers.misc.defaultQueryParamsConfig,
    user: App.Helpers.misc.defaultQueryParamsConfig,
    status: App.Helpers.misc.defaultQueryParamsConfig,
    appid: App.Helpers.misc.defaultQueryParamsConfig,
    dag_name: App.Helpers.misc.defaultQueryParamsConfig
  },

  setupController: function(controller, model) {
    $(document).attr('title', 'All Dags');
    this._super(controller, model);
    controller.loadData();
  },
});

App.DagRoute = Em.Route.extend({
  model: function(params) {
    return this.store.find('dag', params.dag_id);
  },

  setupController: function(controller, model) {
    $(document).attr('title', 'Dag: %@ (%@)'.fmt(model.get('name'), model.id));
    this._super(controller, model);
  }
});

App.DagSwimlaneRoute = Em.Route.extend({

	model: function(params) {
		var model = this.modelFor('dag');
		var queryParams = {'primaryFilter': 'TEZ_DAG_ID:' + model.id};
		this.store.unloadAll('task_attempt');
		return this.store.findQuery('task_attempt', queryParams);
	},

	setupController: function(controller, model) {
		this._super(controller, model);
	}
});

App.TasksRoute = Em.Route.extend({
  queryParams: {
    status: App.Helpers.misc.defaultQueryParamsConfig,
    parentType: App.Helpers.misc.defaultQueryParamsConfig,
    parentID: App.Helpers.misc.defaultQueryParamsConfig
  },

  setupController: function(controller, model) {
    this._super(controller, model);
    controller.loadData();
  }
});

App.TaskRoute = Em.Route.extend({
  model: function(params) {
    return this.store.find('task', params.task_id);
  },

  setupController: function(controller, model) {
    $(document).attr('title', 'Task: %@'.fmt(model.id));
    this._super(controller, model);
  }
});

App.VertexRoute = Em.Route.extend({
  model: function(params) {
    return this.store.find('vertex', params.vertex_id);
  },

  setupController: function(controller, model) {
    $(document).attr('title', 'Vertex: %@ (%@)'.fmt(model.get('name'), model.id));
    this._super(controller, model);
  }
});

App.VertexSwimlaneRoute = Em.Route.extend({
  model: function(params) {
    var model = this.modelFor('vertex');
    var queryParams = {'primaryFilter': 'TEZ_VERTEX_ID:' + model.id };
    this.store.unloadAll('task_attempt');
    return this.store.find('task_attempt', queryParams);
  },

  setupController: function(controller, model) {
    this._super(controller, model);
  }
});

App.VertexInputsRoute = Em.Route.extend({
  setupController: function(controller, model) {
    this._super(controller, model);
    controller.loadEntities();
  }
});

App.DagTasksRoute = Em.Route.extend({
  queryParams: {
    status: App.Helpers.misc.defaultQueryParamsConfig,
    vertex_id: App.Helpers.misc.defaultQueryParamsConfig 
  },

  setupController: function(controller, model) {
    this._super(controller, model);
    controller.loadData();
  }
});

App.DagVerticesRoute = Em.Route.extend({
  queryParams: {
    status: App.Helpers.misc.defaultQueryParamsConfig 
  },

  setupController: function(controller, model) {
    this._super(controller, model);
    controller.loadData();
  }
});

App.VertexTasksRoute = Em.Route.extend({
  queryParams: {
    status: App.Helpers.misc.defaultQueryParamsConfig
  },

  setupController: function(controller, model) {
    this._super(controller, model);
    controller.loadData();
  }
});

App.TaskAttemptsRoute = Em.Route.extend({
  queryParams: {
    status: App.Helpers.misc.defaultQueryParamsConfig 
  },

  setupController: function(controller, model) {
    $(document).attr('title', 'Task Attempt: %@'.fmt(model.id));
    this._super(controller, model);
    controller.loadData();
  }
});

App.VertexInputConfigsRoute = App.TezAppConfigsRoute = Em.Route.extend({
  renderTemplate: function() {
    this.render('common/configs');
  }
});

App.VertexInputRoute = Em.Route.extend({
  model: function (params) {
    var model = this.modelFor('vertex');
    return model.get('inputs').findBy('id', params.input_id);
  },
  setupController: function(controller, model) {
    this._super(controller, model);
  }
});

App.TezAppRoute = Em.Route.extend({
  model: function(params) {
    var store = this.store;
    return store.find('tezApp', 'tez_' + params.app_id).then(function (tezApp){
      if(!tezApp.get('appId')) return tezApp;
      return store.find('appDetail', tezApp.get('appId')).then(function (appDetails){
        tezApp.set('appDetail', appDetails);
        return tezApp;
      });
    });
  },
  setupController: function(controller, model) {
    $(document).attr('title', 'Application: %@'.fmt(model.id));
    this._super(controller, model);
  }
});

App.TezAppDagsRoute = Em.Route.extend({
  queryParams:  {
    count: App.Helpers.misc.defaultQueryParamsConfig,    
    fromID: App.Helpers.misc.defaultQueryParamsConfig,
    user: App.Helpers.misc.defaultQueryParamsConfig,
    status: App.Helpers.misc.defaultQueryParamsConfig
  },

  setupController: function(controller, model) {
    this._super(controller, model);
    controller.loadData();
  }
});
