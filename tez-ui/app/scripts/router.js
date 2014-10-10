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
		this.route('counters'),
		this.route('vertex'),
		this.route('swimlane')
	});
	this.resource('tasks', {path: '/tasks/:dag_id'});
	this.resource('task', {path: '/task/:task_id'}, function(){
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

App.DagsRoute = Em.Route.extend({
	queryParams:  {
		count: {
			refreshModel: true,
			replace: true
		},
		fromID: {
			refreshModel: true,
			replace: true
		},
		fromTS: {
			refreshModel: true,
			replace: true,
		},
		user: {
			refreshModel: true,
			replace: true
		}
	},

	model: function(params) {
		var controllerClass = this.controllerFor('dags');
		var queryParams = controllerClass.getFilterParams(params);

		//TODO remove this
		this.store.unloadAll('dag');
		this.store.unloadAll('counterGroup');
		this.store.unloadAll('counter');
		return this.store.findQuery('dag', queryParams);
	},

	setupController: function(controller, model) {
		this._super(controller, model);
	},
});

App.DagRoute = Em.Route.extend({
	model: function(params) {
		return this.store.find('dag', params.dag_id);
	},

	setupController: function(controller, model) {
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
	model: function(params) {
		var controllerClass = this.controllerFor('tasks');
		var queryParams = controllerClass.getFilterParams(params);
		
		//TODO: fix this
		this.store.unloadAll('task');
		this.store.unloadAll('counterGroup');
		this.store.unloadAll('counter');
		return this.store.findQuery('task', queryParams);
	},

	setupController: function(controller, model) {
		this._super(controller, model);
	}
});

App.TaskRoute = Em.Route.extend({
	model: function(params) {
		return this.store.find('task', params.task_id);
	},

	setupController: function(controller, model) {
		this._super(controller, model);
	}
});
