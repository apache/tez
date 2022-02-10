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

import EmberRouter from '@ember/routing/router';
import config from 'tez-ui/config/environment';

export default class Router extends EmberRouter {
  location = config.locationType;
  rootURL = config.rootURL;
}

Router.map(function() {
  this.route('home', {path: '/'}, function() {
    this.route('queries');
  });
  this.route('dag', {path: '/dag/:dag_id'}, function() {
    this.route('vertices');
    this.route('tasks');
    this.route('attempts');
    this.route('counters');
    this.route('index', {path: '/'}, function() {});
    this.route('graphical');
    this.route('swimlane');
  });
  this.route('vertex', {path: '/vertex/:vertex_id'}, function() {
    this.route('tasks');
    this.route('attempts');
    this.route('counters');
    this.route('configs');
  });
  this.route('task', {path: '/task/:task_id'}, function() {
    this.route('attempts');
    this.route('counters');
  });
  this.route('attempt', {path: '/attempt/:attempt_id'}, function () {
    this.route('counters');
  });
  this.route('query', {path: '/query/:query_id'}, function() {
    this.route('configs');
    this.route('timeline');
  });

  // Alias for backward compatibility with Tez UI V1
  this.route('app', {path: '/tez-app/:app_id'}, function () {
    this.route('dags');
    this.route('configs');
  });
  this.route('app', {path: '/app/:app_id'}, function () {
    this.route('dags');
    this.route('configs');
  });
});
