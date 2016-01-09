/*jshint node:true*/
/* global require, module */

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

var Funnel = require("broccoli-funnel");
var EmberApp = require('ember-cli/lib/broccoli/ember-app');

module.exports = function(defaults) {
  var app = new EmberApp(defaults, {
    storeConfigInMeta: false
  });

  var extraAssets = new Funnel('config', {
     srcDir: '/',
     include: ['*.env'],
     destDir: '/config'
  });

  app.import('bower_components/jquery-ui/jquery-ui.js');
  app.import('bower_components/jquery-ui/ui/tooltip.js');

  app.import('bower_components/more-js/dist/more.js');

  return app.toTree(extraAssets);
};
