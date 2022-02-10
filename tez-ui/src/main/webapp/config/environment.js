/* jshint node: true */

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

const DEFAULT_APP_CONF = require('./default-app-conf');

module.exports = function (environment) {
  let ENV = {
    modulePrefix: 'tez-ui',
    environment: environment,
    rootURL: '/',
    locationType: 'hash',
    EmberENV: {
      FEATURES: {
        // Here you can enable experimental features on an ember canary build
        // e.g. EMBER_NATIVE_DECORATOR_SUPPORT: true
      },
      EXTEND_PROTOTYPES: {
        // Prevent Ember Data from overriding Date.parse.
        Date: false,
      },
    },

    APP: DEFAULT_APP_CONF,

    contentSecurityPolicy: {
      'connect-src': "* 'self'",
      'child-src': "'self' 'unsafe-inline'",
      'style-src': "'self' 'unsafe-inline'",
      'script-src': "'self' 'unsafe-inline'"
    },

    'ember-d3': {
      only: ['d3-dispatch', 'd3-color', 'd3-ease', 'd3-hierarchy', 'd3-interpolate', 'd3-selection', 'd3-timer', 'd3-transition']
    },

  };

  if (environment === 'development') {
    // ENV.APP.LOG_RESOLVER = true;
    // ENV.APP.LOG_ACTIVE_GENERATION = true;
    // ENV.APP.LOG_TRANSITIONS = true;
    // ENV.APP.LOG_TRANSITIONS_INTERNAL = true;
    // ENV.APP.LOG_VIEW_LOOKUPS = true;
  }

  if (environment === 'test') {
    ENV.APP.autoboot = false;
    // Testem prefers this...
    //ENV.baseURL = '/';
    ENV.locationType = 'none';

    // keep test console output quieter
    //ENV.APP.LOG_ACTIVE_GENERATION = false;
    //ENV.APP.LOG_VIEW_LOOKUPS = false;

    ENV.APP.rootElement = '#ember-testing';
  }

  if (environment === 'production') {

  }

  return ENV;
};
