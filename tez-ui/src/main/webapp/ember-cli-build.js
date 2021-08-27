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
const EmberApp = require('ember-cli/lib/broccoli/ember-app');
var MergeTrees = require('broccoli-merge-trees');

module.exports = function (defaults) {
  var isProd = EmberApp.env() === 'production';
  let app = new EmberApp(defaults, {
    autoImport: {
      // customize ember-auto-import for Content Security Policy
      forbidEval: true,
      webpack: {
        node: {
          global: true,
          fs: 'empty'
        },
        module: {
          noParse: /alasql/
        }
      },
    },
    storeConfigInMeta: false,

    lessOptions: {
      paths: ['node_modules/bootstrap-less/bootstrap/'],
    },

    minifyCSS: {
      enabled: isProd
    },

    minifyJS: {
      // Will be minified by wro4j-maven-plugin for performance
      enabled: false,
    },

    fingerprint: {
      enabled: false
    },

    sourcemaps: {
      enabled: !isProd
    }
  });

  var configEnv = new Funnel('config', {
     srcDir: '/',
     include: ['configs.js'],
     destDir: '/config'
  });

  app.import('node_modules/zip-js/WebContent/zip.js');
  var zipWorker = new Funnel('node_modules/zip-js', {
     srcDir: '/WebContent',
     include: ['z-worker.js', 'deflate.js', 'inflate.js'],
     destDir: '/assets/zip'
  });

  var copyFonts = new Funnel('node_modules/@fortawesome/fontawesome-free/', {
     srcDir: '/webfonts',
     include: ['*.woff2'],
     destDir: '/webfonts'
  });

  return app.toTree(new MergeTrees([configEnv, zipWorker, copyFonts]));
};
