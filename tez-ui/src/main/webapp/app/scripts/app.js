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

Ember.FEATURES.I18N_TRANSLATE_HELPER_SPAN = false;
Ember.ENV.I18N_COMPILE_WITHOUT_HANDLEBARS = true;

var App = window.App = Em.Application.createWithMixins(Bootstrap, {
  // Basic logging, e.g. "Transitioned into 'post'"
  LOG_TRANSITIONS: true,

  // Extremely detailed logging, highlighting every internal
  // step made while transitioning into a route, including
  // `beforeModel`, `model`, and `afterModel` hooks, and
  // information about redirects and aborted transitions
  LOG_TRANSITIONS_INTERNAL: true,

  env: {
    isStandalone: true // Can ne set false in the wrapper initializer
  },

  setConfigs: function (configs) {
    if(configs.envDefaults.version == "${version}") {
      delete configs.envDefaults.version;
    }
    App.Helpers.misc.merge(App.Configs, configs);
    $.extend(App.env, {
      timelineBaseUrl: App.Helpers.misc.normalizePath(App.env.timelineBaseUrl),
      RMWebUrl: App.Helpers.misc.normalizePath(App.env.RMWebUrl)
    });
    App.advanceReadiness();
  }
});
App.deferReadiness();

App.Helpers = Em.Namespace.create(),
App.Mappers = Em.Namespace.create(),
App.Configs = Em.Namespace.create();

App.ready = function () {
  $.extend(App.env, App.Configs.envDefaults);

  ["timelineBaseUrl", "RMWebUrl"].forEach(function(item) {
    if (!!App.env[item]) {
      App.env[item] = App.Helpers.misc.normalizePath(App.env[item]);
    }
  })

  App.ApplicationAdapter = App.TimelineRESTAdapter.extend({
    host: App.env.timelineBaseUrl
  });
  App.ApplicationSerializer = App.TimelineSerializer.extend();

  App.AppDetailAdapter = DS.RESTAdapter.extend({
    ajax: function(url, method, hash) {
      hash = hash || {}; // hash may be undefined
      hash.crossDomain = true;
      hash.xhrFields = {withCredentials: true};
      return this._super(url, method, hash);
    },
    namespace: App.Configs.restNamespace.applicationHistory,
    host: App.env.timelineBaseUrl,
    pathForType: function() {
      return "apps";
    },
  });

  App.VertexAdapter = App.ApplicationAdapter.extend({
    _setInputs: function (store, data) {
      var dagId = Ember.get(data, 'primaryfilters.TEZ_DAG_ID.0'),
          vertexName = Ember.get(data, 'otherinfo.vertexName');
      if(dagId) {
        return store.find('dag', dagId).then(function (dag) {
          if(dag.get('vertices') instanceof Array) {
            var vertexData = dag.get('vertices').findBy('vertexName', vertexName);
            if(vertexData && vertexData.additionalInputs) {
              data.inputs = vertexData.additionalInputs;
            }
            if(vertexData && vertexData.additionalOutputs) {
              data.outputs = vertexData.additionalOutputs;
            }
          }
          return data;
        });
      }
      else {
        return Em.RSVP.Promise(data);
      }
    },
    find: function(store, type, id) {
      var that = this;
      return this._super(store, type, id).then(function (data) {
        return that._setInputs(store, data);
      });
    },
    findQuery: function(store, type, queryObj, records) {
      var that = this;
      return that._super(store, type, queryObj, records ).then(function (data) {
        var fetchers = [];
        data.entities.forEach(function (datum) {
          fetchers.push(that._setInputs(store, datum));
        });
        return Em.RSVP.allSettled(fetchers).then(function () {
          return data;
        });
      });
    }
  });

  App.AMInfoAdapter = DS.RESTAdapter.extend({
    ajax: function(url, method, hash) {
      hash = hash || {}; // hash may be undefined
      if (hash && hash.data && hash.data.__app_id__) {
        url = url.replace('__app_id__', hash.data.__app_id__);
        delete hash.data['__app_id__'];
      }
      hash.crossDomain = true;
      hash.xhrFields = {withCredentials: true};
      return this._super(url, method, hash);
    },
    host: App.env.RMWebUrl,
    namespace: App.Configs.restNamespace.aminfo,
  });

  App.DagProgressAdapter = App.AMInfoAdapter.extend({
    buildURL: function(type, id, record) {
      var url = this._super(type, undefined, record);
      return url.replace('__app_id__', record.get('appId'))
        .fmt(record.get('dagIdx'));
    },
    pathForType: function() {
      return 'dagProgress?dagID=%@';
    }
  });

  App.VertexProgressAdapter = App.AMInfoAdapter.extend({
    findQuery: function(store, type, query) {
      var record = query.metadata;
      delete query.metadata;
      return this.ajax(
        this.buildURL(Ember.String.pluralize(type.typeKey),
          record.vertexIds, Em.Object.create(record)), 'GET', { data: query});
    },
    buildURL: function(type, id, record) {
      var url = this._super(type, undefined, record);
      return url.replace('__app_id__', record.get('appId'))
        .fmt(record.get('dagIdx'), id);
    },
    pathForType: function(typeName) {
      return typeName + '?dagID=%@&vertexID=%@';
    }
  });
};

/* Order and include */
require('scripts/default-configs');

require('scripts/translations');
require('scripts/mixins/*');
require('scripts/helpers/*');

require('scripts/router');
require('scripts/views/**/*');
require('scripts/models/**/*');

require('scripts/controllers/**/*');

require('scripts/components/*');
require('scripts/components/dag-view/*');
require('scripts/adapters/*');
