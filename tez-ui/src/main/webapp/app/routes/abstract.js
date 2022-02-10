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

import { action, get, observer } from '@ember/object';
import RecordArray from '@ember-data/store';
import Route from '@ember/routing/route';
import { later } from '@ember/runloop';
import { resolve } from 'rsvp';

import LoaderService from '../services/loader';
import UnlinkedPromise from '../errors/unlinked-promise';
import NameMixin from '../mixins/name';
import MoreObject from '../utils/more-object';

export default Route.extend(NameMixin, {
  title: null, // Must be set by inheriting class

  loaderNamespace: null,
  isMyLoading: false,
  currentPromiseId: null,
  loadedValue: null,

  isLeafRoute: false,
  breadcrumbs: null,
  childCrumbs: null,

  currentQuery: {},

  loaderQueryParams: {},

  init: function () {
    this._super(...arguments);
    var namespace = this.loaderNamespace;
    if(namespace) {
      this.setLoader(namespace);
    }
  },

  model: function(params/*, transition*/) {
    this.set("currentQuery", this.queryFromParams(params));
    later(this, "loadData");
  },

  queryFromParams: function (params) {
    var query = {};

    MoreObject.forEach(this.loaderQueryParams, function (name, paramKey) {
      var value = get(params, paramKey);
      if(value) {
        query[name] = value;
      }
    });

    return query;
  },

  setDocTitle: function () {
    document.title = this.title;
  },

  didTransition: function () {
    this._super(...arguments);
    this.setDocTitle();
  },

  checkAndCall: function (id, functionName, query, options, value) {
    if(id === this.currentPromiseId) {
      return this[functionName](value, query, options);
    }
    else {
      throw new UnlinkedPromise();
    }
  },

  loadData: function (options) {
    var promiseId = Math.random(),
        query = this.currentQuery;

    options = options || {};

    this.set('currentPromiseId', promiseId);

    return resolve().
      then(this.checkAndCall.bind(this, promiseId, "setLoading", query, options)).
      then(this.checkAndCall.bind(this, promiseId, "beforeLoad", query, options)).
      then(this.checkAndCall.bind(this, promiseId, "load", query, options)).
      then(this.checkAndCall.bind(this, promiseId, "afterLoad", query, options)).
      then(this.checkAndCall.bind(this, promiseId, "setValue", query, options)).
      catch(this.onLoadFailure.bind(this));
  },

  setLoading: function (/*query, options*/) {
    this.set('isMyLoading', true);
    this.set('controller.isMyLoading', true);
  },
  beforeLoad: function (value/*, query, options*/) {
    return value;
  },
  load: function (value/*, query, options*/) {
    return value;
  },
  afterLoad: function (value/*, query, options*/) {
    return value;
  },
  setValue: function (value/*, query, options*/) {
    this.set('loadedValue', value);

    this.set('isMyLoading', false);
    this.set('controller.isMyLoading', false);

    this.send("setLoadTime", this.getLoadTime(value));

    return value;
  },
  onLoadFailure: function (error) {
    if(error instanceof UnlinkedPromise) {
      console.warn("Slow down, you are refreshing too fast!");
    }
    else {
      this.send("error", error);
      throw(error);
    }
  },

  getLoadTime: function (value) {
    if(value instanceof RecordArray) {
      value = value.get("content.0.record");
    }
    else if(Array.isArray(value)) {
      value = value[0];
    }

    if(value) {
      return value.loadTime;
    }
  },

  _setControllerModel: observer("loadedValue", function () {
    var controller = this.controller;
    if(controller) {
      controller.set("model", this.loadedValue);
    }
  }),

  setLoader: function (nameSpace) {
    let loader = LoaderService.create( {nameSpace: nameSpace, store: this.store});
    // TODO
    //this.set("loader", loader);
  },

  startCrumbBubble: function () {
    this.send("bubbleBreadcrumbs", []);
  },

  setBreadcrumbs: action(function (crumbs) {
    var name = this.name;
    if(crumbs && crumbs[name]) {
      this.set("breadcrumbs", crumbs[name]);
    }
    return true;
  }),
  bubbleBreadcrumbs: action(function (crumbs) {
    crumbs.unshift.apply(crumbs, this.breadcrumbs);
    return true;
  }),
  reloadAction: action(function () {
    later(this, "loadData", {reload: true});
  }),
  willTransition: action(function () {
    this.set("loadedValue", null);
  })
});
