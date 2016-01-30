/*global more*/
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

import Ember from 'ember';
import DS from 'ember-data';

import LoaderService from '../services/loader';
import UnlinkedPromise from '../errors/unlinked-promise';
import NameMixin from '../mixins/name';

var MoreObject = more.Object;

export default Ember.Route.extend(NameMixin, {
  title: null, // Must be set by inheriting class

  loaderNamespace: null,
  isLoading: false,
  currentPromiseId: null,
  loadedValue: null,

  isLeafRoute: false,
  breadcrumbs: null,
  childCrumbs: null,

  currentQuery: {},

  loaderQueryParams: {},

  init: function () {
    var namespace = this.get("loaderNamespace");
    if(namespace) {
      this.setLoader(namespace);
    }
  },

  model: function(params/*, transition*/) {
    this.set("currentQuery", this.queryFromParams(params));
    Ember.run.later(this, "loadData");
  },

  queryFromParams: function (params) {
    var query = {};

    MoreObject.forEach(this.get("loaderQueryParams"), function (name, paramKey) {
      var value = Ember.get(params, paramKey);
      if(value) {
        query[name] = value;
      }
    });

    return query;
  },

  setDocTitle: function () {
    Ember.$(document).attr('title', "Tez UI : " + this.get('title'));
  },

  setupController: function (controller, model) {
    this._super(controller, model);
    this.setDocTitle();
  },

  checkAndCall: function (id, functionName, query, options, value) {
    if(id === this.get("currentPromiseId")) {
      return this[functionName](value, query, options);
    }
    else {
      throw new UnlinkedPromise();
    }
  },

  loadData: function (options) {
    var promiseId = Math.random(),
        query = this.get("currentQuery");

    options = options || {};

    this.set('currentPromiseId', promiseId);

    return Ember.RSVP.resolve().
      then(this.checkAndCall.bind(this, promiseId, "setLoading", query, options)).
      then(this.checkAndCall.bind(this, promiseId, "beforeLoad", query, options)).
      then(this.checkAndCall.bind(this, promiseId, "load", query, options)).
      then(this.checkAndCall.bind(this, promiseId, "afterLoad", query, options)).
      then(this.checkAndCall.bind(this, promiseId, "setValue", query, options)).
      catch(this.onLoadFailure.bind(this));
  },

  setLoading: function (/*query, options*/) {
    this.set('isLoading', true);
    this.set('controller.isLoading', true);
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

    this.set('isLoading', false);
    this.set('controller.isLoading', false);

    this.send("setLoadTime", this.getLoadTime(value));

    return value;
  },
  onLoadFailure: function (error) {
    if(error instanceof UnlinkedPromise) {
      Ember.Logger.warn("Slow down, you are refreshing too fast!");
    }
    else {
      this.send("error", error);
      throw(error);
    }
  },

  getLoadTime: function (value) {
    if(value instanceof DS.RecordArray) {
      value = value.get("content.0.record");
    }
    else if(Array.isArray(value)) {
      value = value[0];
    }

    if(value) {
      return Ember.get(value, "loadTime");
    }
  },

  _setControllerModel: Ember.observer("loadedValue", function () {
    var controller = this.get("controller");
    if(controller) {
      controller.set("model", this.get("loadedValue"));
    }
  }),

  setLoader: function (nameSpace) {
    this.set("loader", LoaderService.create({
      nameSpace: nameSpace,
      store: this.get("store"),
      container: this.get("container")
    }));
  },

  startCrumbBubble: function () {
    this.send("bubbleBreadcrumbs", []);
  },

  actions: {
    setBreadcrumbs: function (crumbs) {
      var name = this.get("name");
      if(crumbs && crumbs[name]) {
        this.set("breadcrumbs", crumbs[name]);
      }
      return true;
    },
    bubbleBreadcrumbs: function (crumbs) {
      crumbs.unshift.apply(crumbs, this.get("breadcrumbs"));
      return true;
    },
    reload: function () {
      Ember.run.later(this, "loadData", {reload: true});
    },
    willTransition: function () {
      this.set("loadedValue", null);
    },
  }
});
