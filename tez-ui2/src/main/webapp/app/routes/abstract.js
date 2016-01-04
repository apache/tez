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
import LoaderService from '../services/loader';
import UnlinkedPromise from '../errors/unlinked-promise';

export default Ember.Route.extend({
  title: null, // Must be set by inheriting class

  isLoading: false,
  currentPromiseId: null,
  loadedValue: null,

  queryParams: null,

  setDocTitle: function () {
    Ember.$(document).attr('title', this.get('title'));
  },

  setupController: function (controller, model) {
    this._super(controller, model);
    this.setDocTitle();
  },

  beforeModel: function (transition) {
    this.set('queryParams', transition.queryParams);
    return this._super(transition);
  },

  checkAndCall: function (id, functionName, value) {
    if(id === this.get("currentPromiseId")) {
      return this[functionName](value);
    }
    else {
      throw new UnlinkedPromise();
    }
  },

  loadData: Ember.observer("queryParams", function () {
    var promiseId = Math.random();

    this.set('currentPromiseId', promiseId);

    return Ember.RSVP.resolve().
      then(this.checkAndCall.bind(this, promiseId, "setLoading")).
      then(this.checkAndCall.bind(this, promiseId, "beforeLoad")).
      then(this.checkAndCall.bind(this, promiseId, "load")).
      then(this.checkAndCall.bind(this, promiseId, "afterLoad")).
      then(this.checkAndCall.bind(this, promiseId, "setValue"));
  }),

  setLoading: function () {
    this.set('isLoading', true);
  },
  beforeLoad: function (value) {
    return value;
  },
  load: function (value) {
    return value;
  },
  afterLoad: function (value) {
    return value;
  },
  setValue: function (value) {
    this.set('loadedValue', value);
    this.set('isLoading', false);
  },

  _setControllerModel: Ember.observer("_controller", "loadedValue", function () {
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
  }

});
