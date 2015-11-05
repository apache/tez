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

App.BaseController = Em.ObjectController.extend({
  controllerName: null, // Must be set by the respective controllers

  isActive: false,

  setup: function () {
    this.set('isActive', true);
  },
  reset: function () {
    this.set('isActive', false);
  },

  getStoreKey: function (subKey) {
    return "%@:%@".fmt(this.get('controllerName'), subKey);
  },
  storeConfig: function (key, value) {
    try {
      localStorage.setItem(this.getStoreKey(key) , JSON.stringify(value));
    }catch(e){
      return e;
    }
    return value;
  },
  fetchConfig: function (key) {
    try {
      return JSON.parse(localStorage.getItem(this.getStoreKey(key)));
    }catch(e){}
    return undefined;
  },

});
