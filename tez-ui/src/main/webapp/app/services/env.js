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

import { get } from '@ember/object';
import { reads } from '@ember/object/computed';
import Service from '@ember/service';
import MoreObject from '../utils/more-object';

import environment from '../config/environment';

export default Service.extend({
  ENV: null,

  init: function () {
    this._super(...arguments);
    this.collateConfigs();
  },

  collateConfigs: function () {
    var collatedENV = {
          APP: {}
        },
        ENV = window.ENV;

    MoreObject.merge(collatedENV, environment);

    if(ENV) {
      MoreObject.merge(collatedENV.APP, ENV);
    }

    this.setComputedENVs(collatedENV);

    this.set("ENV", collatedENV);
  },

  setComputedENVs: function (env) {
    var navigator = window.navigator;
    env.isIE = navigator.userAgent.indexOf('MSIE') !== -1 || navigator.appVersion.indexOf('Trident/') > 0;

    if(!env.APP.yarnProtocol) {
      let rmHost = get(env, "hosts.rm") || "";
      env.APP.yarnProtocol = rmHost.substr(0, rmHost.indexOf("://")) || window.location.protocol.slice(0, -1);
    }
  },

  app: reads('ENV.APP')
});
