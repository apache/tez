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

//import registerWithContainer from "ember-cli-auto-register/register";

export function initialize(application) {
//  registerWithContainer("entity", application);
  application.inject('entitie', 'store', 'service:store');
}

function registerWithContainer(dirName, application) {
    var directoryRegExp = new RegExp("^" + application.name + "/" + dirName);
    var require = window.require;

    Object.keys(require.entries).filter(function(key) {
        return directoryRegExp.test(key);
    }).forEach(function(moduleName) {
      console.log(moduleName);
        var module = require(moduleName, null, null, true);
        var fileName =  moduleName.match(/[^/]+\/?$/)[0];
        if (!module ||
                !module["default"] ||
                !(module["default"].prototype instanceof Ember.Object)
           ) {
            console.log(dirName + "/" + fileName + ".js did not have an Ember.Object as the default export."); // eslint-disable-line
            throw new Error(moduleName + " must export a default to be registered with application.");
        }
      console.log(dirName + ":" + filename, module["default"]);
        application.register(dirName + ":" + fileName, module["default"]);
    });
};
export default {
  name: 'entities',
  initialize
};
