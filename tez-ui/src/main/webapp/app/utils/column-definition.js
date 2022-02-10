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

import EmberObject, { get } from '@ember/object';
import facetTypes from './facet-types';

function getContentAtPath(row) {
  var contentPath = this.get("contentPath");

  if(contentPath) {
    return get(row, contentPath);
  }
  else {
    throw new Error("contentPath not set!");
  }
}

function returnEmptyString() {
  return "";
}

var ColumnDefinition = EmberObject.extend({
  id: "",
  headerTitle: "Not Available!",

  classNames: [],

  cellComponentName: null,

  enableSearch: true,
  enableSort: true,
  enableColumnResize: true,

  width: null,
  minWidth: "150px",

  contentPath: null,
  observePath: false,

  cellDefinition: null,

  pin: "center",

  facetType: facetTypes.VALUES,

  beforeSort: null,
  getCellContent: getContentAtPath,
  getSearchValue: getContentAtPath,
  getSortValue: getContentAtPath,

  init: function () {
    if(!this.id) {
      throw new Error("ID is not set.");
    }
  },
});

ColumnDefinition.make = function (rawDefinition) {
  if(Array.isArray(rawDefinition)) {
    return rawDefinition.map(function (def) {
      return ColumnDefinition.create(def);
    });
  }
  else if(typeof rawDefinition === 'object') {
    return ColumnDefinition.create(rawDefinition);
  }
  else {
    throw new Error("rawDefinition must be an Array or an Object.");
  }
};

ColumnDefinition.fillerColumn = ColumnDefinition.create({
  id: "fillerColumn",
  headerTitle: "",
  getCellContent: returnEmptyString,
  getSearchValue: returnEmptyString,
  getSortValue: returnEmptyString,

  enableSearch: false,
  enableSort: false,
  enableColumnResize: false,
});

export default ColumnDefinition;
