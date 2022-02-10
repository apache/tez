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

import Component from '@ember/component';
import { action, computed, observer } from '@ember/object';
import { not, oneWay } from '@ember/object/computed';
import layout from '../templates/components/em-table-search-ui';

export default Component.extend({
  attributeBindings: ['shouldHide:hidden'],
  layout: layout,

  tableDefinition: null,
  dataProcessor: null,

  classNames: ['search-ui'],
  classNameBindings: ['hasError'],
  shouldHide: not('tableDefinition.enableSearch'),

  searchTypes: ["Regex", "SQL"],
  actualSearchType: null,

  text: oneWay('tableDefinition.searchText'),

  _actualSearchTypeDecider: observer("tableDefinition.searchType", "text", function () {
    var searchType = this.get("tableDefinition.searchType"),
        actualSearchType = this.actualSearchType;

    switch(searchType) {
      case "SQL":
      case "Regex":
        actualSearchType = searchType;
        break;

      case "manual":
        if(!actualSearchType) {
          actualSearchType = "Regex";
        }
        // Will be set from the template
        break;

      case "auto":
        var text = this.text,
            columns = this.get('tableDefinition.columns');

        if(text) {
          actualSearchType = this.get("dataProcessor.sql").validateClause(text, columns) ? "SQL" : "Regex";
        }
        else {
          actualSearchType = null;
        }
        break;
    }

    this.set("actualSearchType", actualSearchType);
  }),

  hasError: computed('actualSearchType', 'dataProcessor.sql', 'tableDefinition.{columns,searchType}', 'text', function () {
    var text = this.text,
        columns = this.get('tableDefinition.columns'),
        actualSearchType = this.actualSearchType;

    if(text) {
      switch(actualSearchType) {
        case "SQL":
            return !this.get("dataProcessor.sql").validateClause(text, columns);
        case "Regex":
          try {
            new RegExp(text);
          }
          catch(e) {
            return true;
          }
      }
    }
    return false;
  }),

  search: action(function () {
    this.parentView.send('search', this.text, this.actualSearchType);
  })
});
