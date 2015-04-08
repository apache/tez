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

App.BasicTableComponent.SearchView = Ember.View.extend({
  templateName: 'components/basic-table/search-view',

  classNames: ['search-view'],

  text: '',
  _boundText: function () {
    return this.get('text');
  }.property(),

  _validRegEx: function () {
    var regExText = this.get('_boundText');
    regExText = regExText.substr(regExText.indexOf(':') + 1);
    try {
      new RegExp(regExText, 'im');
    }
    catch(e) {
      return false;
    }
    return true;
  }.property('_boundText'),

  actions: {
    search: function () {
      if(this.get('_validRegEx')) {
        this.get('parentView').send('search', this.get('_boundText'));
      }
    }
  }
});