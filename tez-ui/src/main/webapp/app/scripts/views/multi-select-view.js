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

App.MultiSelectView = Ember.View.extend({
  templateName: 'views/multi-select',
  classNames: ['multi-select'],

  selectAll: false,
  searchRegex: '',

  options: null, //Must be set by sub-classes or instances

  _validRegEx: function () {
    var regExText = this.get('searchRegex');
    regExText = regExText.substr(regExText.indexOf(':') + 1);
    try {
      new RegExp(regExText, 'im');
    }
    catch(e) {
      return false;
    }
    return true;
  }.property('searchRegex'),

  visibleOptions: function () {
    var options = this.get('options'),
        regExText = this.get('searchRegex'),
        regEx;

    if (Em.isEmpty(regExText) || !this.get('_validRegEx')) {
      return options;
    }

    regEx = new RegExp(regExText, 'i');
    return options.filter(function (option) {
      return regEx.test(option.get('displayText'));
    });
  }.property('options', 'searchRegex'),

  _selectObserver: function () {
    var selectedCount = 0;
    this.get('visibleOptions').forEach(function (option) {
      if(option.get('selected')) {
        selectedCount++;
      }
    });
    this.set('selectAll', selectedCount > 0 && selectedCount == this.get('visibleOptions.length'));
  }.observes('visibleOptions.@each.selected'),

  actions: {
    selectAll: function (checked) {
      this.get('visibleOptions').forEach(function (option) {
        option.set('selected', checked);
      });
    }
  }
});