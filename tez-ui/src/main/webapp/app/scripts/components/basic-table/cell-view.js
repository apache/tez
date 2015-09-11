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

var ObjectPromiseController = Ember.ObjectController.extend(Ember.PromiseProxyMixin);

function stringifyNumbers(content) {
  var displayText = content.displayText;
  if(typeof displayText == 'number') {
    content.displayText = displayText.toString();
  }
  return content;
}

App.BasicTableComponent.CellView = Ember.View.extend({
  templateName: function () {
    var template = this.get('column.observePath') ? 'bounded-basic-cell' : 'basic-cell';
    return 'components/basic-table/' + template;
  }.property('column.observePath'),

  classNames: ['cell-content'],

  value: null,
  observedPath: null,

  _addObserver: function (path) {
    this._removeObserver();
    this.get('row').addObserver(path, this, this._onValueChange);
    this.set('observedPath', path);
  },

  _removeObserver: function (path) {
    var path = this.get('observedPath');
    if(path) {
      this.get('row').removeObserver(path, this, this._onValueChange);
      this.set('observedPath', null);
    }
  },

  _normalizeContent: function (content) {
    return stringifyNumbers(content && typeof content == 'object' ? content : {
      displayText: content
    });
  },

  _pathObserver: function () {
    var path = this.get('column.contentPath');
    if(path && this.get('column.observePath')) {
      this._addObserver(path);
    }
  }.observes('row', 'column.contentPath', 'column.observePath').on('init'),

  _onValueChange: function (row, path) {
    this.set('value', row.get(path));
  },

  cellContent: function () {
    var cellContent = this.get('column').getCellContent(this.get('row'));

    if(cellContent && $.isFunction(cellContent.then)) {
      return ObjectPromiseController.create({
        promise: cellContent.then(this._normalizeContent)
      });
    }

    return this._normalizeContent(cellContent);
  }.property('row', 'column', 'value'),

  willDestroy: function () {
    this._removeObserver();
  }
});