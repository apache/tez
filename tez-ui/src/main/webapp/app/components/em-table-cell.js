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
import layout from '../templates/components/em-table-cell';

export default Ember.Component.extend({
  layout: layout,

  classNames: ['table-cell'],
  classNameBindings: ['innerCell', 'isWaiting'],

  innerCell: Ember.computed('index', function () {
    if(this.get('index')) {
      return 'inner';
    }
  }),

  row: null,
  columnDefinition: null,

  isWaiting: false,

  _value: null,
  _observedPath: null,
  _comment: null,
  _cellContent: Ember.computed({
    set: function (key, value, prevValue) {
      if(value !== prevValue) {
        this.highlightCell();
      }
      return value;
    }
  }),

  _addObserver: function (path) {
    this._removeObserver();
    this.get('row').addObserver(path, this, this._onValueChange);
    this.set('_observedPath', path);
  },

  _removeObserver: function () {
    var path = this.get('_observedPath');
    if(path) {
      this.get('row').removeObserver(path, this, this._onValueChange);
      this.set('_observedPath', null);
    }
  },

  _pathObserver: Ember.on('init', Ember.observer('row', 'columnDefinition.contentPath', 'columnDefinition.observePath', function () {
    var path = this.get('columnDefinition.contentPath');
    if(path && this.get('columnDefinition.observePath')) {
      this._addObserver(path);
    }
  })),

  _onValueChange: function (row, path) {
    this.set('_value', row.get(path));
  },

  setContent: function (content) {
    var comment;

    if(content && content.hasOwnProperty("content")) {
      comment = content.comment;
      content = content.content;
    }

    this.setProperties({
      _comment: comment,
      _cellContent: content,
      isWaiting: false
    });
  },

  _cellContentObserver: Ember.on('init', Ember.observer('row', 'columnDefinition', '_value', function () {
    var cellContent = this.get('columnDefinition').getCellContent(this.get('row'), this.get("_value")),
        that = this;

    if(cellContent && cellContent.then) {
      cellContent.then(function (content) {
        that.setContent(content);
      });
      this.set('isWaiting', true);
    }
    else if(cellContent === undefined && this.get('columnDefinition.observePath')) {
      this.set('isWaiting', true);
    }
    else {
      this.setContent(cellContent);
    }
  })),

  highlightCell: function () {
    var element = this.$();
    if(element) {
      element.removeClass("bg-transition");
      element.addClass("highlight");
      Ember.run.later(function () {
        element.addClass("bg-transition");
        element.removeClass("highlight");
      }, 100);
    }
  },

  willDestroy: function () {
    this._removeObserver();
  }
});
