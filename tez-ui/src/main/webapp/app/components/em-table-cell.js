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
import { computed, observer } from '@ember/object';
import { on } from '@ember/object/evented';
import { later } from '@ember/runloop';
import layout from '../templates/components/em-table-cell';

export default Component.extend({
  layout: layout,

  classNames: ['table-cell'],
  classNameBindings: ['innerCell', 'isWaiting'],

  innerCell: computed('index', function () {
    if(this.index) {
      return 'inner';
    }
  }),

  row: null,
  columnDefinition: null,

  isWaiting: false,

  _value: null,
  _observedPath: null,
  _comment: null,
  _cellContent: computed({
    set: function (key, value, prevValue) {
      if(value !== prevValue) {
        this.highlightCell();
      }
      return value;
    }
  }),

  _addObserver: function (path) {
    this._removeObserver();
    if (this.row) {
      this.row.addObserver(path, this, this._onValueChange);
      this.set('_observedPath', path);
    }
  },

  _removeObserver: function () {
    var path = this._observedPath;
    if(path) {
      this.row.removeObserver(path, this, this._onValueChange);
      this.set('_observedPath', null);
    }
  },

  _pathObserver: on('init', observer('row', 'columnDefinition.contentPath', 'columnDefinition.observePath', function () {
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

  _cellContentObserver: on('init', observer('row', 'columnDefinition', '_value', function () {
    var cellContent = this.columnDefinition.getCellContent(this.row, this._value),
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
    var element = this.element;
    if(element) {
      element.classList.remove("bg-transition");
      element.classList.add("highlight");
      later(function () {
        element.classList.add("bg-transition");
        element.classList.remove("highlight");
      }, 100);
    }
  },

  willDestroy: function () {
    this._super(...arguments);
    this._removeObserver();
  }
});
