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
import { scheduleOnce } from '@ember/runloop';
import layout from '../templates/components/em-table-column';

export default Component.extend({
  layout: layout,

  definition: null,
  rows: null,
  index: 0,

  tableDefinition: null,
  dataProcessor: null,
  adjustedWidth: null,
  defaultWidth: "",

  classNames: ['table-column'],
  classNameBindings: ['inner', 'extraClassNames'],

  inner: computed('index', function () {
    return !!this.index;
  }),

  extraClassNames: computed("definition.classNames", function () {
    var classNames = this.get("definition.classNames");
    if(classNames) {
      return classNames.join(" ");
    }
  }),

  didInsertElement: function () {
    this._super(...arguments);
    scheduleOnce('afterRender', this, function() {
      this.setWidth();
      this.setMinWidth();
    });
  },

  setMinWidth: observer("definition.minWidth", function () {
    this.element.style.minWidth = this.get('definition.minWidth');
  }),

  setWidth: observer("adjustedWidth", "defaultWidth", function () {
    this.element.style.width = this.adjustedWidth || this.defaultWidth;
  }),

  onColResize: function (mouseEvent) {

    if(!this.mouseTracker.startEvent) {
      this.mouseTracker.startEvent = mouseEvent;
    }

    var width = this.mouseTracker.startWidth + mouseEvent.clientX - this.mouseTracker.startEvent.clientX;
    this.set('adjustedWidth', width + 'px');
  },

  endColResize: function () {
    document.removeEventListener('mousemove', this._onColResize);
    document.removeEventListener('mouseup', this._endColResize);
    this.mouseTracker = null;
  },

  sort: action(function () {
    var definition = this.definition,
      beforeSort = definition.get('beforeSort');

    if(!beforeSort || beforeSort.call(definition, definition)) {
      let columnId = this.get('definition.id'),
        sortOrder = this.get('tableDefinition.sortOrder') === 'desc' ? 'asc' : 'desc';

      if (this.parentView) {
        this.parentView.send('sort', columnId, sortOrder);
      }
    }
  }),
  startColResize: action(function () {
    var rect = this.element.getBoundingClientRect();
    var mouseTracker = {
      startWidth: rect.right - rect.left,
      startEvent: null
    };
    this.mouseTracker = mouseTracker;

    this.set('_onColResize', this.onColResize.bind(this));
    this.set('_endColResize', this.endColResize.bind(this));
    document.addEventListener('mousemove', this._onColResize);
    document.addEventListener('mouseup', this._endColResize);
  })
});
