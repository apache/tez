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
import { htmlSafe } from '@ember/template';

import layout from '../templates/components/em-tgraph';

import fullscreen from '../utils/fullscreen';
import GraphView from '../utils/graph-view';
import GraphDataProcessor from '../utils/graph-data-processor';

export default Component.extend({

  layout: layout,

  classNames: ['dag-view-container'],

  graphView: null,

  errMessage: null,

  isHorizontal: false,
  hideAdditionals: false,
  isFullscreen: false,

  styles: computed(function () {
    var pathname = window.location.pathname,
        safe = htmlSafe;
    return {
      vertex: safe(`fill: url(${pathname}#vertex-grad); filter: url(${pathname}#grey-glow)`),
      input: safe(`fill: url(${pathname}#input-grad); filter: url(${pathname}#grey-glow)`),
      output: safe(`fill: url(${pathname}#output-grad); filter: url(${pathname}#grey-glow)`),
      task: safe(`fill: url(${pathname}#task-grad); filter: url(${pathname}#grey-glow)`),
      io: safe(`fill: url(${pathname}#input-grad); filter: url(${pathname}#grey-glow)`),
      group: safe(`fill: url(${pathname}#group-grad); filter: url(${pathname}#grey-glow)`),
    };
  }),

  _onOrientationChange: observer('isHorizontal', function () {
  }),

  _onTglAdditionals: observer('hideAdditionals', function () {
    this.graphView.additionalDisplay(this.hideAdditionals);
  }),

  _onTglFullScreen: observer('isFullscreen', function () {
    fullscreen.toggle(this.element);
  }),

  tglOrientation: action(function() {
    var isTopBottom = this.graphView.toggleLayouts();
    this.set('isHorizontal', !isTopBottom);
  }),
  tglAdditionals: action(function() {
    this.set('hideAdditionals', !this.hideAdditionals);
  }),
  fullscreen: action(function () {
    this.set('isFullscreen', !this.isFullscreen);
  }),
  fitGraph: action(function () {
    this.graphView.fitGraph();
  }),
  cogClicked: action(function () {
    this.openColumnSelector();
  }),

  handleResize: function () {
    var container = document.querySelector('#graphical-view-component-container');

    if(container) {
      let rect = container.getBoundingClientRect();
      let offsetTop = rect.top + document.body.scrollTop;
      let idealHeight = window.innerHeight - offsetTop - 70;

      // Minimum dag view component container height
      let minimumHeight = 500;

      // Leave 70 pixel at the bottom
      let height = Math.max(idealHeight, minimumHeight);
      container.style.height = height + "px";
    }
  },

  didInsertElement: function () {
    this._super(...arguments);
    var result = GraphDataProcessor.graphifyData(this.data);

    this.graphView = GraphView.createNewGraphView();

    if(typeof result === "string") {
      this.set('errMessage', result);
    }
    else {
      this.graphView.create(
        this,
        this.element,
        result
      );
    }
    this.handleResize();
    this.set('_handleResize', this.handleResize.bind(this));
    window.addEventListener('resize', this._handleResize);
  },

  willDestroyElement: function () {
    this._super(...arguments);
    if(this._handleResize) {
      window.removeEventListener('resize', this._handleResize);
      this._handleResize = null;
    }
  }
});
