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

import layout from '../templates/components/em-tgraph';

import fullscreen from '../utils/fullscreen';
import GraphView from '../utils/graph-view';
import GraphDataProcessor from '../utils/graph-data-processor';

export default Ember.Component.extend({

  layout: layout,

  classNames: ['dag-view-container'],

  graphView: null,

  errMessage: null,

  isHorizontal: false,
  hideAdditionals: false,
  isFullscreen: false,

  styles: Ember.computed(function () {
    var pathname = window.location.pathname,
        safe = Ember.String.htmlSafe;
    return {
      vertex: safe(`fill: url(${pathname}#vertex-grad); filter: url(${pathname}#grey-glow)`),
      input: safe(`fill: url(${pathname}#input-grad); filter: url(${pathname}#grey-glow)`),
      output: safe(`fill: url(${pathname}#output-grad); filter: url(${pathname}#grey-glow)`),
      task: safe(`fill: url(${pathname}#task-grad); filter: url(${pathname}#grey-glow)`),
      io: safe(`fill: url(${pathname}#input-grad); filter: url(${pathname}#grey-glow)`),
      group: safe(`fill: url(${pathname}#group-grad); filter: url(${pathname}#grey-glow)`),
    };
  }),

  _onOrientationChange: function () {
  }.observes('isHorizontal'),

  _onTglAdditionals: function () {
    this.graphView.additionalDisplay(this.get('hideAdditionals'));
  }.observes('hideAdditionals'),

  _onTglFullScreen: function () {
    fullscreen.toggle(this.get('element'));
  }.observes('isFullscreen'),

  actions: {
    tglOrientation: function() {
      var isTopBottom = this.graphView.toggleLayouts();
      this.set('isHorizontal', !isTopBottom);
    },
    tglAdditionals: function() {
      this.set('hideAdditionals', !this.get('hideAdditionals'));
    },
    fullscreen: function () {
      this.set('isFullscreen', !this.get('isFullscreen'));
    },
    fitGraph: function () {
      this.graphView.fitGraph();
    },
    configure: function () {
      this.sendAction('configure');
    }
  },

  didInsertElement: function () {
    var result = GraphDataProcessor.graphifyData(this.get('data'));

    this.graphView = GraphView.createNewGraphView();

    if(typeof result === "string") {
      this.set('errMessage', result);
    }
    else {
      this.graphView.create(
        this,
        this.get('element'),
        result
      );
    }
  }

});
