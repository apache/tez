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
import { observer } from '@ember/object';
import { on } from '@ember/object/evented';
import { scheduleOnce, later } from '@ember/runloop';

const TIP_PADDING = 15; // As in em-tooltip.css

export default Component.extend({

  title: null,
  description: null,
  properties: null,
  contents: null,

  classNames: ["em-tooltip"],
  classNameBindings: ["arrowPos"],

  x: 0,
  y: 0,

  _contents: null,
  show: false,
  arrowPos: null,

  bubbles: null,

  _contentObserver: on("init", observer("title", "description", "properties", "contents", function () {
    var contents;

    if(this.title || this.description || this.properties){
      contents = [{
        title: this.title,
        description: this.description,
        properties: this.properties,
      }];
    }
    else if(Array.isArray(this.contents)){
      contents = this.contents;
    }

    if(contents) {
      this.set("_contents", contents);

      later(this, function () {
        this.element.classList.add('show');
        this.element.classList.remove('hide');
        this.bubbles = this.element.querySelectorAll(".bubble");
        this.renderTip();
      });
    }
    else if(this.element){
      later(this, function () {
        // must run in later to prevent inconsistent state with already queued render
        this.element.classList.add('hide');
        this.element.classList.remove('show');
        this.bubbles = null;
      });
    }
  })),

  didInsertElement: function () {
    this._super(...arguments);
    this.set('_handleMouseMove', this.handleMouseMove.bind(this));
    document.addEventListener('mousemove', this._handleMouseMove);
  },

  willDestroyElement: function () {
    this._super(...arguments);
    document.removeEventListener('mousemove', this._handleMouseMove);
  },

  handleMouseMove: function (event) {
    this.x = event.clientX;
    this.y = event.clientY;

    // Using the presents of bubbles for when to consider rendering
    if(this.element && this.bubbles) {
      this.renderTip();
    }
  },

  getBubbleOffset: function (x, bubbleElement, winWidth) {
    var bubbleWidth = Math.max(parseFloat(getComputedStyle(bubbleElement, null).width.replace("px", "")), 0),
        bubbleOffset = bubbleWidth >> 1;

    if(x - bubbleOffset - TIP_PADDING < 0) {
      bubbleOffset = x - TIP_PADDING;
    }
    else if(x + TIP_PADDING + bubbleOffset > winWidth) {
      bubbleOffset = x - (winWidth - bubbleWidth) + TIP_PADDING;
    }

    return -bubbleOffset;
  },

  renderTip: function () {

    if(!this.isDestroyed) {
      let x = this.x,
          y = this.y,

          winHeight = window.innerHeight,
          winWidth = window.innerWidth,

          showAbove = y < (winHeight >> 1),

          that = this;

      if(x > TIP_PADDING && x < winWidth - TIP_PADDING) {
        if(!showAbove) {
          y -= parseFloat(getComputedStyle(this.element, null).height.replace("px", ""));
          this.set("arrowPos", "below");
        }
        else {
          this.set("arrowPos", "above");
        }
      }
      else {
        this.set("arrowPos", null);
      }

      this.element.style.left = `${x}px`;
      this.element.style.top = `${y}px`;

      this.get("bubbles").forEach(function (bubble) {
        var bubbleOffset = that.getBubbleOffset(x, bubble, winWidth);
        bubble.style.left = `${bubbleOffset}px`;
      });
    }
  }

});
