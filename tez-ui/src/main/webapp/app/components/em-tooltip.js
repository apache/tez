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

const TIP_PADDING = 15, // As in em-tooltip.css
      FADE_TIME = 150;

export default Ember.Component.extend({

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

  window: null,
  tip: null,
  bubbles: null,

  _contentObserver: Ember.on("init", Ember.observer("title", "description", "properties", "contents", function () {
    var contents,
        tip = this.get("tip");

    if(this.get("title") || this.get("description") || this.get("properties")){
      contents = [{
        title: this.get("title"),
        description: this.get("description"),
        properties: this.get("properties"),
      }];
    }
    else if(Array.isArray(this.get("contents"))){
      contents = this.get("contents");
    }

    this.set("show", false);
    if(contents) {
      if(tip) {
        tip.hide();
      }
      this.set("_contents", contents);

      Ember.run.later(this, function () {
        this.set("bubbles", this.$(".bubble"));
        this.set("show", true);
        this.renderTip();
      });
    }
    else if(tip){
      tip.stop(true).fadeOut(FADE_TIME);
    }
  })),

  didInsertElement: function () {
    this.setProperties({
      window: Ember.$(window),
      tip: this.$(),
    });
    Ember.$(document).on("mousemove", this, this.onMouseMove);
  },

  willDestroyElement: function () {
    Ember.$(document).off("mousemove", this.onMouseMove);
  },

  onMouseMove: function (event) {
    event.data.setProperties({
      x: event.clientX,
      y: event.clientY
    });

    if(Ember.get(event, "data.tip")) {
      event.data.renderTip();
    }
  },

  getBubbleOffset: function (x, bubbleElement, winWidth) {
    var bubbleWidth = Math.max(bubbleElement.width(), 0),
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
    if(this.get("show")) {
      let x = this.get("x"),
          y = this.get("y"),

          winHeight = this.get("window").height(),
          winWidth = this.get("window").width(),

          showAbove = y < (winHeight >> 1),

          that = this,
          tip = this.get("tip");

      if(x > TIP_PADDING && x < winWidth - TIP_PADDING) {
        if(!showAbove) {
          y -= tip.height();
          this.set("arrowPos", "below");
        }
        else {
          this.set("arrowPos", "above");
        }
      }
      else {
        this.set("arrowPos", null);
      }

      tip.css({
        left: `${x}px`,
        top: `${y}px`,
      });

      tip.fadeIn({
        duration: FADE_TIME,
        start: function () {
          that.get("bubbles").each(function () {
            var bubble = Ember.$(this),
                bubbleOffset = that.getBubbleOffset(x, bubble, winWidth);
            bubble.css("left", `${bubbleOffset}px`);
          });
        }
      });
    }
  }

});
