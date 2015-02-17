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

App.DagViewView = Ember.View.extend({
  setHeight: function () {
    var container = $('.dag-view-component-container'),
        offset;
    if(container) {
      offset = container.offset();
      container.height(
        Math.max(
          // 50 pixel is left at the bottom
          offset ? $(window).height() - offset.top - 50 : 0,
          450 // Minimum dag view component container height
        )
      );
    }
  },

  didInsertElement: function() {
    $(window).on('resize', this.setHeight);
    this.setHeight();
  },

  willDestroyElement: function () {
    $(window).off('resize', this.setHeight);
  }
});