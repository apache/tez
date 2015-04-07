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

App.BasicTableComponent.HeaderCellView = Ember.View.extend({
  templateName: 'components/basic-table/header-cell',

  _onColResize: function (event) {
    var data = event.data;

    if(!data.startEvent) {
      data.startEvent = event;
    }

    data.thisHeader.set(
      'column.width',
      (data.startWidth + event.clientX - data.startEvent.clientX) + 'px'
    );
  },

  _endColResize: function (event) {
    var thisHeader = event.data.thisHeader;
    $(document).off('mousemove', thisHeader._onColResize);
    $(document).off('mouseup', thisHeader._endColResize);
  },

  actions: {
    startColResize: function () {
      var mouseTracker = {
        thisHeader: this,
        startWidth: $(this.get('element')).width(),
        startEvent: null
      };
      $(document).on('mousemove', mouseTracker, this._onColResize);
      $(document).on('mouseup', mouseTracker, this._endColResize);
    }
  }
});