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

App.BasicTableComponent.CellView = Ember.View.extend({
  templateName: 'components/basic-table/basic-cell',

  classNames: ['cell-content'],

  _normalizeContent: function (content) {
    return content && typeof content == 'object' ? content : {
      displayText: content
    };
  },

  cellContent: function () {
    var cellContent = this.get('column').getCellContent(this.get('row'));

    if(cellContent && $.isFunction(cellContent.then)) {
      return ObjectPromiseController.create({
        promise: cellContent.then(this._normalizeContent)
      });
    }

    return this._normalizeContent(cellContent);
  }.property('row', 'column')
});