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

App.BasicTableComponent.ColumnDefinition = (function () {
  function getContentAtPath(row) {
    var contentPath = this.get('contentPath');

    if(contentPath) {
      return row.get(contentPath);
    }
    else {
      throw new Error("contentPath not set!");
    }
  }

  return Em.Object.extend({
    contentPath: null,
    headerCellName: "Not Available!",

    width: "",

    customStyle: function () {
      return 'width:%@'.fmt(this.get('width'));
    }.property('width'),

    getSearchValue: getContentAtPath,
    getSortValue: getContentAtPath,
    getCellContent: getContentAtPath
  });
})();

