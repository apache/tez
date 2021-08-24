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
import layout from '../templates/components/em-breadcrumbs';

export default Ember.Component.extend({
  layout: layout,

  itemStyle: Ember.computed("items", function () {
    var itemCount = this.get("items.length");

    if(itemCount) {
      let widthPercent = 100 / itemCount;
      return new Ember.Handlebars.SafeString(`max-width: ${widthPercent}%`);
    }
  }),

  normalizedItems: Ember.computed("items", function () {
    var items = this.get("items");

    if(items) {
      let lastIndex = items.length - 1;
      items = items.map(function (item, index) {
        var itemDef = {
          text: item.text || "",
          classNames: item.classNames || [],
        };

        Ember.assert("classNames must be an array", Array.isArray(itemDef.classNames));

        if(index === lastIndex) {
          itemDef.classNames.push("active");
        }
        else {
          itemDef.routeName = item.routeName;
          itemDef.model = item.model;
          itemDef.href = item.href;
          if(item.queryParams) {
            itemDef.queryParams = {
              isQueryParams: true,
              values: item.queryParams
            };
          }
        }

        itemDef.classNames = itemDef.classNames.join(" ");
        return itemDef;
      });
    }

    return items;
  })
});
