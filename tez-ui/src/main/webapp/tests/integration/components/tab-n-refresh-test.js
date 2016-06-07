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

import { moduleForComponent, test } from 'ember-qunit';
import hbs from 'htmlbars-inline-precompile';

moduleForComponent('tab-n-refresh', 'Integration | Component | tab n refresh', {
  integration: true
});

test('Basic creation test', function(assert) {
  this.render(hbs`{{tab-n-refresh}}`);

  assert.equal(this.$(".refresh-ui button").text().trim(), 'Refresh');
  assert.equal(
    this.$(".refresh-ui .text-elements").text().trim().split(" ").slice(-4).join(" "),
    "Load time not available!"
  );
  assert.equal(this.$(".refresh-ui input").val(), 'on');

  this.render(hbs`
    {{#tab-n-refresh}}
      template block text
    {{/tab-n-refresh}}
  `);

  assert.equal(this.$(".refresh-ui button").text().trim(), 'Refresh');
});

test('normalizedTabs test', function(assert) {
  var testTabs = [{
    text: "Tab 1",
    routeName: "route_1",
  },{
    text: "Tab 2",
    routeName: "route_2",
  }];

  this.set("tabs", testTabs);

  this.render(hbs`{{tab-n-refresh tabs=tabs}}`);

  assert.equal($(this.$("li")[0]).text().trim(), testTabs[0].text);
  assert.equal($(this.$("li")[1]).text().trim(), testTabs[1].text);
});

test('loadTime test', function(assert) {
  var loadTime = 1465226174574;

  this.set("loadTime", loadTime);

  this.render(hbs`{{tab-n-refresh loadTime=loadTime}}`);
  assert.equal(
    this.$(".refresh-ui .text-elements").text().trim().split(" ").slice(-7).join(" ").replace("\n", ""),
    "Last refreshed at 06 Jun 2016 20:46:14"
  );
});
