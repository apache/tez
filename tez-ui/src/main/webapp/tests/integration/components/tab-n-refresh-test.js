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

import { setupRenderingTest } from 'ember-qunit';
import { module, test } from 'qunit';
import { find, findAll, render } from '@ember/test-helpers';
import { hbs } from 'ember-cli-htmlbars';

import formatters from 'tez-ui/utils/formatters';

module('Integration | Component | tab n refresh', function(hooks) {
  setupRenderingTest(hooks);

  test('Basic creation test', async function(assert) {
    await render(hbs`{{tab-n-refresh}}`);

    assert.equal(find(".refresh-ui button").textContent.trim(), 'Refresh');
    assert.equal(
      find(".refresh-ui .text-elements").textContent.trim().split(" ").slice(-4).join(" "),
      "Load time not available!"
    );
    assert.equal(find(".refresh-ui input").value, 'on');

    await render(hbs`
      {{#tab-n-refresh}}
        template block text
      {{/tab-n-refresh}}
    `);

    assert.equal(find(".refresh-ui button").textContent.trim(), 'Refresh');
  });

  test('normalizedTabs test', async function(assert) {
    var testTabs = [{
      text: "Tab 1",
      routeName: "route_1",
    },{
      text: "Tab 2",
      routeName: "route_2",
    }];

    this.set("tabs", testTabs);

    await render(hbs`{{tab-n-refresh tabs=tabs}}`);

    let listItems = findAll('li');
    assert.equal(listItems.length, 2);
    assert.dom(listItems[0]).hasText(testTabs[0].text);
    assert.dom(listItems[1]).hasText(testTabs[1].text);
  });

  test('loadTime test', async function(assert) {
    var loadTime = 1465226174574,
      timeInText = formatters['date'](loadTime, {dateFormat: "DD MMM YYYY HH:mm:ss"});

    this.set("loadTime", loadTime);

    await render(hbs`{{tab-n-refresh loadTime=loadTime}}`);
    assert.equal(
      find(".refresh-ui .text-elements").textContent.trim().split(" ").slice(-7).join(" ").replace("\n", ""),
      `Last refreshed at ${timeInText}`
    );
  });
});
