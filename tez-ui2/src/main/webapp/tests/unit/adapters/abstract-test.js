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

import { moduleFor, test } from 'ember-qunit';

moduleFor('adapter:abstract', 'Unit | Adapter | abstract', {
  // Specify the other units that are required for this test.
  // needs: ['serializer:foo']
});

test('Basic creation test', function(assert) {
  let adapter = this.subject();

  assert.ok(adapter);
  assert.equal(adapter.serverName, null);

  assert.ok(adapter.host);
  assert.ok(adapter.namespace);
  assert.ok(adapter.pathTypeHash);

  assert.ok(adapter.ajaxOptions);
  assert.ok(adapter.pathForType);
});

test('host, namespace & pathTypeHash test', function(assert) {
  let adapter = this.subject(),
      testServerName = "sn",
      testHosts = {
        sn: "foo.bar",
      },
      testENV = {
        app: {
          namespaces: {
            webService: {
              sn: "ws"
            }
          },
          paths: {
            sn: "path"
          }
        }
      };

  adapter.hosts = testHosts;
  adapter.env = testENV;
  adapter.set("serverName", testServerName);

  assert.equal(adapter.get("host"), testHosts.sn);
  assert.equal(adapter.get("namespace"), testENV.app.namespaces.webService.sn);
  assert.equal(adapter.get("pathTypeHash"), testENV.app.paths.sn);
});

test('ajaxOptions test', function(assert) {
  let adapter = this.subject(),
      testUrl = "foo.bar",
      testMethod = "tm",
      testOptions = {
        a: 1
      },
      testServer = "ts",

      result;

  // Without options
  adapter.serverName = testServer;
  result = adapter.ajaxOptions(testUrl, testMethod);
  assert.ok(result);
  assert.ok(result.crossDomain);
  assert.ok(result.xhrFields.withCredentials);
  assert.equal(result.targetServer, testServer);

  // Without options
  adapter.serverName = testServer;
  result = adapter.ajaxOptions(testUrl, testMethod, testOptions);
  assert.ok(result);
  assert.ok(result.crossDomain);
  assert.ok(result.xhrFields.withCredentials);
  assert.equal(result.targetServer, testServer);
  assert.equal(result.a, testOptions.a);
});

test('pathForType test', function(assert) {
  let adapter = this.subject(),
      testHash = {
        typ: "type"
      };

  assert.expect(2);

  adapter.pathTypeHash = testHash;
  assert.equal(adapter.pathForType("typ"), testHash.typ);
  assert.throws(function () {
    adapter.pathForType("noType");
  });
});
