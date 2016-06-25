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

  assert.ok(adapter.normalizeErrorResponse);
  assert.ok(adapter._loaderAjax);
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

test('normalizeErrorResponse test', function(assert) {
  let adapter = this.subject(),
      status = "200",
      testTitle = "title",
      strPayload = "StringPayload",
      objPayload = {x: 1, message: testTitle},
      testHeaders = {},
      response;

  response = adapter.normalizeErrorResponse(status, testHeaders, strPayload);
  assert.equal(response[0].title, undefined);
  assert.equal(response[0].status, status);
  assert.equal(response[0].detail, strPayload);
  assert.equal(response[0].headers, testHeaders);

  response = adapter.normalizeErrorResponse(status, testHeaders, objPayload);
  assert.equal(response[0].title, testTitle);
  assert.equal(response[0].status, status);
  assert.deepEqual(response[0].detail, objPayload);
  assert.equal(response[0].headers, testHeaders);
});

test('normalizeErrorResponse html payload test', function(assert) {
  let adapter = this.subject(),
      status = "200",
      htmlPayload = "StringPayload <b>boldText</b> <script>scriptText</script> <style>styleText</style>",
      testHeaders = {},
      response;

  response = adapter.normalizeErrorResponse(status, testHeaders, htmlPayload);
  assert.equal(response[0].detail, "StringPayload boldText");
});

test('_loaderAjax resolve test', function(assert) {
  let result = {},
      adapter = this.subject({
        ajax: function () {
          assert.ok(1);
          return Ember.RSVP.resolve(result);
        }
      });

  assert.expect(1 + 1);

  adapter._loaderAjax().then(function (val) {
    assert.equal(val.data, result);
  });
});

test('_loaderAjax reject, without title test', function(assert) {
  let errorInfo = {
        status: "500",
        detail: "testDetails"
      },
      msg = "Msg",
      testUrl = "http://foo.bar",
      testQuery = {},
      testNS = "namespace",
      adapter = this.subject({
        outOfReachMessage: "OutOfReach",
        ajax: function () {
          assert.ok(1);
          return Ember.RSVP.reject({
            message: msg,
            errors:[errorInfo]
          });
        }
      });

  assert.expect(1 + 7);

  adapter._loaderAjax(testUrl, testQuery, testNS).catch(function (val) {
    assert.equal(val.message, `${msg} » ${errorInfo.status}: Error accessing ${testUrl}`);
    assert.equal(val.details, errorInfo.detail);
    assert.equal(val.requestInfo.adapterName, "abstract");
    assert.equal(val.requestInfo.url, testUrl);

    assert.equal(val.requestInfo.queryParams, testQuery);
    assert.equal(val.requestInfo.namespace, testNS);

    assert.ok(val.requestInfo.hasOwnProperty("responseHeaders"));
  });
});

test('_loaderAjax reject, with title test', function(assert) {
  let errorInfo = {
        status: "500",
        title: "Server Error",
        detail: "testDetails"
      },
      msg = "Msg",
      testUrl = "url",
      adapter = this.subject({
        outOfReachMessage: "OutOfReach",
        ajax: function () {
          assert.ok(1);
          return Ember.RSVP.reject({
            message: msg,
            errors:[errorInfo]
          });
        }
      });

  assert.expect(1 + 5);

  adapter._loaderAjax(testUrl).catch(function (val) {
    assert.equal(val.message, `${msg} » ${errorInfo.status}: ${errorInfo.title}`);
    assert.equal(val.details, errorInfo.detail);
    assert.equal(val.requestInfo.adapterName, "abstract");
    assert.equal(val.requestInfo.url, testUrl);

    assert.ok(val.requestInfo.hasOwnProperty("responseHeaders"));
  });
});

test('_loaderAjax reject, status 0 test', function(assert) {
  let errorInfo = {
        status: 0,
        title: "Server Error",
        detail: "testDetails"
      },
      msg = "Msg",
      testUrl = "url",
      adapter = this.subject({
        outOfReachMessage: "OutOfReach",
        ajax: function () {
          assert.ok(1);
          return Ember.RSVP.reject({
            message: msg,
            errors:[errorInfo]
          });
        }
      });

  assert.expect(1 + 5);

  adapter._loaderAjax(testUrl).catch(function (val) {
    assert.equal(val.message, `${msg} » ${adapter.outOfReachMessage}`);
    assert.equal(val.details, errorInfo.detail);
    assert.equal(val.requestInfo.adapterName, "abstract");
    assert.equal(val.requestInfo.url, testUrl);

    assert.ok(val.requestInfo.hasOwnProperty("responseHeaders"));
  });
});
