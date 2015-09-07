/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

Bootstrap.BsProgressAnimatedComponent = Bootstrap.BsProgressComponent.extend({
 progressDecimal: null,

 init: function () {
   this._super.call(this, arguments);
   this.progressDecimalObserver();
   this.progressObserver();
 },

 progressDecimalObserver: function () {
   this.set('progress', parseInt(this.get('progressDecimal') * 100).toString());
 }.observes('progressDecimal'),

 progressObserver: function () {
   var progressing = this.get('progress') < 100;
   this.setProperties({
     stripped: progressing,
     animated: progressing
   });
 }.observes('progress')
});

Ember["TEMPLATES"]["components/bs-progressbar"] = Ember.Handlebars.
    template(function anonymous(Handlebars,depth0,helpers,partials,data) {
  var buffer = '', escapeExpression=this.escapeExpression;

  this.compilerInfo = [4,'>= 1.0.0'];
  helpers = this.merge(helpers, Ember.Handlebars.helpers);
  data = data || {};

  data.buffer.push("<span class=\"sr-only\">");
  data.buffer.push(escapeExpression(helpers._triageMustache.call(
    depth0,
    "progress",
    {hash:{},contexts:[depth0],types:["ID"],hashContexts:{},hashTypes:{},data:data}
  )));
  data.buffer.push("%</span>");
  return buffer;
});

Ember.Handlebars.helper('bs-progress-animated', Bootstrap.BsProgressAnimatedComponent);
