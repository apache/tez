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
import formatters from '../utils/formatters';

export function txt(value, hash) {
  var message,
      dataType = hash.type,
      formatter = hash.formatter,
      titleAttr = "";

  if(value) {
    value = value[0];
  }

  if(value instanceof Error) {
    message = value.message;
    titleAttr = `title="${value.message}" `;
  }
  else {
    try {
      if(value !== undefined && !formatter && dataType) {
        formatter = formatters[dataType];
      }

      if(formatter && value !== undefined && value !== null) {
        value = formatter(value, hash);
      }

      if(value === undefined || value === null) {
        message = 'Not Available!';
      }
      else {
        return Ember.String.htmlSafe(Ember.Handlebars.Utils.escapeExpression(value.toString()));
      }
    }
    catch(error) {
      message = "Invalid Data!";
      Ember.Logger.error(error);
    }
  }

  return Ember.String.htmlSafe(`<span ${titleAttr}class="txt-message"> ${message} </span>`);
}

export default Ember.Helper.helper(txt);
