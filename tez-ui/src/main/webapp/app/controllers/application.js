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

import Controller from '@ember/controller';
import { computed } from '@ember/object';

const BREADCRUMB_PREFIX = [{
  text: "Home",
  routeName: "application"
}];

export default Controller.extend({
  breadcrumbs: null,
  appError: null,

  prefixedBreadcrumbs: computed("breadcrumbs", function () {
    var prefix = BREADCRUMB_PREFIX,
    breadcrumbs = this.breadcrumbs;

    if(Array.isArray(breadcrumbs)) {
      prefix = prefix.concat(breadcrumbs);
    }

    return prefix;
  })
});
