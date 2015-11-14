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

App.Helpers.emData = {
  /**
   * Merge data from an array of records to another
   * @param target {Array} Target record array
   * @param source {Array} Source record array
   * @param mergeProps {Array} Array of strings of property names to be merged
   * @return true if merge was success, else false
   */
  mergeRecords: function (target, source, mergeProps) {

    if(source && target && mergeProps) {
      target.forEach(function (row) {
        var info = source.findBy('id', row.get('id')),
            merge = !!info;

        if(info && info.get('counters')) {
          row.set('counterGroups',
            App.Helpers.misc.mergeCounterInfo(
              row.get('counterGroups'),
              info.get('counters')
            ).slice(0)
          );
          row.didLoad();// To update the record time stamp
        }

        if(merge && row.get('progress') && info.get('progress')) {
          if(row.get('progress') >= info.get('progress')) {
            merge = false;
          }
        }

        if(merge) {
          row.setProperties(info.getProperties.apply(info, mergeProps));
          row.didLoad();// To update the record time stamp
        }
      });
      return true;
    }
    return false;
  }
};
