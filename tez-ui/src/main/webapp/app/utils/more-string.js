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

"use strict"

var MoreString = {
  /*
   * Replaces the patterns in current string with the given values.
   * Pattern can be {} or {argumentIndex} or {keyName}. {} will be replaced in the order of arguments.
   * Optionally a hash of key value pairs can be passed as last argument.
   * @param string {String} String to format
   * @params [val1, val2 ... Key Object]
   * @return formatted string
   */
  fmt: function (string) {
    var stringParts = string.split(/{(.*?)}/),
      finalString = [],
      key, value, splitPos,
      i, blankPatternCount, partCount,
      argLength, paramObject, args;

    args = finalString.slice.call(arguments);
    args.shift();

    argLength = args.length,
      paramObject = args[argLength - 1];

    if(stringParts.length > 2) {
      if(typeof paramObject !== "object") paramObject = {};

      for(i = 0, blankPatternCount = 0, partCount = stringParts.length - 1; i < partCount; i++) {
        finalString.push(stringParts[i]);
        if(key = stringParts[++i]) {
          splitPos = key.indexOf(":");
          if(splitPos !== -1) {
            value = key.substr(splitPos + 1);
            key = key.substr(0, splitPos);
          }
          else {
            value = undefined;
          }

          if(!key && blankPatternCount < argLength){
            key = args[blankPatternCount];
            blankPatternCount++;
          }

          if(paramObject.hasOwnProperty(key)) {
            value = paramObject[key];
          }
          else if(args.hasOwnProperty(key)) {
            value = args[key];
          }

          finalString.push(value);
        }
        else if(blankPatternCount < argLength){
          finalString.push(args[blankPatternCount]);
          blankPatternCount++;
        }
      }
      finalString.push(stringParts[partCount]);

      return finalString.join('');
    }

    return string;
  },

  /*
   * Removes HTML tags from a string.
   * @param string {String}
   * @return {String}
   */
  removeTags: function(string) {
    return string.replace(/<(.*?)>/g, "");
  }
};

export default MoreString;
