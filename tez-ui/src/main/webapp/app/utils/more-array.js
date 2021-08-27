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

import MoreObject from './more-object';
import MoreString from './more-string';

var MoreArray = {

  /*
   * Returns the first element in the array
   * @params array {Array}
   * @return Value at index 0
   */
  first: function (array) {
    return array[0];
  },

  /*
   * Returns the last element in the array
   * @params array {Array}
   * @return Value at last position
   */
  last: function (array) {
    return array[array.length - 1];
  },

  /*
   * Returns true if the index is in limit
   * @params array {Array}
   * @param index {Number} Index to validate
   * @return {Boolean} True if valid, else false
   */
  validIndex: function (array, index) {
    return index >= 0 && index < array.length;
  },

  /*
   * Mainly to convert a negative index to a vlaid index from the right
   * @params array {Array}
   * @param index {Number} Index to normalize
   * @return {Number} Normalized index if valid, else undefined
   */
  normalizeIndex: function (array, index, length) {
    length = length || array.length;
    if(index < 0) index = length + index;
    if(index >= 0 && index < length) {
      return index;
    }
  },

  /*
   * Get indexes of all occurrence of an element
   * @params array {Array}
   * @param element {Object} Any javascript variable
   * @return indexes {Array} Array of indexes with the element
   */
  indexesOf: function (array, element) {
    var indexes = [],
      index = array.indexOf(element);

    while(index != -1) {
      indexes.push(index);
      index = array.indexOf(element, index + 1);
    }
    return indexes;
  },

  /*
   * Swaps values at indexes A & B
   * @params array {Array}
   * @param indexA {Number} Any value from 0...length.
   * @param indexB {Number}
   * @return status {Boolean} true if the swap was successful, else false
   */
  swap: function (array, indexA, indexB) {
    var length = array.length,
      tmp;

    indexA = array.normalizeIndex(indexA, length),
      indexB = array.normalizeIndex(indexB, length);

    if(indexA != undefined && indexB != undefined) {
      tmp = array[indexA],
        array[indexA] = array[indexB],
        array[indexB] = tmp;
      return true;
    }
    return false;
  },

  /*
   * Removes count number of value from the specified index
   * @params array {Array}
   * @param index {Number} Index to be removed from
   * @param count {Number} Number of elements to be removed starting from the given index
   * Must be > 0, and defaults to 1
   * @return {Array} The removed value(s) if successful, else undefined
   */
  removeFrom: function (array, index, count) {
    index = MoreArray.normalizeIndex(array, index);
    if(index != undefined) {
      return array.splice(index, count || 1);
    }
  },

  /*
   * Remove instances of a specific element from the array
   * @params array {Array}
   * @param element {Object} Element to be removed
   * @param count {Number} Number of elements to be removed. Must be greater than 0, by default all.
   * @return {Boolean} Actual number of deleted items
   */
  remove: function (array, element, count) {
    var index = array.indexOf(element),
      delCount = 0;

    count = count || Number.MAX_VALUE;

    while(index != -1 && delCount < count) {
      array.splice(index, 1);
      delCount++;
      index = array.indexOf(element, index);
    }

    return delCount;
  },

  /*
   * Inserts a set of elements at a position
   * @params array {Array}
   * @param index {Number} Index to insert
   * @param element1...N {Object}, any number of optional arguments that would be inserted as the values
   * @return The array if successful, else undefined
   */
  insert: function (array, index) {
    var args;

    index = MoreArray.normalizeIndex(array, index),
      args = [index, 0];

    // Todo: optimize
    args.shift.apply(arguments);
    args.shift.apply(arguments);
    args.append(arguments);

    if(index != undefined) {
      array.splice.apply(array, args);
      return array;
    }
  },

  /*
   * Append an array to the end of another
   * @params array {Array}
   * @params arrayToAppend {Array}
   * @return Current array
   */
  append: function (array, arrayToAppend) {
    // Todo: Check perf and improve
    array.push.apply(array, arrayToAppend);
    return array;
  },

  /*
   * Pushes only non-null values into the array
   * @params array {Array}
   * @params val1, [val2... valn]
   * @return Current array
   */
  filterPush: function (array) {
    for(var i = 1, count = arguments.length; i < count; i++) {
      if(arguments[i]) {
        array.push(arguments[i]);
      }
    }
    return array;
  },

  /*
   * Returns a new array of all uneque elements
   * @params array {Array}
   * @param none
   * @return {Array}
   */
  unique: function (array) {
    return array.filter(function(item, i){
      return array.indexOf(item) === i;
    }, array);
  },

  /*
   * Returns a new array of all the non-(null/undefined/zero/empty/NaN/false) values
   * @params array {Array}
   * @param none
   * @return {Array}
   */
  realValues: function (array) {
    return array.filter(function (value) {
      return value;
    });
  },

  /*
   * Recursively merge two arrays
   * @params targetArray {Array} Data would be merged to this array
   * @param sourceArray {Array} Array to merge
   * @param append {Boolean} Default false.
   * @return {Array} Current array
   */
  merge: function(targetArray, sourceArray, append) {
    var targetVal, sourceVal;

    if(!Array.isArray(targetArray) || !Array.isArray(sourceArray)) {
      throw new Error(MoreString.fmt(
        "Merge Failed: Cannot merge {} and {}",
        MoreObject.typeOf(targetObject),
        MoreObject.typeOf(sourceObject)
      ));
    }

    if(append) {
      targetArray.append(sourceArray);
    }
    else {
      for(var i = 0, length = sourceArray.length; i < length; i++) {
        targetVal = targetArray[i],
          sourceVal = sourceArray[i];

        if(MoreObject.isPlainObject(targetVal) && MoreObject.isPlainObject(sourceVal)) {
          MoreObject.merge(targetVal, sourceVal, append);
        }
        else if(Array.isArray(targetVal) && Array.isArray(sourceVal)) {
          MoreArray.merge(targetVal, sourceVal, append);
        }
        else {
          targetArray[i] = sourceVal;
        }
      }
    }

    return targetArray;
  },

  /*
   * Converts an array into a hash with value at the specified path as key
   * ie [{x:"a", y:"aa"}, {x:"b", y:"bb"}] and path 'x' will
   * give {a:{x:"a", y:"aa"}, b:{x:"b", y:"bb"}}
   * @params array {Array}
   * @param path {String}
   * @return {Object}
   */
  hashify: function (array, path) {
    return array.reduce(function (obj, element) {
      if(element) {
        obj[element.val(path)] = element;
      }
      return obj;
    }, {});
  },

  /*
   * Find first element from the array with matching value at the specified path
   * The function uses === for comparison
   * @params array {Array}
   * @param path {String}
   * @param value {*}
   * @return {*}
   */
  findBy: function (array, path, value) {
    var element;
    for(var i = 0, length = array.length; i < length; i++){
      element = array[i];
      if(element && element.val(path) === value) {
        return element;
      }
    }
  },

  /*
   * Finds all element from the array with matching value at the specified path
   * The function uses === for comparison
   * @params array {Array}
   * @param path {String}
   * @param value {*}
   * @return {*}
   */
  findAllBy: function (array, path, value) {
    return array.filter(function (element) {
      return element && element.val(path) === value;
    });
  },
};

export default MoreArray;
