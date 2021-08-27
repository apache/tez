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

import MoreArray from './more-array';
import MoreString from './more-string';

    var MoreObject = {

  /*
   * Returns type of an object as a string
   * @param object {Object}
   * @return {String}
   */
  typeOf: function (object) {
    var type = typeof object;
    switch(type) {
      case "object":
        if(Object.isArray(object)) {
          type = "array";
        }
        else if (object === null) {
          type = "null";
        }
        else if(object instanceof RegExp) {
          type = "regexp";
        }
        else if(object instanceof Date) {
          type = "date";
        }
        else if(object instanceof Boolean) {
          type = "boolean";
        }
        else if(object instanceof Number) {
          type = "number";
        }
        else if(object instanceof String) {
          type = "string";
        }
      break;
      case "number":
        if(isNaN(object)) {
          type = "nan";
        }
        else if (object === Infinity) {
          type = "infinity";
        }
      break;
    }
    return type;
  },

  /*
   * Returns true for non-null objects
   * @param object {Object}
   * @return {Boolean}
   */
  isObject: function (object) {
    return !!object && typeof object === 'object';
  },

  /*
   * Returns true for a plain objects - non-null, not array, not dom element, not window, not any other basic types
   * @param object {Object}
   * @return {Boolean}
   */
  isPlainObject: function (object) {
    // Check for null || object || array || dom element || window
    if(!object ||
        typeof object !== 'object' ||
        Array.isArray(object) ||
        object.nodeType ||
        object.Object === Object) {
      return false;
    }
    return true;
  },

  /*
   * Returns true if object is an Array
   * @param object {Object}
   * @return {Boolean}
   */
  isArray: Array.isArray,

  /*
   * Returns true if object is a String
   * @param object {Object}
   * @return {Boolean}
   */
  isString: function (object) {
    return typeof object === 'string';
  },

  /*
   * Returns true if object is Boolean
   * @param object {Object}
   * @return {Boolean}
   */
  isBoolean: function (object) {
    return typeof object === 'boolean';
  },

  /*
   * Returns true if object is a Number
   * @param object {Object}
   * @return {Boolean}
   */
  isNumber: function (object) {
    return typeof object === 'number';
  },

  /*
   * Returns true if object is NaN
   * @param object {Object}
   * @return {Boolean}
   */
  isFunction: function (object) {
    return typeof object === 'function';
  },

  /*
   * Return true if the objects are equal
   * @param objectA {Object} First object to equate
   * @param objectB {Object} Second object to equate
   * @return {Boolean}
   */
  equals: function(objectA, objectB) {
    var property;
    for(property in objectA) {
      if(objectA[property] !== objectB[property]) {
        return false;
      }
    }
    for(property in objectB) {
      if(objectA[property] !== objectB[property]) {
        return false;
      }
    }
    return true;
  },

  /*
   * Gets the value at the specified key path
   * @params object {Object}
   * @param  path {String} Key path to a property inside the object. Dot separated
   */
  val: function(object, path) {
    var properties = path.split('.');

    while(object !== undefined && properties.length) {
      object = object[properties.shift()];
    }
    return object;
  },

  /*
   * Return an array of all values in the object
   * @params object {Object}
   * @return {Array} Array of values
   */
  values: function (object) {
    return Object.keys(object).map
    (function (key) {
      return object[key];
    });
  },

  /*
   * Return all keys of current object
   * @params object {Object}
   * @return {Array} Array of keys
   */
  keys: Object.keys,

  /*
   * Given a value does a reverse look-up for a key
   * @params object {Object}
   * @param value Any javascript variable
   * @param key {String}
   */
  keyOf: function (object, value) {
    var keys = Object.keys(object),
        key,
        index = 0,
        length = keys.length;

    while(index < length) {
      key = keys[index++];

      if(object[key] === value) {
        return key;
      }
    }
    return undefined;
  },

  /*
   * Given a value does a reverse look-up for all matching keys
   * @params object {Object}
   * @param value Any javascript variable
   * @param keys {Array}
   */
  keysOf: function (object, value) {
    return Object.keys(object).filter(function (key) {
      if(object[key] === value) {
        return key;
      }
    });
  },

  /*
   * Adds the missing forEach function for Objects
   * @params object {Object}
   * @param callback {Function} The function will be called with two arguments, key and value
   * @return none
   */
  forEach: function (object, callback, context) {
    Object.keys(object).forEach(function (key) {
      callback.call(context, key, object[key]);
    });
  },

  /*
   * Recursively merge two plain objects
   * @params targetObject {Object} Data would be merged to this object
   * @param sourceObject {Object} Object to merge
   * @param appendArray {Boolean} Default false.
   * @return
   */
  merge: function(targetObject, sourceObject, appendArray) {
    if(!MoreObject.isPlainObject(targetObject) || !MoreObject.isPlainObject(sourceObject)) {
      throw new Error(MoreString.fmt(
        "Merge Failed: Cannot merge {} and {}",
        MoreObject.typeOf(targetObject),
        MoreObject.typeOf(sourceObject)
      ));
    }

    MoreObject.keys(sourceObject).forEach(function (key) {
      var targetVal = targetObject[key],
          sourceVal = sourceObject[key];

      if(MoreObject.isPlainObject(targetVal) && MoreObject.isPlainObject(sourceVal)) {
        MoreObject.merge(targetVal, sourceVal, appendArray);
      }
      else if(Array.isArray(targetVal) && Array.isArray(sourceVal)) {
        MoreArray.merge(targetVal, sourceVal, appendArray);
      }
      else {
        targetObject[key] = sourceVal;
      }
    });

    return targetObject;
  },

  /*
   * Injects a set of values as non-enumerable properties of an object.
   * Old value if any would be available at newValue._old_.
   * @params object {Object} Object to inject to
   * @params properties {Object} Key-value hash of properties
   * @return none
   */
  inject: function (object, properties) {
    MoreObject.forEach(properties, function (key, value) {
      // Inject value
      Object.defineProperty(object, key, {
        value: value
      });
    });
  },

  /*
   * Converts an object/hash into an array of object of key-value pairs
   * @params object {Object} A hash of properties
   * @return array {Array} An array of objects
   */
  arrayfy: function (object) {
    var array = [];
    MoreObject.forEach(object, function (key, value) {
      array.push({
        key: key,
        value: value
      });
    });
    return array;
  },

};

export default MoreObject;
