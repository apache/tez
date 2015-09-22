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


/*
 * A singleton class to control the error bar.
 */
App.Helpers.ErrorBar = (function () {
  var _instance; // Singleton instance of the class

  var ErrorBar = Em.Object.extend({
    init: function () {
      var errorBar = $('.error-bar');
      errorBar.find('.close').click(function () {
        if(_instance) {
          _instance.hide();
        }
      });
    },
    /*
     * Displays an error message in the error bar.
     * @param message String Error message
     * @param details String HTML to be displayed as details.
     */
    show: function (message, details) {
      var errorBar = $('.error-bar'),
          messageElement;

      errorBar.find('.expander').unbind('click');
      errorBar.find('.details').removeClass('visible');

      if(details) {
        messageElement = $('<a class="expander" href="#">' + message + '</a>');
        messageElement.click(function (event) {
          errorBar.find('.details').toggleClass('visible');
          event.preventDefault();
        });

        errorBar.find('.details').html(details.replace(/\n/g, "<br />"));
      }
      else {
        messageElement = $('<span>' + message + '</span>');
      }

      errorBar.find('.message').empty().append(messageElement);
      errorBar.addClass('visible');
    },

    /*
     * Hides if the error bar is visible.
     */
    hide: function () {
      var errorBar = $('.error-bar').first();

      errorBar.find('.expander').unbind('click');
      errorBar.find('.details').removeClass('visible');

      errorBar.removeClass('visible');
    }
  });

  ErrorBar.getInstance = function(){
    return _instance || (_instance = ErrorBar.create());
  };
  return ErrorBar;
})();

