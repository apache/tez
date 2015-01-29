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

App.Dialogs = Em.Namespace.create({
  /*
   * Displays a dialog with a multiselector based on the provided data.
   * - Helper looks for id & displayText in listItems.
   * @param listItems Array of all items
   * @param selectedItems
   * @param keyHash Defines the key that helper must use to get value from item.
   * @return Returns a promoise that would be fulfilled when Ok is pressed
   */
  displayMultiSelect: function (title, listItems, selectedItems, keyHash) {
    /*
     * Looks in an object for properties.
     */
    function getProperty(object, propertyName) {
      var propertyName = (keyHash && keyHash[propertyName]) || propertyName;
      return object[propertyName] || (object.get && object.get(propertyName));
    }

    var container = $( "<div/>" ),
        listHTML = "";

    listItems.forEach(function (item) {
      var id = getProperty(item, 'id'),
          displayText = getProperty(item, 'displayText');

      listHTML += '<li class="no-wrap"><input id=%@ type="checkbox" %@ /> %@</li>'.fmt(
        id,
        selectedItems[id] ? 'checked' : '',
        displayText
      );
    });

    container.append('<ol class="selectable"> %@ </ol>'.fmt(listHTML));
    $('#dialogContainer').append(container);

    return new Em.RSVP.Promise(function (resolve, reject) {
      container.dialog({
        modal: true,
        title: title,
        width: 350,
        height: 500,
        resizable: false,
        open: function() {
          $(this).closest(".ui-dialog")
          .find(".ui-dialog-titlebar-close")
          .append('<span\
              class="ui-button-icon-primary ui-icon ui-icon-closethick align-close-button">\
              </span>');
        },
        buttons: {
          Ok: function() {
            var visibleColumnIds = {};

            container.find('input:checked').each(function(index, checkbox){
              visibleColumnIds[checkbox.id] = true;
            });
            resolve(visibleColumnIds);

            $( this ).dialog("close");
            container.remove();
          }
        }
      });
    });
  }
});
