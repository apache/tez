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

App.VertexAdditionalsController = Em.ObjectController.extend({
  needs: 'vertex',

  controllerName: 'VertexAdditionalsController',

  loadEntities: function() {
    var inputs = this.get('inputs.content'),
        outputs = this.get('outputs.content');

    this.set('inputContent', inputs);
    this.set('inputsAvailable', inputs.length > 0);

    this.set('outputContent', outputs);
    this.set('outputsAvailable', outputs.length > 0);

    this.set('loading', false);
  },

  inputColumns: function() {
    return App.Helpers.misc.createColumnsFromConfigs([
      {
        id: 'inputId',
        headerCellName: 'Name',
        contentPath: 'inputName',
      },
      {
        id: 'inputClass',
        headerCellName: 'Class',
        contentPath: 'inputClass',
      },
      {
        id: 'inputInitializer',
        headerCellName: 'Initializer',
        contentPath: 'inputInitializer',
      },
      {
        id: 'configurations',
        headerCellName: 'Configurations',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#if view.cellContent.count}}\
              {{#link-to 'input.configs' view.cellContent.id class='ember-table-content'}}View Configurations{{/link-to}}\
            {{else}}\
              <span class='ember-table-content'>Not Available</span>\
            {{/if}}")
        }),
        getCellContent: function(row) {
          return {
            count: row.get('configs.content.length'),
            id: row.get('id')
          };
        }
      }
    ]);
  }.property(),

  outputColumns: function() {
    return App.Helpers.misc.createColumnsFromConfigs([
      {
        id: 'outputId',
        headerCellName: 'Name',
        contentPath: 'outputName',
      },
      {
        id: 'outputClass',
        headerCellName: 'Class',
        contentPath: 'outputClass',
      },
      {
        id: 'configurations',
        headerCellName: 'Configurations',
        tableCellViewClass: Em.Table.TableCell.extend({
          template: Em.Handlebars.compile(
            "{{#if view.cellContent.count}}\
              {{#link-to 'output.configs' view.cellContent.id class='ember-table-content'}}View Configurations{{/link-to}}\
            {{else}}\
              <span class='ember-table-content'>Not Available</span>\
            {{/if}}")
        }),
        getCellContent: function(row) {
          return {
            count: row.get('configs.content.length'),
            id: row.get('id')
          };
        }
      }
    ]);
  }.property(),

});