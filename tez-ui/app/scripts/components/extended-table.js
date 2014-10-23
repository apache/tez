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

 App.ExTable = Ember.Namespace.create();

//TODO: fix top calculation in FooterTableContainer

App.ExTable.FilterField = Em.TextField.extend({
	classNames: ['filter'],
  classNameBindings: ['isPopulated','isInputDirty:input-dirty'],
  type: 'search',
  results: 1,
  attributeBindings: ['autofocus', 'results'],
  valueBinding: Em.Binding.oneWay('filterValue'),
  isPopulated: function() {
  	return !Em.isEmpty(this.get('value'));
  }.property('value'),
  insertNewline: function(event) {
  	if (this.get('isInputDirty')) {
  		this.set('filterValue', this.get('value'))
  	}
  },
  cancel: function() {
  	//TODO: ignoring cancel for now. user needs to press enter.
  	//this.set('filterValue', this.get('value'))
  },
  /* TODO: remove this
  doUpdateFilter: function(action) {
  	if (action) {
  		this.get('parentView.controller').send(action, this.get('parentView.content'));
  	}
  },*/
  isInputDirty: function() {
  	return $.trim(this.get('value')) != $.trim(this.get('filterValue'));
  }.property('value', 'filterValue')
});

App.ExTable.FilterRow = Ember.View.extend(Ember.AddeparMixins.StyleBindingsMixin, {
  templateName: 'components/extended-table/filter-row',
  classNames: ['ember-table-table-row', 'ember-table-header-row'],
  styleBindings: ['width'],
  columns: Ember.computed.alias('content'),
  width: Ember.computed.alias('controller._rowWidth'),
  scrollLeft: Ember.computed.alias('controller._tableScrollLeft'),
  onScrollLeftDidChange: function() {
    return this.$().scrollLeft(this.get('scrollLeft'));
  }.observes('scrollLeft'),
  onScroll: function(event) {
    this.set('scrollLeft', event.target.scrollLeft);
    return event.preventDefault();
  }
});

App.ExTable.FilterBlock = Ember.Table.TableBlock.extend({
  classNames: ['ember-table-header-block'],
  itemViewClass: 'App.ExTable.FilterRow',
  content: function() {
    return [this.get('columns')];
  }.property('columns')
});

App.ExTable.FilterTableContainer = Ember.Table.TableContainer.extend(Ember.Table.ShowHorizontalScrollMixin, {
  templateName: 'components/extended-table/filter-container',
  classNames: ['ember-table-table-container', 'ember-table-fixed-table-container', 'ember-table-header-container'],
  height: Ember.computed.alias('controller._filterHeight'),
  width: Ember.computed.alias('controller._tableContainerWidth')
});

App.ExTable.FilterCell = Ember.View.extend(Ember.AddeparMixins.StyleBindingsMixin, {
  templateName: 'components/extended-table/filter-cell',
  classNames: ['ember-table-cell', 'ember-table-header-cell'],
  classNameBindings: ['column.textAlign'],
  styleBindings: ['width', 'height'],
 	column: Ember.computed.alias('content'),
  width: Ember.computed.alias('column.columnWidth'),
  height: function() {
  	return this.get('controller._filterHeight');
  }.property('controller._filterHeight'),
  isPopulated: function() {//TODO: remove
  	return true;
  }.property()
  //TODO: handle resizing if required.
});

App.ExTable.ColumnDefinition = Ember.Table.ColumnDefinition.extend({
	init: function() {
		var columnFilterValueBinding = Em.Binding.from('columnFilterValue').to('controller.filters.' + this.get('filterID'));
		columnFilterValueBinding.connect(this);
	},
	filterCellView: 'App.ExTable.FilterCell',
  filterCellViewClass: Ember.computed.alias('filterCellView'),
  filterID: null,
  isPopulated: function() {
  	return !Em.isEmpty(this.get('columnFilterValue'));
  }.property('columnFilterValue'),
});

App.ExTable.TableComponent = Ember.Table.EmberTableComponent.extend({
	layoutName: 'components/extended-table/extable',
	filters: {},

	hasFilter: true,
	minFilterHeight: 30, //TODO: less changes

	actions: {
		updateFilter: function(columnDef) {
			var filterID = columnDef.get('filterID');
			filterID = filterID || columnDef.get('headerCellName').underscore();
			var filterValue = $.trim(columnDef.get('columnFilterValue'));
			if (filterValue === this.filters[filterID]) {
				return; // nothing to do.
			}
			if (Em.empty(filterValue)) {
				delete this.filters[filterID];
			} else {
				//this.filters[filterID] = filterValue;
				this.get('filters').set(filterID, filterValue);
			}
			if (this.get('onFilterUpdated')) {
				this.sendAction('onFilterUpdated', this.filters);
			}
		},

		removeFilter: function(columnDef) {
			var filterID = columnDef.get('filterID');
			if (!this.filters[filterID]) {
				return;
			}
			delete this.filters[filterID];
			columnDef.set('columnFilterValue', null);
			if (this.get('onFilterUpdated')) {
				this.sendAction('onFilterUpdated', this.filters);
			}
		}
	},

	// private variables
	// Dynamic filter height that adjusts according to the filter content height
	_contentFilterHeight: null,
  _filterHeight: function() {
    var minHeight = this.get('minFilterHeight');
    var contentFilterHeight = this.get('_contentFilterHeight');
    if (contentFilterHeight < minHeight) {
      return minHeight;
    } else {
      return contentFilterHeight;
    }
  }.property('_contentFilterHeight', 'minFilterHeight'),

	// some of these below are private functions extend. however to add the filterrow we need them.
	// tables-container height adjusts to the content height
	_tablesContainerHeight: function() {
    var contentHeight, height;
    height = this.get('_height');
    contentHeight = this.get('_tableContentHeight') + this.get('_headerHeight') + this.get('_footerHeight') 
    	+ this.get('_filterHeight');
    if (contentHeight < height) {
      return contentHeight;
    } else {
      return height;
    }
  }.property('_height', '_tableContentHeight', '_headerHeight', '_footerHeight', '_filterHeight'),

  _bodyHeight: function() {
    var bodyHeight;
    bodyHeight = this.get('_tablesContainerHeight');
    if (this.get('hasHeader')) {
      bodyHeight -= this.get('_headerHeight');
    }
    if (this.get('hasFilter')) { 
      bodyHeight -= this.get('_filterHeight');
    }
    if (this.get('hasFooter')) {
      bodyHeight -= this.get('footerHeight');
    }
    return bodyHeight;
  }.property('_tablesContainerHeight', '_hasHorizontalScrollbar', '_headerHeight', 'footerHeight', '_filterHeight',
  	'hasHeader', 'hasFooter', 'hasFilter'), 

  _hasVerticalScrollbar: function() {
    var contentHeight, height;
    height = this.get('_height');
    contentHeight = this.get('_tableContentHeight') + this.get('_headerHeight') + this.get('_footerHeight') 
    	+ this.get('_filterHeight');
    if (height < contentHeight) {
      return true;
    } else {
      return false;
    }
  }.property('_height', '_tableContentHeight', '_headerHeight', '_footerHeight', '_filterHeight'),

  _tableContentHeight: function() {
    return this.get('rowHeight') * this.get('bodyContent.length');
  }.property('rowHeight', 'bodyContent.length')
});

App.ExTable.FilterColumnMixin = Ember.Mixin.create({
		isFilterable: true,
		filterPresent: function() {
			return !Em.isEmpty(this.get('columnFilterValue'));
		}.property('columnFilterValue'),
});

Ember.Handlebars.helper('extended-table-component', App.ExTable.TableComponent);