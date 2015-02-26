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

App.SwimlanesView = Ember.View.extend({

  didInsertElement: function() {
    var _tip;     // Instance of tip.js
    var task_attempts = this.get("content");
    var controller = this.get("controller");
    var timeBegin = d3.min(task_attempts, function (d) { return d.get('startTime') });
    var timeEnd = d3.max(task_attempts, function (d) { return d.get('endTime') });
    var containers = d3.set(task_attempts.map(function (d) {return d.get('containerId')})).values().sort();
    var laneLength = containers.length;

    var margin = {top: 20, right: 15, bottom: 15, left: 280};
    var width = 960 - margin.left - margin.right;
    var height = 500 - margin.top - margin.bottom;
    var laneHeight = 18;
    var miniHeight = laneLength * laneHeight;

    //scales
    var x = d3.scale.linear()
    .range([0, width])
    .domain([timeBegin, timeEnd]);

    var y = d3.scale.ordinal()
    .domain(containers)
    .rangeRoundBands([0, miniHeight], .20);

    var xAxis = d3.svg.axis()
    .scale(x)
    .orient("bottom")
    .tickSize(0)
    .tickFormat(function(d) { return (d - timeBegin)/1000; });

    var yAxis = d3.svg.axis()
    .scale(y)
    .tickSize(0)
    .orient("left");

    var svg = d3.select('.svg-container')
    .append("svg")
    .attr("width", width + margin.left + margin.right)
    .attr("height", height + margin.top + margin.bottom)
    .attr("class", "svg");
    _tip = App.DagViewComponent.tip;
    _tip.init($('.tool-tip'), $(svg.node()));

    var mini = svg.append("g")
    .attr("transform", "translate(" + margin.left + "," + margin.top + ")")
    .attr("class", "mini");

    mini.append("g")
    .attr("class", "y axis")
    .call(yAxis)
    .selectAll("text")
    .style("text-anchor", "end");

    mini.append("g")
    .attr("class", "x axis")
    .attr("transform", "translate(0," + miniHeight + ")")
    .call(xAxis)
    .selectAll("text")
    .style("text-anchor", "end")
    .attr("transform", "rotate(-90)" );

    // draw container rectangles
    mini.append("g").selectAll("container")
    .data(containers)
    .enter().append("a").attr("xlink:href","file:///Users/jeagles/myember/")
    .append("rect")
    .attr("class", "container")
    .attr("x", 0)
    .attr("y", function(d) {return y(d);})
    .attr("width", width)
    .attr("rx", 6)
    .attr("height", y.rangeBand());

    // draw task attempt rectangles
    mini.append("g").selectAll("task_attempt")
    .data(task_attempts)
    .enter().append("rect")
    .attr("class", function(d) {return "task_attempt";})
    .attr("x", function(d) {return x(d.get('startTime'));})
    .attr("y", function(d) {return y(d.get('containerId'));})
    .attr("width", function(d) {return x(d.get('endTime')) - x(d.get('startTime'));})
    .attr("rx", 6)
    .attr("height", y.rangeBand())
    .on({
      mouseover: _onMouseOver,
      mouseout: _tip.hide,
      click: function (d) { controller.send('taskAttemptClicked', d.get('id'))}
    });

  /**
   * Mouse over handler for all displayed SVG DOM elements.
   * Later the implementation will be refactored and moved into the respective DataNode.
   * d {DataNode} Contains data to be displayed
   */
  function _onMouseOver(d) {
    var event = d3.event,
        node = event.target,
        tooltipData = {}; // Will be populated with {title/text/kvList}.

    node = node.correspondingUseElement || node;

    switch(_getType(node)) {
      case "task_attempt":
        node = d3.event.target;
        tooltipData = {
          position: {
            x: event.clientX,
            y: event.clientY
          },
          title: '%@'.fmt(
            "Task Attempt"
          )
        };
        tooltipData.kvList = {
          "Id": d.get('id'),
          "Task Id": d.get("taskID"),
          "Vertex Id": d.get("vertexID"),
          "DAG Id": d.get("dagID"),
          "Start Time": App.Helpers.date.dateFormat(d.get("startTime")),
          "End Time": App.Helpers.date.dateFormat(d.get("endTime")),
          "Duration": App.Helpers.date.timingFormat(App.Helpers.date.duration(d.get("startTime"), d.get("endTime"))),
        };
      break;
    }

    _tip.show(node, tooltipData, event);
  }

  function _getType(node) {
    return $(node).attr('class');
  }

    /*
    // TODO: task attempt labels - draw labels if they fit
    mini.append("g").selectAll("task_attempt_label")
    .data(task_attempts)
    .enter().append("text")
    .text(function(d) {return d.get('id');})
    .attr("x", function(d) {return x(d.get('startTime'));})
    .attr("y", function(d) {return y(d.get('containerId'));})
    .attr("dy", ".5ex");
    */

  },
});
