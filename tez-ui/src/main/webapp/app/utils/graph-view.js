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

import { event, select } from 'd3-selection';
import { transition } from 'd3-transition';
import { hierarchy, tree } from 'd3-hierarchy';
import { htmlSafe } from '@ember/template';

import GraphDataProcessor from './graph-data-processor';
import Tip from './tip';
import formatters from './formatters';

var isIE = navigator.userAgent.indexOf('MSIE') !== -1 || navigator.appVersion.indexOf('Trident/') > 0;

/**
 * The view part of Dag View.
 *
 * Displays TEZ DAG graph in a tree layout. (Uses d3.layout.tree)
 * Graph view exposes just 4 functions to the outside world, everything else
 * happens inside the main closure:
 *   1. create
 *   2. fitGraph
 *   3. additionalDisplay
 *   4. toggleLayouts
 *
 * Links, Paths:
 * --------------
 * d3 layout uses the term links, and SVG uses path. Hence you would see both getting used
 * in this file. You can consider link to be a JavaScript data object, and path to be a visible
 * SVG DOM element on the screen.
 *
 * Extra Nodes:
 * ------------
 * Root Node (Invisible):
 *  Dag view support very complex DAGs, even DAGs without interconnections and backward links.
 *  Hence to fit it into a tree layout I have inserted an invisible root node.
 *
 * Dummy Node (Invisible):
 *  Sinks of a vertex are added at the same level of its parent node, Hence to ensure that all
 *  nodes come under the root, a dummy node was added as the child of the root. The visible tree
 *  would be added as child of dummy node.
 *  Dummy also ensures the view symmetry when multiple outputs are present at the dummy level.
 *
 * Sample Structure, inverted tree representation:
 *
 *            As in the view
 *
 *               Source_m1
 *                  |
 *   Source_m2      M1----------+
 *      |           |           |
 *      +-----------M2      Sink_M1
 *                  |
 *      +-----------R1----------+
 *      |                       |
 *   Sink1_R1               Sink2_R1
 *
 *
 *        Internal representation
 *
 *               Source_m1
 *                  |
 *   Source_m2      M1
 *      |           |
 *      +-----------M2      Sink_M1
 *                  |           |
 *                  R1----------+
 *                  |
 *   Sink1_R1     Dummy     Sink2_R1
 *      |           |           |
 *      +-----------+-----------+
 *                  |
 *                 Root
 *
 */

function createNewGraphView() {
var PADDING = 30, // Adding to be applied on the svg view

    LAYOUTS = { // The view supports two layouts - left to right and top to bottom.
      leftToRight: {
        hSpacing: 180,     // Horizontal spacing between nodes
        vSpacing: 70,      // Vertical spacing between nodes
        depthSpacing: 180, // In leftToRight depthSpacing = hSpacing
        linkDelta: 30,     // Used for links starting and ending at the same point
        projector: function (x, y) { // Converts coordinate based on current orientation
          return {x: y, y: x};
        },
        // Defines how path between nodes are drawn
        pathFormatter: function (mx, my, q1x1, q1y1, q1x, q1y, q2x1, q2y1, q2x, q2y ) {
          return `M ${mx} ${my} Q ${q1x1} ${q1y1} ${q1x} ${q1y} Q ${q2x1} ${q2y1} ${q2x} ${q2y}`;
        }
      },
      topToBottom: {
        hSpacing: 120,
        vSpacing: 100,
        depthSpacing: 100, // In topToBottom depthSpacing = vSpacing
        linkDelta: 15,
        projector: function (x, y) {
          return {x: x, y: y};
        },
        pathFormatter: function (mx, my, q1x1, q1y1, q1x, q1y, q2x1, q2y1, q2x, q2y ) {
          return `M ${my} ${mx} Q ${q1y1} ${q1x1} ${q1y} ${q1x} Q ${q2y1} ${q2x1} ${q2y} ${q2x}`;
        }
      }
    },

    DURATION = 350, // Animation duration

    HREF_TYPE_HASH = { // Used to assess the entity type from an event target
      "#task-bubble": "task",
      "#vertex-bg": "vertex",
      "#input-bg": "input",
      "#output-bg": "output",
      "#io-bubble": "io",
      "#group-bubble": "group"
    };

var _width = 0,
    _height = 0,

    _component = null,  // The parent ember component
    _data = null,       // Data object created by data processor
    _treeData = null,   // Points to root data node of the tree structure
    _treeLayout = null, // Instance of d3 tree layout helper
    _treeRoot = null,
    _layout = null,     // Current layout, one of the values defined in LAYOUTS object
    _dimension = null,     // Current layout, one of the values defined in LAYOUTS object

    _svg = null, // svg DOM element
    _g = null,   // For pan and zoom: Svg DOM group element that encloses all the displayed items

    _idCounter = 0,        // To create a fresh id for all displayed nodes
    _scheduledClickId = 0, // Id of scheduled click, used for double click.

    _tip,     // Instance of tip.js

    _panZoomValues, // Temporary storage of pan zoom values for fit toggle
    _panZoom; // A closure returned by _attachPanZoom to reset/modify pan and zoom values

/**
 * Texts grater than maxLength will be trimmed.
 * @param text {String} Text to trim
 * @param maxLength {Number}
 * @return Trimmed text
 */
function _trimText(text, maxLength) {
  if(text) {
    text = text.toString();
    if(text.length > maxLength) {
      text = text.substr(0, maxLength - 1) + '..';
    }
  }
  return text;
}

/**
 * IE 11 does not support css transforms on svg elements. So manually set the same.
 * please keep the transform parameters in sync with the ones in dag-view.less
 * See https://connect.microsoft.com/IE/feedbackdetail/view/920928
 *
 * This can be removed once the bug is fixed in all supported IE versions
 * @param value
 */
function translateIfIE(element, x, y) {
  // Todo - pass it as option
  if(isIE) {
    element.attr('transform', `translate(${x}, ${y})`);
  }
}

/**
 * Add task bubble to a vertex node
 * @param node {SVG DOM element} Vertex node
 * @param d {VertexDataNode}
 */
function _addTaskBubble(node, d) {
  var group = node.append('g');
  group.attr('class', 'task-bubble');
  group.append('use').attr('xlink:href', '#task-bubble');
  translateIfIE(group.append('text')
      .text(_trimText(d.data.totalTasks || 0, 3)), 0, 4);

  translateIfIE(group, 38, -15);
}
/**
 * Add IO(source/sink) bubble to a vertex node
 * @param node {SVG DOM element} Vertex node
 * @param d {VertexDataNode}
 */
function _addIOBubble(node, d) {
  var group,
      inputs = d.inputs.length,
      outputs = d.outputs.length;

  if(inputs || outputs) {
    group = node.append('g');
    group.attr('class', 'io-bubble');
    group.append('use').attr('xlink:href', '#io-bubble');
    translateIfIE(group.append('text')
        .text(_trimText(`${inputs}/${outputs}`, 3)), 0, 4);

    translateIfIE(group, -38, -15);
  }
}
/**
 * Add vertex group bubble to a vertex node
 * @param node {SVG DOM element} Vertex node
 * @param d {VertexDataNode}
 */
function _addVertexGroupBubble(node, d) {
  var group;

  if(d.vertexGroup) {
    group = node.append('g');
    group.attr('class', 'group-bubble');
    group.append('use').attr('xlink:href', '#group-bubble');
    translateIfIE(group.append('text')
        .text(_trimText(d.vertexGroup.groupMembers.length, 2)), 0, 4);

    translateIfIE(group, 38, 15);
  }
}
/**
 * Add status bar to a vertex node
 * @param node {SVG DOM element} Vertex node
 * @param d {VertexDataNode}
 */
function _addStatusBar(node, d) {
  var group = node.append('g');
  group.attr('class', 'status-bar');

  group.append('foreignObject')
      .attr("class", "status")
      .attr("width", 70)
      .attr("height", 15)
      .html('<span class="msg-container">' +
          d.data.status +
          '</span>'
      );
}
/**
 * Creates a base SVG DOM node, with bg and title based on the type of DataNode
 * @param node {SVG DOM element} Vertex node
 * @param d {DataNode}
 * @param titleProperty {String} d's property who's value is the title to be displayed.
 *    By default 'name'.
 * @param maxTitleLength {Number} Title would be trimmed beyond maxTitleLength. By default 3 chars
 */
function _addBasicContents(node, d, titleProperty, maxTitleLength) {
  var className = d.type;

  node.attr('class', `node ${className}`);
  node.append('use').attr('xlink:href', `#${className}-bg`);
  translateIfIE(node.append('text')
      .attr('class', 'title')
      .text(_trimText(d[titleProperty] || d['name'], maxTitleLength || 12)), 0, 4);
}
/**
 * Populates the calling node with the required content.
 * @param s {DataNode}
 */
function _addContent(d) {
  var node = select(this);

  switch(d.data.type) {
    case 'vertex':
      _addBasicContents(node, d.data, 'vertexName');
      _addStatusBar(node, d.data);
      _addTaskBubble(node, d.data);
      _addIOBubble(node, d.data);
      _addVertexGroupBubble(node, d.data);
    break;
    case 'input':
    case 'output':
      _addBasicContents(node, d.data);
    break;
  }
}

/**
 * Create a list of all links connecting nodes in the given array.
 * @param nodes {Array} A list of d3 nodes created by tree layout
 * @return links {Array} All links between nodes in the current DAG
 */
function _getLinks(nodes) {
  var links = [],
      nodeHash;

  nodeHash = nodes.reduce(function (obj, node) {
    obj[node.data.id] = node;
    return obj;
  }, {});

  _data.links.forEach(function (link) {
    var source = nodeHash[link.sourceId],
        target = nodeHash[link.targetId];
    if(source && target) {
      link.setProperties({
        source: source,
        target: target,
        isBackwardLink: source.data.isSelfOrAncestor(target.data)
      });
      links.push(link);
    }
  });

  return links;
}

/**
 * Apply proper depth spacing and remove the space occupied by dummy node
 * if the number of other nodes are odd.
 * @param nodes {Array} A list of d3 nodes created by tree layout
 */
function _normalize(nodes) {
  // Set layout
  var farthestY = 0;
  nodes.forEach(function (d) {
    d.y = d.depth * -_layout.depthSpacing;
    if(d.y < farthestY) {
      farthestY = d.y;
    }
  });

  farthestY -= PADDING;
  nodes.forEach(function (d) {
    d.y -= farthestY;
  });

  //Remove space occupied by dummy
  var rootChildren = _treeData.get('children'),
      rootChildCount = rootChildren.length,
      dummyIndex,
      i;

  if(rootChildCount % 2 === 0) {
    dummyIndex = rootChildren.indexOf(_treeData.get('dummy'));
    if(dummyIndex >= rootChildCount / 2) {
      for(i = 0; i < dummyIndex; i++) {
        rootChildren[i].x = rootChildren[i + 1].x;
        rootChildren[i].y = rootChildren[i + 1].y;
      }
    }
    else {
      for(i = rootChildCount - 1; i > dummyIndex; i--) {
        rootChildren[i].x = rootChildren[i - 1].x;
        rootChildren[i].y = rootChildren[i - 1].y;
      }
    }
  }

  // Put all single vertex outputs in-line with the vertex node
  // So that they are directly below the respective vertex in vertical layout
  nodes.forEach(function (node) {
    if(node.type === GraphDataProcessor.types.OUTPUT &&
        node.get('vertex.outputs.length') === 1 &&
        !node.get('vertex.outEdgeIds.length') &&
        node.get('treeParent.x') !== node.get('x')
    ) {
      node.x = node.get('vertex.x');
    }
  });
}

function _getType(node) {
  if(node.tagName === 'path') {
    return 'path';
  }
  return HREF_TYPE_HASH[node.getAttribute('href')];
}

function _getEndName(fullName) {
  return fullName.substr(fullName.lastIndexOf('.') + 1);
}

/**
 * Mouse over handler for all displayed SVG DOM elements.
 * Later the implementation will be refactored and moved into the respective DataNode.
 * d {DataNode} Contains data to be displayed
 */
function _onMouseOver(d, /*id, nodegroup*/) {
  var node = event.target,
    tooltipData = {}; // Will be populated with {title/text/kvList}.

  node = node.correspondingUseElement || node;

  switch(_getType(node)) {
    case "vertex":
      var list  = {},
          vertex = d.data.data;

      _component.get('vertexProperties').forEach(function (property) {
        var value = {};

        if(vertex && property.getCellContent) {
          value = property.getCellContent(vertex);
          if(value && value.text !== undefined) {
            value = value.text;
          }
        }
        else if(property.contentPath) {
          value = d.data[property.contentPath];
        }

        if(property.cellComponentName === "date-formatter") {
          value = formatters['date'](value, window.ENV);
        }

        if(property.get("id") === "progress" && value) {
          value = Math.round(value * 100) + "%";
        }
        else if(property.get("id") === "duration" && value) {
          value = value + " ms";
        }

        if(typeof value !== 'object') {
          list[property.get('headerTitle')] = value;
        }
      });

      tooltipData = {
        title: d.data.vertexName,
        kvList: list
      };
    break;
    case "input":
      list = {
        "Class": _getEndName(d.data.class),
        "Initializer": _getEndName(d.data.initializer),
        //TODO
        //"Configurations": d.data.configs.length || 0
      };
      tooltipData = {
        title: d.data.name,
        kvList: list
      };
    break;
    case "output":
      list = {
        "Class": _getEndName(d.data.class),
        //TODO
        //"Configurations": d.data.configs.length || 0
      };
      tooltipData = {
        title: d.data.name,
        kvList: list
      };
    break;
    case "task":
      var totalTasks = d.data.data.totalTasks || 0;
      tooltipData.title = totalTasks > 1 ? `${totalTasks} Tasks` : `${totalTasks} Task`;

      if(!isIE) {
        node = event.target;
      }
    break;
    case "io":
      var inputs = d.data.inputs.length,
          outputs = d.data.outputs.length,
          title = "";
      title += inputs > 1 ? `${inputs} Sources` : `${inputs} Source`;
      title += " & ";
      title += outputs > 1 ? `${outputs} Sinks` : `${outputs} Sink`;
      tooltipData.title = title;

      if(!isIE) {
        node = event.target;
      }
    break;
    case "group":
      tooltipData = {
        title: d.data.vertexGroup.groupName,
        text: d.data.vertexGroup.groupMembers.join(", ")
      };
    break;
    case "path":
      let sourceName = d.source.data.name || d.source.data.vertexName,
          targetName = d.target.data.name || d.target.data.vertexName;

      tooltipData = {
        position: {
          x: event.clientX,
          y: event.clientY
        },
        title: `${sourceName} - ${targetName}`
      };
      if(d.edgeId) {
        tooltipData.kvList = {
          "Edge Id": d.edgeId,
          "Data Movement Type": d.dataMovementType,
          "Data Source Type": d.dataSourceType,
          "Scheduling Type": d.schedulingType,
          "Edge Source Class": _getEndName(d.edgeSourceClass),
          "Edge Destination Class": _getEndName(d.edgeDestinationClass)
        };
      }
      else {
        tooltipData.text = d.source.data.type === "input" ? "Source link" : "Sink link";
      }
    break;
  }

  if(tooltipData.kvList) {
    let kvList = tooltipData.kvList,
        newKVList = {};

    Object.keys(kvList).forEach(function (key) {
      if(kvList[key]) {
        newKVList[key] = kvList[key];
      }
    });

    tooltipData.kvList = newKVList;
  }

  _tip.show(node, tooltipData, event);
}

/**
 * onclick handler scheduled using setTimeout
 * @params d {DataNode} data of the clicked element
 * @param node {D3 element} Element that was clicked
 */
function _scheduledClick(d, node) {
  node = node.correspondingUseElement || node;

  _component.entityClicked({
    type: _getType(node),
    d: d
  });

  _tip.hide();
  _scheduledClickId = 0;
}

/**
 * Schedules an onclick handler. If double click event is not triggered the handler
 * will be called in 200ms.
 * @param d {DataNode} Data of the clicked element
 */
function _onClick(d) {
  if(!_scheduledClickId) {
    _scheduledClickId = setTimeout(_scheduledClick.bind(this, d.data, event.target), 200);
  }
}

/**
 * Callback for mousedown & mousemove interactions. To disable click on drag
 * @param d {DataNode} Data of the clicked element
 */
function _onMouse(/*d*/) {
  select(this).on('click', event.type === 'mousedown' ? _onClick : null);
}

/**
 * Double click event handler.
 * @param d {DataNode} Data of the clicked element
 */
function _onDblclick(d) {
  var node = event.target;

  node = node.correspondingUseElement || node;

  if(_scheduledClickId) {
    clearTimeout(_scheduledClickId);
    _scheduledClickId = 0;
  }

  switch(_getType(node)) {
    case "io":
      d.data.toggleAdditionalInclusion();
      _update();
    break;
  }
}

/**
 * Creates a path data string for the given link. Google SVG path data to learn what it is.
 * @param d {Object} Must contain source and target properties with the start and end positions.
 * @return pathData {String} Path data string based on the current layout
 */
function _createPathData(d) {
  var sX = d.source.y,
      sY = d.source.x,
      tX = d.target.y,
      tY = d.target.x,
      mX = (sX + tX)/2,
      mY = (sY + tY)/2,

      sH = Math.abs(sX - tX) * 0.35,
      sV = 0; // strength

  if(d.isBackwardLink) {
    if(sY === tY) {
      sV = 45;
      mY -= 50;
      if(sX === tX) {
        sX += _layout.linkDelta;
        tX -= _layout.linkDelta;
      }
    }
    sH = Math.abs(sX - tX) * 1.1;
  }

  return _layout.pathFormatter(
    sX, sY,

    sX + sH, sY - sV,
    mX, mY,

    tX - sH, tY - sV,
    tX, tY
  );
}

/**
 * Get the node from/to which the node must transition on enter/exit
 * @param d {DataNode}
 * @param property {String} Property to be checked for
 * @return vertex node
 */
function _getVertexNode(d, property) {
  if(d.data.vertex[property] !== undefined) {
    return d.data.vertex;
  }
  return null;
}
/**
 * Update position of all nodes in the list and preform required transitions.
 * @param nodes {Array} Nodes to be updated
 * @param source {d3 element} Node that trigged the update, in first update source will be root.
 */
function _updateNodes(nodes, source) {
  // Enter any new nodes at it's previous position or the parent's previous position.
  nodes.join(
    enter => enter.append('g')
    .attr('transform', function(d) {
      var node = _layout.projector(source.x0, source.y0);
      if (d.data.x0 !== undefined) {
        // We have a previous stored position
        node = {x: d.data.x0, y: d.data.y0};
      }
      return 'translate(' + node.x + ',' + node.y + ')';
    })
    .on('mouseover', _onMouseOver)
    .on('mouseout', _tip.hide)
    .on('mousedown', _onMouse)
    .on('mousemove', _onMouse)
    .on('dblclick', _onDblclick)
    .each(_addContent)
  )
  // Transition nodes to their new position.
  .transition()
    .duration(DURATION)
    .attr('transform', function(d) {
      var node = _layout.projector(d.x, d.y);
      return 'translate(' + node.x + ',' + node.y + ')';
    })

  // Transition exiting nodes to the parent's new position.
  nodes.exit().transition()
    .duration(DURATION)
    .remove();
}

/**
 * Get the node from/to which the link must transition on enter/exit
 * @param d {DataNode}
 * @param property {String} Property to be checked for
 * @return node
 */
function _getTargetNode(d, property) {
  if(d.target.data.type === GraphDataProcessor.types.OUTPUT && d.source[property] !== undefined) {
    return d.source;
  }
  if(d.target[property] !== undefined) {
    return d.target;
  }
  return null;
}
/**
 * Update position of all links in the list and preform required transitions.
 * @param links {Array} Links to be updated
 * @param source {d3 element} Node that trigged the update, in first update source will be root.
 */
function _updateLinks(links, source) {
  // Enter any new links at the parent's previous position.
  links.join(
    enter => enter.insert('path', 'g')
    .attr('class', function (d) {
      var type = d.dataMovementType || "";
      return 'link ' + type.toLowerCase();
    })
    /**
     * IE11 rendering does not work for svg path element with marker set.
     * See https://connect.microsoft.com/IE/feedback/details/801938
     * This can be removed once the bug is fixed in all supported IE versions
     */
    .attr('style', isIE ? "" : htmlSafe("marker-mid: url(" + window.location.pathname + "#arrow-marker);"))
    .attr('d', function(d) {
      var node = _getTargetNode(d, "x0")
      if (node == null) {
        node = source;
      }
      var o = {x: node.x0, y: node.y0};
      return _createPathData({source: o, target: o});
    })
    .on('mouseover', _onMouseOver)
    .on('mouseout', _tip.hide)
  )
    .transition()
    .duration(DURATION)
    .attr('d', _createPathData);

  // Transition exiting nodes to the parent's new position.
  links.exit().transition()
    .duration(DURATION)
    .remove();
}

function _getNodeId(d) {
  return d.data.nodeid || (d.data.nodeid = ++_idCounter);
}
function _getLinkId(d) {
  return d.source.data.id.toString() + d.target.data.id;
}
function _stashOldPositions(d) {
  var node = _layout.projector(d.x, d.y);
  d.data.x0 = node.x;
  d.data.y0 = node.y;
}

/**
 * Updates position of nodes and links based on changes in _treeData.
 */
function _update() {
  _treeRoot = hierarchy(_treeData);
  _treeLayout = tree().size([_dimension.x, _dimension.y])(_treeRoot);
  var nodesData = _treeRoot.descendants(),
      linksData = _getLinks(nodesData);

  _normalize(nodesData);

  var nodes = _g.selectAll('g.node')
    .data(nodesData, _getNodeId);
  _updateNodes(nodes, _treeData);

  var links = _g.selectAll('path.link')
      .data(linksData, _getLinkId);
  _updateLinks(links, _treeData);

  nodesData.forEach(_stashOldPositions);
}

/**
 * Attach pan and zoom events on to the container.
 * @param container {DOM element} Element onto which events are attached.
 * @param g {d3 DOM element} SVG(d3) element that will be moved or scaled
 */
function _attachPanZoom(container, g, element) {
  var SCALE_TUNER = 1 / 700,
      MIN_SCALE = 0.5,
      MAX_SCALE = 2;

  var prevX = 0,
      prevY = 0,

      panX = PADDING,
      panY = PADDING,
      scale = 1,

      scheduleId = 0;

  /**
   * Transform g to current panX, panY and scale.
   * @param animate {Boolean} Animate the transformation in DURATION time.
   */
  function transform(animate) {
    var base = animate ? g.transition().duration(DURATION) : g;
    base.attr('transform', `translate(${panX}, ${panY}) scale(${scale})`);
  }

  /**
   * Check if the item have moved out of the visible area, and reset if required
   */
  function visibilityCheck() {
    var graphBound = g.node().getBoundingClientRect(),
        containerBound = container.getBoundingClientRect();

    if(graphBound.right < containerBound.left ||
      graphBound.bottom < containerBound.top ||
      graphBound.left > containerBound.right ||
      graphBound.top > containerBound.bottom) {
        panX = PADDING;
        panY = PADDING;
        scale = 1;
        transform(true);
    }
  }

  /**
   * Schedule a visibility check and reset if required
   */
  function scheduleVisibilityCheck() {
    if(scheduleId) {
      clearTimeout(scheduleId);
      scheduleId = 0;
    }
    scheduleId = setTimeout(visibilityCheck, 100);
  }

  /**
   * Set pan values
   */
  function onMouseMove(event) {
    panX += event.pageX - prevX;
    panY += event.pageY - prevY;

    transform();

    prevX = event.pageX;
    prevY = event.pageY;
  }
  /**
   * Set zoom values, pan also would change as we are zooming with mouse position as pivote.
   */
  function onWheel(event) {
    var prevScale = scale,

        offset = container.getBoundingClientRect(),
        mouseX = event.pageX - offset.left,
        mouseY = event.pageY - offset.top,
        factor = 0;

    scale += event.deltaY * SCALE_TUNER;
    if(scale < MIN_SCALE) {
      scale = MIN_SCALE;
    }
    else if(scale > MAX_SCALE) {
      scale = MAX_SCALE;
    }

    factor = 1 - scale / prevScale;
    panX += (mouseX - panX) * factor;
    panY += (mouseY - panY) * factor;

    transform();
    scheduleVisibilityCheck();

    _tip.reposition();
    event.preventDefault();
  }

  element.addEventListener('mousewheel', onWheel);

  container.addEventListener('mousedown', function (event) {
    prevX = event.pageX;
    prevY = event.pageY;

    container.addEventListener('mousemove', onMouseMove);
    container.parentNode.classList.add('panning');
  })
  container.addEventListener('mouseup', function () {
    container.removeEventListener('mousemove', onMouseMove);
    container.parentNode.classList.remove('panning');

    scheduleVisibilityCheck();
  });

  /**
   * A closure to reset/modify panZoom based on an external event
   * @param newPanX {Number}
   * @param newPanY {Number}
   * @param newScale {Number}
   */
  return function(newPanX, newPanY, newScale) {
    var values = {
      panX: panX,
      panY: panY,
      scale: scale
    };

    panX = newPanX === undefined ? panX : newPanX;
    panY = newPanY === undefined ? panY : newPanY;
    scale = newScale === undefined ? scale : newScale;

    transform(true);

    return values;
  };
}

/**
 * Sets the layout and update the display.
 * @param layout {Object} One of the values defined in LAYOUTS object
 */
function _setLayout(layout) {
  var leafCount = _data.leafCount,
      dimension;

  // If count is even dummy will be replaced by output, so output would no more be leaf
  if(_data.tree.get('children.length') % 2 === 0) {
    leafCount--;
  }
  dimension = layout.projector(leafCount, _data.maxDepth - 1);

  _layout = layout;

  _width = dimension.x *= _layout.hSpacing;
  _height = dimension.y *= _layout.vSpacing;

  _dimension = _layout.projector(dimension.x, dimension.y); // Because tree is always top to bottom

  _update();
}

var GraphView = {
  /**
   * Creates a DAG view in the given element based on the data
   * @param component {DagViewComponent} Parent ember component, to sendAction
   * @param element {HTML DOM Element} HTML element in which the view will be created
   * @param data {Object} Created by data processor
   */
  create: function (component, element, data) {
    var svg = select(element).select('svg');

    _component = component;
    _data = data;
    _g = svg.append('g').attr('transform', `translate(${PADDING},${PADDING})`);
    _svg = svg.node();
    _tip = Tip;

    _tip.init(element.querySelector('.tool-tip'), _svg);

    _treeData = data.tree;
    _treeData.x0 = 0;
    _treeData.y0 = 0;

    _panZoom = _attachPanZoom(_svg, _g, element);

    _setLayout(LAYOUTS.topToBottom);
  },

  /**
   * Calling this function would fit the graph to the available space.
   */
  fitGraph: function (){
    var scale = Math.min(
      (parseFloat(getComputedStyle(_svg, null).width.replace("px", "")) - PADDING * 2) / _width,
      (parseFloat(getComputedStyle(_svg, null).height.replace("px", "")) - PADDING * 2) / _height
    ),
    panZoomValues = _panZoom();

    if(
      panZoomValues.panX !== PADDING ||
      panZoomValues.panY !== PADDING ||
      panZoomValues.scale !== scale
    ) {
      _panZoomValues = _panZoom(PADDING, PADDING, scale);
    }
    else {
      _panZoomValues = _panZoom(
        _panZoomValues.panX,
        _panZoomValues.panY,
        _panZoomValues.scale);
    }
  },

  /**
   * Control display of additionals or sources and sinks.
   * @param hide {Boolean} If true the additionals will be excluded, else included in the display
   */
  additionalDisplay: function (hide) {
    if(hide) {
      _g.attr('class', 'hide-io');
      _treeData.recursivelyCall('excludeAdditionals');
    }
    else {
      _treeData.recursivelyCall('includeAdditionals');
      _g.attr('class', null);
    }
    _update();
  },

  /**
   * Toggle graph layouts between the available options
   */
  toggleLayouts: function () {
    _setLayout(_layout === LAYOUTS.topToBottom ?
        LAYOUTS.leftToRight :
        LAYOUTS.topToBottom);
    return _layout === LAYOUTS.topToBottom;
  }
};

return GraphView;
}

var GraphView = createNewGraphView();
GraphView.createNewGraphView = createNewGraphView;

export default GraphView;
