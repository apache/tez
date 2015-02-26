/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * 'License'); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
App.DagViewComponent.graphView = (function (){

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
          pathDataFormat: "M %@ %@ Q %@ %@ %@ %@ Q %@ %@ %@ %@"
        },
        topToBottom: {
          hSpacing: 120,
          vSpacing: 100,
          depthSpacing: 100, // In topToBottom depthSpacing = vSpacing
          linkDelta: 15,
          projector: function (x, y) {
            return {x: x, y: y};
          },
          pathDataFormat: "M %@2 %@1 Q %@4 %@3 %@6 %@5 Q %@8 %@7 %@10 %@9"
        }
      },

      DURATION = 750, // Animation duration

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
      _layout = null,     // Current layout, one of the values defined in LAYOUTS object

      _svg = null, // jQuery instance of svg DOM element
      _g = null,   // For pan and zoom: Svg DOM group element that encloses all the displayed items

      _idCounter = 0,        // To create a fresh id for all displayed nodes
      _scheduledClickId = 0, // Id of scheduled click, used for double click.

      _tip,     // Instance of tip.js
      _panZoom; // A closure returned by _attachPanZoom to reset/modify pan and zoom values

  function _getName(d) {
    switch(d.get('type')) {
      case 'vertex':
        return d.get('vertexName');
      break;
      default:
        return d.get('name');
    }
  }
  function _getSub(d) {
    switch(d.get('type')) {
      case 'vertex':
        return d.get('data.status');
      break;
      default:
        return d.get('class');
    }
  }
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
   * Add task bubble to a vertex node
   * @param node {SVG DOM element} Vertex node
   * @param d {VertexDataNode}
   */
  function _addTaskBubble(node, d) {
    var group = node.append('g');
    group.attr('class', 'task-bubble');
    group.append('use').attr('xlink:href', '#task-bubble');
    group.append('text')
        .text(_trimText(d.get('data.numTasks'), 3));
  }
  /**
   * Add IO(source/sink) bubble to a vertex node
   * @param node {SVG DOM element} Vertex node
   * @param d {VertexDataNode}
   */
  function _addIOBubble(node, d) {
    var group,
        inputs = d.get('inputs.length'),
        outputs = d.get('outputs.length');

    if(inputs || outputs) {
      group = node.append('g');
      group.attr('class', 'io-bubble');
      group.append('use').attr('xlink:href', '#io-bubble');
      group.append('text')
          .text(_trimText('%@/%@'.fmt(inputs, outputs), 3));
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
      group.append('text')
          .text(_trimText(d.get('vertexGroup.groupMembers.length'), 2));
    }
  }
  /**
   * Add status bar to a vertex node
   * @param node {SVG DOM element} Vertex node
   * @param d {VertexDataNode}
   */
  function _addStatusBar(node, d) {
    var group = node.append('g'),
        statusIcon = App.Helpers.misc.getStatusClassForEntity(d.get('data.status'));
    group.attr('class', 'status-bar');

    group.append('foreignObject')
        .attr("class", "status")
        .attr("width", 70)
        .attr("height", 15)
        .html('<span class="msg-container"><i class="task-status ' +
            statusIcon +
            '"></i> ' +
            d.get('data.status') +
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

    node.attr('class', 'node %@'.fmt(className));
    node.append('use').attr('xlink:href', '#%@-bg'.fmt(className));
    node.append('text')
        .attr('class', 'title')
        .text(_trimText(d.get(titleProperty || 'name'), maxTitleLength || 3));
  }
  /**
   * Populates the calling node with the required content.
   * @param s {DataNode}
   */
  function _addContent(d) {
    var node = d3.select(this);

    switch(d.type) {
      case 'vertex':
        _addBasicContents(node, d, 'vertexName', 15);
        _addStatusBar(node, d);
        _addTaskBubble(node, d);
        _addIOBubble(node, d);
        _addVertexGroupBubble(node, d);
      break;
      case 'input':
      case 'output':
        _addBasicContents(node, d);
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
      obj[node.id] = node;
      return obj;
    }, {});

    _data.links.forEach(function (link) {
      var source = nodeHash[link.sourceId],
          target = nodeHash[link.targetId];
      if(source && target) {
        link.setProperties({
          source: source,
          target: target,
          isBackwardLink: source.isSelfOrAncestor(target)
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
      if(d.y < farthestY) farthestY = d.y;
    });
    farthestY -= PADDING;
    nodes.forEach(function (d) {
      d.y -= farthestY;
    });

    //Remove space occupied by dummy
    var rootChildren = _treeData.get('children'),
        rootChildCount = rootChildren.length,
        dummyIndex;

    if(rootChildCount % 2 == 0) {
      dummyIndex = rootChildren.indexOf(_treeData.get('dummy'));
      if(dummyIndex >= rootChildCount / 2) {
        for(var i = dummyIndex - 1; i >= 0; i--) {
          rootChildren[i].x = rootChildren[i + 1].x,
          rootChildren[i].y = rootChildren[i + 1].y;
        }
      }
      else {
        for(var i = dummyIndex + 1; i < rootChildCount; i++) {
          rootChildren[i].x = rootChildren[i - 1].x,
          rootChildren[i].y = rootChildren[i - 1].y;
        }
      }
    }
  }

  function _getType(node) {
    if(node.tagName == 'path') {
      return 'path';
    }
    return HREF_TYPE_HASH[$(node).attr('href')];
  }

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
      case "vertex":
        var list  = {};

        _component.get('vertexProperties').forEach(function (property) {
          var value = {};

          if(property.contentPath) {
            value = d.get('data.' + property.contentPath);
          }
          else if(property.getCellContent && !property.tableCellViewClass) {
            value = property.getCellContent(d.get('data'));
          }

          if(typeof value != 'object') {
            list[property.get('headerCellName')] = value;
          }
        });

        tooltipData = {
          title: d.get("vertexName"),
          kvList: list
        };
      break;
      case "input":
        var list = {
          "Class": App.Helpers.misc.getClassName(d.get("class")),
          "Initializer": App.Helpers.misc.getClassName(d.get("initializer")),
          "Configurations": d.get("configs.length")
        };
        tooltipData = {
          title: d.get("name"),
          kvList: list
        };
      break;
      case "output":
        var list = {
          "Class": App.Helpers.misc.getClassName(d.get("class")),
          "Configurations": d.get("configs.length")
        };
        tooltipData = {
          title: d.get("name"),
          kvList: list
        };
      break;
      case "task":
        var numTasks = d.get('data.numTasks');
        tooltipData.title = (numTasks > 1 ? '%@ Tasks' : '%@ Task').fmt(numTasks);
        node = d3.event.target;
      break;
      case "io":
        var inputs = d.get('inputs.length'),
            outputs = d.get('outputs.length'),
            title = "";
        title += (inputs > 1 ? '%@ Sources' : '%@ Source').fmt(inputs);
        title += " & ";
        title += (outputs > 1 ? '%@ Sinks' : '%@ Sink').fmt(outputs);
        tooltipData.title = title;

        node = d3.event.target;
      break;
      case "group":
        tooltipData = {
          title: d.get("vertexGroup.groupName"),
          text: d.get("vertexGroup.groupMembers").join(", ")
        };
      break;
      case "path":
        tooltipData = {
          position: {
            x: event.clientX,
            y: event.clientY
          },
          title: '%@ - %@'.fmt(
            d.get('source.name') || d.get('source.vertexName'),
            d.get('target.name') || d.get('target.vertexName')
          )
        };
        if(d.get("edgeId")) {
          tooltipData.kvList = {
            "Edge Id": d.get("edgeId"),
            "Data Movement Type": d.get("dataMovementType"),
            "Data Source Type": d.get("dataSourceType"),
            "Scheduling Type": d.get("schedulingType"),
            "Edge Destination Class": App.Helpers.misc.getClassName(d.get("edgeDestinationClass")),
            "Edge Source Class": App.Helpers.misc.getClassName(d.get("edgeSourceClass"))
          };
        }
        else {
          tooltipData.text = d.get('source.type') == "input" ? "Source link" : "Sink link";
        }
      break;
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

    _component.sendAction('entityClicked', {
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
      _scheduledClickId = setTimeout(_scheduledClick.bind(this, d, d3.event.target), 200);
    }
  }

  /**
   * Callback for mousedown & mousemove interactions. To disable click on drag
   * @param d {DataNode} Data of the clicked element
   */
  function _onMouse(d) {
    d3.select(this).on('click', d3.event.type == 'mousedown' ? _onClick : null);
  }

  /**
   * Double click event handler.
   * @param d {DataNode} Data of the clicked element
   */
  function _onDblclick(d) {
    var event = d3.event,
        node = event.target,
        dataProcessor = App.DagViewComponent.dataProcessor;

    node = node.correspondingUseElement || node;

    if(_scheduledClickId) {
      clearTimeout(_scheduledClickId);
      _scheduledClickId = 0;
    }

    switch(_getType(node)) {
      case "io":
        d.toggleAdditionalInclusion();
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
      if(sY == tY) {
        sV = 45,
        mY -= 50;
        if(sX == tX) {
          sX += _layout.linkDelta,
          tX -= _layout.linkDelta;
        }
      }
      sH = Math.abs(sX - tX) * 1.1;
    }

    return "".fmt.apply(_layout.pathDataFormat, [
      sX, sY,

      sX + sH, sY - sV,
      mX, mY,

      tX - sH, tY - sV,
      tX, tY
    ]);
  }

  /**
   * Get the node from/to which the node must transition on enter/exit
   * @param d {DataNode}
   * @param property {String} Property to be checked for
   * @return vertex node
   */
  function _getVertexNode(d, property) {
    if(d.get('vertex.' + property)) {
      return d.get('vertex');
    }
  }
  /**
   * Update position of all nodes in the list and preform required transitions.
   * @param nodes {Array} Nodes to be updated
   * @param source {d3 element} Node that trigged the update, in first update source will be root.
   */
  function _updateNodes(nodes, source) {
    // Enter any new nodes at the parent's previous position.
    nodes.enter().append('g')
      .attr('transform', function(d) {
        var node = _getVertexNode(d, "x0") || source;
        node = _layout.projector(node.x0, node.y0);
        return 'translate(' + node.x + ',' + node.y + ')';
      })
      .on({
        mouseover: _onMouseOver,
        mouseout: _tip.hide,
        mousedown: _onMouse,
        mousemove: _onMouse,
        dblclick: _onDblclick
      })
      .style('opacity', 1e-6)
      .each(_addContent);

    // Transition nodes to their new position.
    nodes.transition()
      .duration(DURATION)
      .attr('transform', function(d) {
        d = _layout.projector(d.x, d.y);
        return 'translate(' + d.x + ',' + d.y + ')';
      })
      .style('opacity', 1);

    // Transition exiting nodes to the parent's new position.
    nodes.exit().transition()
      .duration(DURATION)
      .attr('transform', function(d) {
        var node = _getVertexNode(d, "x") || source;
        node = _layout.projector(node.x, node.y);
        return 'translate(' + node.x + ',' + node.y + ')';
      })
      .style('opacity', 1e-6)
      .remove();
  }

  /**
   * Get the node from/to which the link must transition on enter/exit
   * @param d {DataNode}
   * @param property {String} Property to be checked for
   * @return node
   */
  function _getTargetNode(d, property) {
    if(d.get('target.type') == App.DagViewComponent.dataProcessor.types.OUTPUT
        && d.get('source.' + property)) {
      return d.source;
    }
    if(d.get('target.' + property)) {
      return d.target;
    }
  }
  /**
   * Update position of all links in the list and preform required transitions.
   * @param links {Array} Links to be updated
   * @param source {d3 element} Node that trigged the update, in first update source will be root.
   */
  function _updateLinks(links, source) {
    // Enter any new links at the parent's previous position.
    links.enter().insert('path', 'g')
      .attr('class', function (d) {
        var type = d.get('dataMovementType') || "";
        return 'link ' + type.toLowerCase();
      })
      .attr("style", "marker-mid: url(#arrow-marker);")
      .attr('d', function(d) {
        var node = _getTargetNode(d, "x0") || source;
        var o = {x: node.x0, y: node.y0};
        return _createPathData({source: o, target: o});
      })
      .on({
        mouseover: _onMouseOver,
        mouseout: _tip.hide
      });

    // Transition links to their new position.
    links.transition()
      .duration(DURATION)
      .attr('d', _createPathData);

    // Transition exiting nodes to the parent's new position.
    links.exit().transition()
      .duration(DURATION)
      .attr('d', function(d) {
        var node = _getTargetNode(d, "x") || source;
        var o = {x: node.x, y: node.y};
        return _createPathData({source: o, target: o});
      })
      .remove();
  }

  function _getNodeId(d) {
    return d.id || (d.id = ++_idCounter);
  }
  function _getLinkId(d) {
    return d.source.id.toString() + d.target.id;
  }
  function _stashOldPositions(d) {
    d.x0 = d.x,
    d.y0 = d.y;
  }

  /**
   * Updates position of nodes and links based on changes in _treeData.
   */
  function _update() {
    var nodesData = _treeLayout.nodes(_treeData),
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
  function _attachPanZoom(container, g) {
    var SCALE_TUNER = 1 / 700,
        MIN_SCALE = .5,
        MAX_SCALE = 2;

    var prevX = 0,
        prevY = 0,

        panX = PADDING,
        panY = PADDING,
        scale = 1;

    /**
     * Transform g to current panX, panY and scale.
     * @param animate {Boolean} Animate the transformation in DURATION time.
     */
    function transform(animate) {
      var base = animate ? g.transition().duration(DURATION) : g;
      base.attr('transform', 'translate(%@, %@) scale(%@)'.fmt(panX, panY, scale));
    }

    /**
     * Set pan values
     */
    function onMouseMove(event) {
      panX += event.pageX - prevX,
      panY += event.pageY - prevY;

      transform();

      prevX = event.pageX,
      prevY = event.pageY;
    }
    /**
     * Set zoom values, pan also would change as we are zooming with mouse position as pivote.
     */
    function onWheel(event) {
      var prevScale = scale,

          offset = container.offset(),
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

      factor = 1 - scale / prevScale,
      panX += (mouseX - panX) * factor,
      panY += (mouseY - panY) * factor;

      transform();

      _tip.reposition();
      event.preventDefault();
    }

    container
    .on('mousewheel', onWheel)
    .mousedown(function (event){
      prevX = event.pageX,
      prevY = event.pageY;

      container.on('mousemove', onMouseMove);
    })
    .mouseup(function (event){
      container.off('mousemove', onMouseMove);
    })

    /**
     * A closure to reset/modify panZoom based on an external event
     * @param newPanX {Number}
     * @param newPanY {Number}
     * @param newScale {Number}
     */
    return function (newPanX, newPanY, newScale) {
      panX = newPanX,
      panY = newPanY,
      scale = newScale;

      transform(true);
    }
  }

  /**
   * Sets the layout and update the display.
   * @param layout {Object} One of the values defined in LAYOUTS object
   */
  function _setLayout(layout) {
    var leafCount = _data.leafCount,
        dimention;

    // If count is even dummy will be replaced by output, so output would no more be leaf
    if(_data.tree.get('children.length') % 2 == 0) {
      leafCount--;
    }
    dimention = layout.projector(leafCount, _data.maxDepth - 1);

    _layout = layout;

    _width = dimention.x *= _layout.hSpacing,
    _height = dimention.y *= _layout.vSpacing;

    dimention = _layout.projector(dimention.x, dimention.y), // Because tree is always top to bottom
    _treeLayout = d3.layout.tree().size([dimention.x, dimention.y]);

    _update();
  }

  return {
    /**
     * Creates a DAG view in the given element based on the data
     * @param component {DagViewComponent} Parent ember component, to sendAction
     * @param element {HTML DOM Element} HTML element in which the view will be created
     * @param data {Object} Created by data processor
     */
    create: function (component, element, data) {
      var svg = d3.select(element).select('svg');

      _component = component,
      _data = data,
      _g = svg.append('g').attr('transform', 'translate(%@,%@)'.fmt(PADDING, PADDING));
      _svg = $(svg.node());
      _tip = App.DagViewComponent.tip;

      _tip.init($(element).find('.tool-tip'), _svg);

      _treeData = data.tree,
      _treeData.x0 = 0,
      _treeData.y0 = 0;

      _panZoom = _attachPanZoom(_svg, _g);

      _setLayout(LAYOUTS.topToBottom);
    },

    /**
     * Calling this function would fit the graph to the available space.
     */
    fitGraph: function (){
      var scale = Math.min(
        (_svg.width() - PADDING * 2) / _width,
        (_svg.height() - PADDING * 2) / _height
      );
      _panZoom(PADDING, PADDING, scale);
    },

    /**
     * Control display of additionals or sources and sinks.
     * @param hide {Boolean} If true the additionals will be excluded, else included in the display
     */
    additionalDisplay: function (hide) {
      var dataProcessor = App.DagViewComponent.dataProcessor,
          filterTypes = null;

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
      _setLayout(_layout == LAYOUTS.topToBottom ?
          LAYOUTS.leftToRight :
          LAYOUTS.topToBottom);
      return _layout == LAYOUTS.topToBottom;
    }
  };

})();
