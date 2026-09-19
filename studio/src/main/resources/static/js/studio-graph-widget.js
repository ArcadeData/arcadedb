var globalRenderedVerticesRID = {};
var globalTotalEdges = 0;
var globalSelected = null;

function renderGraph() {
  if (globalResultset == null) return;

  globalCy = null;

  loadGraphTypeStyles();
  $("#settingGraphSpacing").val(globalGraphSettings.graphSpacing);
  $("#settingGraphSpacingVal").text(globalGraphSettings.graphSpacing);
  $("#settingNodeSize").val(globalGraphSettings.nodeSize || 25);
  $("#settingNodeSizeVal").text(globalGraphSettings.nodeSize || 25);
  $("#settingDefaultLabel").val(globalGraphSettings.defaultLabel != null ? globalGraphSettings.defaultLabel : "");
  let maxLabelVal = globalGraphSettings.maxLabelLength || 45;
  $("#settingMaxLabel").val(maxLabelVal);
  $("#settingMaxLabelVal").text(maxLabelVal == 0 ? "off" : maxLabelVal);

  let elements = [];
  globalRenderedVerticesRID = {};

  globalTotalEdges = 0;

  for (let i in globalResultset.vertices) {
    let vertex = globalResultset.vertices[i];
    assignTypeColor(vertex.t);
    assignProperties(vertex);
  }

  for (let i in globalResultset.edges) {
    let edge = globalResultset.edges[i];
    assignTypeColor(edge.t);
    assignProperties(edge);
  }

  let reachedMax = false;
  for (let i in globalResultset.vertices) {
    let vertex = globalResultset.vertices[i];

    let rid = vertex["r"];
    if (rid == null) continue;

    let v = { data: createVertex(vertex), classes: vertex["t"] };
    elements.push(v);

    globalRenderedVerticesRID[rid] = true;

    if (elements.length >= globalGraphMaxResult) {
      reachedMax = true;
      break;
    }
  }

  for (let i in globalResultset.edges) {
    let edge = globalResultset.edges[i];
    if (globalRenderedVerticesRID[edge.i] && globalRenderedVerticesRID[edge.o]) {
      // DISPLAY ONLY EDGES RELATIVE TO VERTICES THAT ARE PART OF THE GRAPH
      elements.push({ data: createEdge(edge), classes: edge["t"] });
      ++globalTotalEdges;
    }
  }

  let randomize = false;
  if (globalGraphSettings._spacingChanged) {
    randomize = true;
    globalGraphSettings._spacingChanged = false;
  }

  globalLayout = {
    name: "fcose",
    animate: true,
    animationDuration: 500,
    nodeSeparation: globalGraphSettings.graphSpacing * 3,
    idealEdgeLength: globalGraphSettings.graphSpacing * 3,
    quality: "default",
    randomize: randomize,
    fit: true,
    padding: 30,
  };

  let styles = [
    {
      selector: "node",
      style: {
        label: "data(label)",
        width: "data(size)",
        height: "data(size)",
        "border-color": "gray",
        "border-width": 0,
        "text-valign": "center",
        "text-halign": "center",
        "text-wrap": "wrap",
        "text-max-width": "data(size)",
        "z-index-compare": "manual",
        "z-index": 2,
      },
    },
    {
      selector: "edge",
      style: {
        width: 1,
        label: "data(label)",
        color: "gray",
        "line-color": "gray",
        "target-arrow-color": "gray",
        "target-arrow-shape": "triangle",
        "curve-style": "bezier",
        "edge-text-rotation": "autorotate",
        "text-outline-color": "#F7F7F7",
        "text-outline-width": 8,
        "z-index-compare": "manual",
        "z-index": 1,
      },
    },
  ];

  assignStyles(styles);

  // ADD SELECTED STYLES TO GET PRIORITY OVER OTHER STYLES
  styles.push({
    selector: "node:selected",
    style: {
      color: "red",
      "border-color": "red",
      "border-width": 10,
      "z-index-compare": "manual",
      "z-index": 3,
    },
  });

  styles.push({
    selector: "edge:selected",
    style: {
      color: "red",
      "line-color": "red",
      width: 10,
      "z-index-compare": "manual",
      "z-index": 3,
    },
  });

  globalCy = cytoscape({
    container: $("#graph"),
    elements: elements,
    style: styles,
    maxZoom: 2.0,
  });

  initGraph();

  let warning = null;
  if (reachedMax) {
    warning = "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.";
    globalNotify("Warning", warning, "warning");
  }

  updateGraphStatus(warning);
}

/**
 * Appends new query results to the existing graph (cumulative mode).
 * Skips vertices/edges already present. Runs layout on new nodes only.
 */
function appendToGraph(newResult) {
  if (globalCy == null || newResult == null) return;

  var added = [];

  for (var i in newResult.vertices) {
    var vertex = newResult.vertices[i];
    var rid = vertex["r"];
    if (rid == null || globalRenderedVerticesRID[rid]) continue;

    assignTypeColor(vertex.t);
    assignProperties(vertex);

    globalResultset.vertices.push(vertex);
    globalRenderedVerticesRID[rid] = true;

    var node = globalCy.add([{ group: "nodes", data: createVertex(vertex), classes: vertex["t"] }]);
    added.push(node[0]);
  }

  for (var i in newResult.edges) {
    var edge = newResult.edges[i];
    if (!globalRenderedVerticesRID[edge.i] || !globalRenderedVerticesRID[edge.o]) continue;

    // Skip if edge already in graph
    if (edge.r && globalCy.getElementById(edge.r).length > 0) continue;

    assignTypeColor(edge.t);
    assignProperties(edge);

    globalResultset.edges.push(edge);
    globalCy.add([{ group: "edges", data: createEdge(edge), classes: edge["t"] }]);
    ++globalTotalEdges;
  }

  // Merge records
  if (newResult.records)
    for (var i in newResult.records)
      globalResultset.records.push(newResult.records[i]);

  if (added.length > 0) {
    // Run layout only on the new nodes so existing positions are preserved
    var newCollection = globalCy.collection(added);
    newCollection.layout({
      name: "fcose",
      animate: true,
      animationDuration: 300,
      nodeSeparation: globalGraphSettings.graphSpacing * 3,
      idealEdgeLength: globalGraphSettings.graphSpacing * 3,
      quality: "default",
      randomize: true,
      fit: false
    }).run();

    setGraphStyles();
  }

  updateGraphStatus(null);
}

function initGraph() {
  setGraphStyles();

  globalCy.cxtmenu({
    selector: "node",
    menuRadius: function (ele) {
      return 50;
    },
    adaptativeNodeSpotlightRadius: true,
    openMenuEvents: "taphold",
    commands: [
      {
        content: '<span class="fa fa-eye-slash fa-2x"></span>',
        select: function (ele) {
          removeGraphElement(ele);
        },
      },
      {
        content: '<span class="fa fa-project-diagram fa-2x"></span>',
        select: function (ele) {
          loadNodeNeighbors("both", ele.data("id"));
        },
      },
      {
        content: '<span class="fa fa-arrow-circle-right fa-2x"></span>',
        select: function (ele) {
          loadNodeNeighbors("out", ele.data("id"));
        },
      },
      {
        content: '<span class="fa fa-arrow-circle-left fa-2x"></span>',
        select: function (ele) {
          loadNodeNeighbors("in", ele.data("id"));
        },
      },
      {
        // Expand a chosen subset instead of everything (issue #7847). The three commands above are the whole
        // neighbourhood at once, which on a node with thousands of edges buries the graph it was meant to show.
        content: '<span class="fa fa-filter fa-2x"></span>',
        select: function (ele) {
          expandNodePrompt(ele.data("id"));
        },
      },
    ],
  });

  globalCy.cxtmenu({
    selector: "edge",
    adaptativeNodeSpotlightRadius: false,
    commands: [
      {
        content: '<span class="fa fa-eye-slash fa-2x"></span>',
        select: function (ele) {
          removeGraphElement(ele);
        },
      },
    ],
  });

  globalCy.on("select", "node", function (event) {
    displaySelectedNode();
  });

  globalCy.on("select", "edge", function (event) {
    displaySelectedEdge();
  });

  // Show full label tooltip on hover for truncated labels
  let tooltipEl = null;
  globalCy.on("mouseover", "node", function (event) {
    let node = event.target;
    let fullLabel = node.data("fullLabel");
    let label = node.data("label");
    if (fullLabel && fullLabel !== label) {
      if (!tooltipEl) {
        tooltipEl = document.createElement("div");
        tooltipEl.style.cssText = "position:absolute;background:#333;color:#fff;padding:4px 8px;border-radius:4px;font-size:12px;max-width:400px;word-wrap:break-word;pointer-events:none;z-index:10000;";
        document.body.appendChild(tooltipEl);
      }
      tooltipEl.textContent = fullLabel;
      let pos = node.renderedPosition();
      let container = globalCy.container().getBoundingClientRect();
      tooltipEl.style.left = (container.left + pos.x + 10) + "px";
      tooltipEl.style.top = (container.top + pos.y - 30) + "px";
      tooltipEl.style.display = "block";
    }
  });
  globalCy.on("mouseout", "node", function () {
    if (tooltipEl) tooltipEl.style.display = "none";
  });

  globalCy.layout(globalLayout).run();
}

function createVertex(vertex) {
  let type = vertex["t"];

  getOrCreateStyleTypeAttrib(type, "element", "v");

  let label = resolveLabel(type, vertex["p"]);
  let fullLabel = resolveFullLabel(type, vertex["p"]);

  let nodeSize = globalGraphSettings.nodeSize || 25;
  let maxAutoSize = nodeSize * 8;
  let size = label.length > 0 ? Math.min(nodeSize + 6 * label.length, maxAutoSize) : nodeSize;

  return { id: vertex["r"], label: label, fullLabel: fullLabel, size: size, type: type, weight: vertex["i"] + vertex["o"], properties: vertex["p"] };
}

function createEdge(edge) {
  let type = edge["t"];

  getOrCreateStyleTypeAttrib(type, "element", "e");

  let label = resolveLabel(type, edge["p"]);

  return { id: edge["r"], label: label, type: type, source: edge["o"], target: edge["i"], properties: edge["p"] };
}

function resolveLabel(type, properties) {
  let labelProp = getOrCreateStyleTypeAttrib(type, "labelText");
  if (labelProp == null) {
    let defaultLabel = globalGraphSettings.defaultLabel;
    labelProp = defaultLabel != null ? defaultLabel : "";
  }

  if (labelProp == "@type") return type;
  if (labelProp.length == 0) return "";

  let label = properties[labelProp];
  if (label == null) return "";

  label = String(label);
  let maxLen = globalGraphSettings.maxLabelLength || 45;
  if (maxLen > 0 && label.length > maxLen)
    label = label.substring(0, maxLen) + "...";
  return label;
}

function resolveFullLabel(type, properties) {
  let labelProp = getOrCreateStyleTypeAttrib(type, "labelText");
  if (labelProp == null) {
    let defaultLabel = globalGraphSettings.defaultLabel;
    labelProp = defaultLabel != null ? defaultLabel : "";
  }

  if (labelProp == "@type") return type;
  if (labelProp.length == 0) return "";

  let label = properties[labelProp];
  return label != null ? String(label) : "";
}

function updateLabelsForType(type) {
  if (globalCy == null) return;

  let nodeSize = globalGraphSettings.nodeSize || 25;
  let maxAutoSize = nodeSize * 8;
  let isEdge = getOrCreateStyleTypeAttrib(type, "element") === "e";

  globalCy.elements("." + type).forEach(function (ele) {
    let props = ele.data("properties");
    let label = resolveLabel(type, props || {});
    let fullLabel = resolveFullLabel(type, props || {});
    ele.data("label", label);
    ele.data("fullLabel", fullLabel);
    if (!isEdge)
      ele.data("size", label.length > 0 ? Math.min(nodeSize + 6 * label.length, maxAutoSize) : nodeSize);
  });
}

function assignTypeColor(type) {
  // Preserve any color already set (either default or user override).
  if (getOrCreateStyleTypeAttrib(type, "shapeColor") != null) return;

  let sidebarColor = typeof globalSidebarTypeColors !== "undefined" ? globalSidebarTypeColors[type] : null;
  if (sidebarColor != null) {
    getOrCreateStyleTypeAttrib(type, "shapeColor", sidebarColor);
    if (getOrCreateStyleTypeAttrib(type, "labelColor") == null)
      getOrCreateStyleTypeAttrib(type, "labelColor", "white");
  } else {
    if (globalLastColorIndex >= globalBgColors.length) globalLastColorIndex = 0;
    getOrCreateStyleTypeAttrib(type, "labelColor", globalFgColors[globalLastColorIndex]);
    getOrCreateStyleTypeAttrib(type, "shapeColor", globalBgColors[globalLastColorIndex]);
    ++globalLastColorIndex;
  }
}

function assignProperties(element) {
  let type = element["t"];
  let properties = globalGraphPropertiesPerType[type];
  if (properties == null) {
    properties = {};
    globalGraphPropertiesPerType[type] = properties;
  }

  for (let p in element.p) properties[p] = true;
}

function assignStyles(styles) {
  if (styles == null) styles = [];

  for (let type in globalGraphSettings.types) {
    let element = getOrCreateStyleTypeAttrib(type, "element");

    let labelColor = getOrCreateStyleTypeAttrib(type, "labelColor");
    if (labelColor == null) labelColor = "black";

    let borderColor = getOrCreateStyleTypeAttrib(type, "borderColor");
    if (borderColor == null) borderColor = "gray";

    let shapeColor = getOrCreateStyleTypeAttrib(type, "shapeColor");
    let icon = getOrCreateStyleTypeAttrib(type, "icon");

    let shapeSize = getOrCreateStyleTypeAttrib(type, "shapeSize");
    if (shapeSize == null) shapeSize = element == "v" ? "data(size)" : 1;

    let labelSize = getOrCreateStyleTypeAttrib(type, "labelSize");
    if (labelSize == null) labelSize = 0.7;

    let style = {
      selector: "." + type,
      style: {
        width: shapeSize,
        height: shapeSize,
        color: labelColor,
        "background-color": shapeColor,
        "font-size": labelSize + "em",
        "z-index": element == "v" ? 2 : 1,
      },
    };

    if (element == "e") {
      style.style["line-color"] = shapeColor;
      style.style["target-arrow-color"] = shapeColor;
      style.style["color"] = shapeColor;
    }

    let labelPosition = getOrCreateStyleTypeAttrib(type, "labelPosition");
    let borderSize = getOrCreateStyleTypeAttrib(type, "borderSize");

    if (icon != null) {
      if (labelPosition == null) {
        labelPosition = "bottom center";
        getOrCreateStyleTypeAttrib(type, "labelPosition", labelPosition);
      }
      labelPosition = labelPosition.split(" ");

      if (borderSize == null) borderSize = 0;

      style.style["background-opacity"] = 0;
      style.style["text-max-width"] = 200;
    } else {
      if (labelPosition == null) labelPosition = "center center";
      labelPosition = labelPosition.split(" ");

      if (borderSize == null) borderSize = 1;

      style.style["text-max-width"] = "data(size)";
    }

    style.style["border-color"] = borderColor;
    style.style["border-width"] = borderSize;
    style.style["text-valign"] = labelPosition[0];
    style.style["text-halign"] = labelPosition[1];

    styles.push(style);
    styles.push({
      selector: "." + type + ":selected",
      style: {
        "border-color": "red",
        "border-width": 5,
      },
    });
  }

  return styles;
}

function setGraphStyles() {
  let nodeHtmlStyles = [];
  for (let type in globalGraphSettings.types) {
    let iconColor = getOrCreateStyleTypeAttrib(type, "iconColor");
    if (iconColor == null) iconColor = "black";

    let icon = getOrCreateStyleTypeAttrib(type, "icon");
    if (icon != null) {
      let iconSize = getOrCreateStyleTypeAttrib(type, "iconSize");
      if (iconSize == null) iconSize = 2;

      let iconPosition = getOrCreateStyleTypeAttrib(type, "iconPosition");
      if (iconPosition == null) iconPosition = "center center";
      iconPosition = iconPosition.split(" ");

      nodeHtmlStyles.push({
        query: "." + type,
        valign: iconPosition[0],
        halign: iconPosition[1],
        valignBox: "center",
        tpl: function (data) {
          return "<span style='font-size: " + iconSize + "em; color: " + iconColor + "'><i class='" + icon + "'></i></span>";
        },
      });
    }
  }
  globalCy.nodeHtmlLabel(nodeHtmlStyles);
}

function toggleWorkareaFullscreen() {
  let root = document.getElementById("tab-query");
  if (root == null) return;

  let active = root.classList.toggle("workarea-fullscreen");

  document.querySelectorAll(".fullscreen-toggle-btn").forEach(function (btn) {
    btn.title = active ? "Exit fullscreen" : "Maximize";
  });

  setTimeout(function () {
    if (globalCy != null) {
      globalCy.resize();
      globalCy.fit(undefined, 30);
    }
    if (window.jQuery && $.fn.dataTable && $.fn.dataTable.isDataTable("#result")) {
      try { $("#result").DataTable().columns.adjust(); } catch (e) { /* ignore */ }
    }
  }, 50);
}

function removeGraphElement(ele) {
  if (ele == null) return;

  globalCy.remove(ele);

  // Handle both single elements and collections
  let elements;
  if (ele.length !== undefined) {
    // It's a collection (like globalSelected from Cytoscape)
    elements = ele;
  } else {
    // It's a single element
    elements = [ele];
  }

  try {
    for (let i = 0; i < elements.length; i++) {
      let element = elements[i];

      // Skip if not a valid Cytoscape element
      if (!element || typeof element.data !== 'function') continue;

      let rid = element.data().id;

      arrayRemoveAll(globalResultset.vertices, (row) => row.r == rid);
      let edgeRemoved = arrayRemoveAll(globalResultset.edges, (row) => row.r == rid || row.i == rid || row.o == rid);
      globalTotalEdges -= edgeRemoved.length;
      delete globalRenderedVerticesRID[rid];
    }
  } finally {
    updateGraphStatus();
  }
}

/**
 * A RID as it can be embedded in a SQL statement, or null when it is not one.
 *
 * Every RID these builders receive comes from a Cytoscape node this Studio rendered from a server answer, so it
 * is already well formed - the check is here because the alternative to validating is concatenating whatever
 * reached the node data into a command, and a validated shape costs nothing on a path that runs once per click.
 */
function sqlRid(rid) {
  return typeof rid === "string" && /^#\d+:\d+$/.test(rid) ? rid : null;
}

/**
 * The command that counts this node's edges per type in one direction, so the expansion picker can show how big
 * each choice is BEFORE it is made (issue #7847).
 *
 * Aggregated on the server on purpose: the whole point is not to pull a supernode's edges down in order to find
 * out how many there are.
 */
function edgeTypeCountsCommand(direction, rid) {
  const safeRid = sqlRid(rid);
  if (safeRid === null) return null;
  return "select @type as type, count(*) as total from (select expand( " + direction + "E() ) from " + safeRid + ") group by @type";
}

/**
 * The expansion itself: every edge of this node in `direction`, or only those of the chosen types, with an
 * optional ceiling.
 *
 * The type names travel as named PARAMETERS rather than as quoted literals. A type name is free-form - it can
 * contain a quote - and the escaping convention for a SQL string literal here is backslash rather than doubling,
 * which is exactly the kind of detail a builder gets wrong once and then carries. A parameter has no convention
 * to get wrong. The limit cannot be one (it is not an expression position), so it is coerced to a positive
 * integer and omitted when it is not one.
 *
 * Returns { command, params } or null when the RID is not one.
 */
function neighborExpansionCommand(direction, rid, edgeTypes, limit) {
  const safeRid = sqlRid(rid);
  if (safeRid === null) return null;

  const params = {};
  let args = "";
  const types = Array.isArray(edgeTypes) ? edgeTypes : [];
  for (let i = 0; i < types.length; i++) {
    const name = "t" + i;
    params[name] = types[i];
    args += (i > 0 ? ", " : "") + ":" + name;
  }

  let command = "select expand( " + direction + "E(" + args + ") ) from " + safeRid;

  const ceiling = parseInt(limit, 10);
  if (ceiling > 0) command += " limit " + ceiling;

  return { command: command, params: params };
}

/**
 * Adds a node's neighbours to the graph.
 *
 * @param direction "out", "in" or "both"
 * @param rid       the node to expand
 * @param edgeTypes optional array of edge type names; empty or absent means every type
 * @param limit     optional ceiling on the number of edges fetched
 */
function loadNodeNeighbors(direction, rid, edgeTypes, limit) {
  let database = getCurrentDatabase();

  const expansion = neighborExpansionCommand(direction, rid, edgeTypes, limit);
  if (expansion === null) {
    globalNotify("Error", "Cannot expand '" + escapeHtml(String(rid)) + "': not a record id", "danger");
    return;
  }

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/command/" + encodeDatabaseName(database),
      data: JSON.stringify({
        language: "sql",
        command: expansion.command,
        params: expansion.params,
        serializer: "studio",
      }),
      beforeSend: function (xhr) {
        xhr.setRequestHeader("Authorization", globalCredentials);
      },
    })
    .done(function (data) {
      globalCy.startBatch();

      let reachedMax = false;
      for (let i in data.result.vertices) {
        let vertex = data.result.vertices[i];

        // Already on the canvas. Cytoscape THROWS on a second element with the same id, and the throw would
        // escape before endBatch(), leaving the graph wedged mid-batch. Overlapping expansions used to be an
        // edge case; with the relationship picker, expanding one type and then another from the same node is
        // the ordinary way to use it, so the two expansions share their endpoints by construction (#7939
        // review). Checked before the ceiling so a re-expansion cannot be counted against it.
        if (globalRenderedVerticesRID[vertex.r]) continue;

        if (Object.keys(globalRenderedVerticesRID).length >= globalGraphMaxResult) {
          reachedMax = true;
          break;
        }

        assignTypeColor(vertex.t);
        assignProperties(vertex);

        globalResultset.vertices.push(vertex);

        globalCy.add([
          {
            group: "nodes",
            data: createVertex(vertex),
            classes: vertex["t"],
          },
        ]);

        globalRenderedVerticesRID[vertex.r] = true;
      }

      for (let i in data.result.edges) {
        let edge = data.result.edges[i];

        if (!globalRenderedVerticesRID[edge.i] || !globalRenderedVerticesRID[edge.o]) continue;
        // Same duplicate-id throw as the vertices above, and reached the same way: a self-loop, or a second
        // expansion of a node whose edge is already drawn.
        if (globalCy.getElementById(edge.r).nonempty()) continue;

        assignTypeColor(edge.t);
        assignProperties(edge);

        globalResultset.edges.push(edge);
        globalCy.add([
          {
            group: "edges",
            data: createEdge(edge),
            classes: edge["t"],
          },
        ]);

        ++globalTotalEdges;
      }

      let typeStyles = assignStyles();
      for (i in typeStyles) {
        let s = typeStyles[i];
        globalCy.style().selector(s.selector).style(s.style);
      }

      setGraphStyles();

      globalCy.makeLayout(globalLayout).run();

      globalCy.endBatch();

      let warning = null;
      if (reachedMax) {
        warning = "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.";
        globalNotify("Warning", warning, "warning");
      }

      updateGraphStatus(warning);
    })
    .fail(function (jqXHR, textStatus, errorThrown) {
      globalNotify("Error", escapeHtml(jqXHR.responseText), "danger");
    })
    .always(function (data) {
      $("#executeSpinner").hide();
    });
}

/**
 * Turns the per-type count answer into rows the picker renders, biggest first.
 *
 * Biggest first because the reason the picker exists is a node with thousands of edges: the type that would
 * flood the canvas is the one the operator has to see, and it must not be the row they have to scroll to.
 */
function parseEdgeTypeCounts(data) {
  const rows = [];
  // Two shapes, because two serializers produce them. 'record' - what the count query asks for, it being an
  // aggregate and not a graph - answers a flat array in `result`. The 'studio' serializer every other call on
  // this page uses answers an OBJECT, {vertices, edges, records}, and puts a non-element row in `records`; a
  // reader that knew only the array shape found nothing there and the picker reported "no connections" for
  // every node (PR #7939 review). Both are read, so the parser survives the call site changing serializer.
  const payload = data && data.result ? data.result : [];
  const result = Array.isArray(payload) ? payload : Array.isArray(payload.records) ? payload.records : [];
  for (let i = 0; i < result.length; i++) {
    const type = result[i].type;
    if (type == null) continue;
    rows.push({ type: String(type), total: Number(result[i].total) || 0 });
  }
  rows.sort(function (a, b) {
    return b.total - a.total || a.type.localeCompare(b.type);
  });
  return rows;
}

/**
 * The word an operator reads for a direction. One source for it, because the picker names a direction in three
 * places - the row label, the timeout message, the tooltip - and a message built by concatenation produced
 * "ingoing" where the table beside it said "incoming" (PR #7939 review).
 */
function directionLabel(direction) {
  return direction === "in" ? "incoming" : "outgoing";
}

/** Groups the picker's checked rows back into one expansion per direction. */
function groupSelectedEdgeTypes(selected) {
  const grouped = { out: [], in: [] };
  for (let i = 0; i < selected.length; i++) {
    const choice = selected[i];
    if (grouped[choice.direction] && grouped[choice.direction].indexOf(choice.type) < 0)
      grouped[choice.direction].push(choice.type);
  }
  return grouped;
}

/**
 * Opens the expansion picker for a node (issue #7847).
 *
 * The three radial commands beside it expand the WHOLE neighbourhood, which on a node with many connections
 * produces the hairball the issue was reported with: the graph stops showing anything. This asks the server how
 * many edges of each type the node has - an aggregate, so a supernode's edges are not pulled down just to be
 * counted - and lets the operator expand only what they came for, with a ceiling.
 */
function expandNodePrompt(rid) {
  const safeRid = sqlRid(rid);
  if (safeRid === null) {
    globalNotify("Error", "Cannot expand '" + escapeHtml(String(rid)) + "': not a record id", "danger");
    return;
  }

  const database = getCurrentDatabase();
  const counts = { out: null, in: null };

  $("#executeSpinner").show();

  // The picker opens only once BOTH directions have answered, so a request that never resolves would leave the
  // spinner up and the picker unopened, with nothing said. A hung socket has no answer of its own to wait for,
  // and jQuery sets no timeout by default; the ceiling is generous because the request is an aggregate over a
  // supernode's edges, and a timeout lands in the .fail arm below, which reports it and opens the picker on
  // whichever direction did answer (PR #7939 review).
  const COUNT_TIMEOUT_MS = 120000;

  ["out", "in"].forEach(function (direction) {
    jQuery
      .ajax({
        type: "POST",
        url: "api/v1/command/" + encodeDatabaseName(database),
        data: JSON.stringify({
          language: "sql",
          command: edgeTypeCountsCommand(direction, safeRid),
          // 'record', not the 'studio' serializer the expansions use: these rows are an aggregate, with no
          // element in them to expand into a graph document, and the flat array is what the counts are.
          serializer: "record",
        }),
        timeout: COUNT_TIMEOUT_MS,
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function (data) {
        counts[direction] = parseEdgeTypeCounts(data);
      })
      .fail(function (jqXHR, textStatus) {
        // One direction failing must not strand the picker: report it and carry on with the other, which is
        // still a usable answer.
        counts[direction] = [];
        globalNotify(
          "Error",
          textStatus === "timeout"
            ? "Counting the " + directionLabel(direction) + " relationships timed out"
            : escapeHtml(jqXHR.responseText),
          "danger"
        );
      })
      .always(function () {
        if (counts.out === null || counts.in === null) return;
        $("#executeSpinner").hide();
        showExpandNodeModal(safeRid, counts.out, counts.in);
      });
  });
}

/** The picker itself: one row per (direction, edge type) with its edge count, plus a ceiling. */
function showExpandNodeModal(rid, outRows, inRows) {
  if (outRows.length === 0 && inRows.length === 0) {
    globalNotify("Expand", "This node has no connections to expand", "info");
    return;
  }

  let rows = "";
  let total = 0;

  function appendRows(direction, list, arrow, label) {
    for (let i = 0; i < list.length; i++) {
      const row = list[i];
      total += row.total;
      rows +=
        '<tr><td style="width:2rem;">' +
        '<input class="form-check-input expand-edge-type" type="checkbox" checked ' +
        'data-direction="' + direction + '" data-type="' + escapeHtml(row.type) + '"></td>' +
        '<td style="width:6rem;" title="' + label + '">' + arrow + " " + label + "</td>" +
        "<td><b>" + escapeHtml(row.type) + "</b></td>" +
        '<td class="text-end">' + row.total + "</td></tr>";
    }
  }

  appendRows("out", outRows, '<i class="fa fa-arrow-right"></i>', directionLabel("out"));
  appendRows("in", inRows, '<i class="fa fa-arrow-left"></i>', directionLabel("in"));

  const html =
    '<div class="text-muted small mb-2">' +
    escapeHtml(rid) +
    " has <b>" +
    total +
    "</b> connection(s). Choose which ones to add to the graph.</div>" +
    '<div class="table-responsive" style="max-height:18rem; overflow-y:auto;">' +
    '<table class="table table-sm table-striped mb-0" id="expandNodeTable"><tbody>' +
    rows +
    "</tbody></table></div>" +
    '<div class="mt-3">' +
    '<label for="expandNodeLimit" class="form-label" style="font-size:0.85rem;">Max elements per direction</label>' +
    '<input type="number" min="1" class="form-control" id="expandNodeLimit" value="' +
    globalGraphMaxResult +
    '">' +
    "</div>";

  globalPrompt("Expand " + rid, html, "Expand", function () {
    const selected = [];
    $("#expandNodeTable input.expand-edge-type:checked").each(function () {
      // attr(), not data(): jQuery's data() coerces a data-* attribute that LOOKS like a literal, so an edge
      // type genuinely named "null", "true" or "42" would arrive as the value rather than as its name and the
      // expansion would ask for something that does not exist (PR #7939 review).
      selected.push({ direction: $(this).attr("data-direction"), type: $(this).attr("data-type") });
    });

    if (selected.length === 0) {
      globalNotify("Expand", "No relationship selected, nothing to add", "info");
      return;
    }

    const limit = parseInt($("#expandNodeLimit").val(), 10);
    const grouped = groupSelectedEdgeTypes(selected);

    // One request per direction rather than one per type: outE('A','B') is a single traversal, and the graph
    // then lays out both additions together instead of jumping once per type.
    if (grouped.out.length > 0) loadNodeNeighbors("out", rid, grouped.out, limit);
    if (grouped.in.length > 0) loadNodeNeighbors("in", rid, grouped.in, limit);
  });
}

function addNodeFromRecord(rid) {
  if (globalResultset == null) globalResultset = {};
  if (globalResultset.vertices == null) globalResultset.vertices = [];
  if (globalResultset.edges == null) globalResultset.edges = [];
  if (globalResultset.records == null) globalResultset.records = [];

  let vertex = null;
  for (let i in globalResultset.vertices) {
    let v = globalResultset.vertices[i];
    if (v.r == rid) {
      vertex = v;
      break;
    }
  }

  if (vertex == null) {
    // LOAD FROM THE DATABASE
    jQuery
      .ajax({
        type: "POST",
        url: "api/v1/command/" + encodeDatabaseName(getCurrentDatabase()),
        async: false,
        data: JSON.stringify({
          language: "sql",
          command: "select from " + rid,
          serializer: "graph",
        }),
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        },
      })
      .done(function (data) {
        vertex = data.result.vertices[0];
        globalResultset.vertices.push(vertex);
      });
  }

  if (globalCy != null) globalCy.elements("node:selected").unselect();
  else renderGraph();

  globalActivateTab("tab-graph");

  let node = globalCy.nodes("[id = '" + rid + "']")[0];

  node.select();

  globalCy.makeLayout(globalLayout).run();
}

function getOrCreateStyleTypeAttrib(type, attrib, value) {
  let style = globalGraphSettings.types[type];
  if (style == null) {
    style = {};
    globalGraphSettings.types[type] = style;
  }

  if (typeof value !== "undefined") {
    style[attrib] = value;
    saveGraphTypeStyles();
  }

  return style[attrib];
}

function saveGraphTypeStyles() {
  globalStorageSave("graphTypeStyles", JSON.stringify(globalGraphSettings.types));
}

function saveGraphGlobalSettings() {
  globalStorageSave("graphGlobalSettings", JSON.stringify({
    nodeSize: globalGraphSettings.nodeSize,
    defaultLabel: globalGraphSettings.defaultLabel,
    graphSpacing: globalGraphSettings.graphSpacing,
    cumulativeSelection: globalGraphSettings.cumulativeSelection,
    maxLabelLength: globalGraphSettings.maxLabelLength
  }));
}

function loadGraphTypeStyles() {
  var savedGlobal = globalStorageLoad("graphGlobalSettings", null);
  if (savedGlobal != null) {
    try {
      var parsed = JSON.parse(savedGlobal);
      if (parsed.nodeSize != null) globalGraphSettings.nodeSize = parsed.nodeSize;
      if (parsed.defaultLabel != null) globalGraphSettings.defaultLabel = parsed.defaultLabel;
      if (parsed.graphSpacing != null) globalGraphSettings.graphSpacing = parsed.graphSpacing;
      if (parsed.cumulativeSelection != null) globalGraphSettings.cumulativeSelection = parsed.cumulativeSelection;
      if (parsed.maxLabelLength != null) globalGraphSettings.maxLabelLength = parsed.maxLabelLength;
    } catch (e) { /* ignore corrupt data */ }
  }

  var saved = globalStorageLoad("graphTypeStyles", null);
  if (saved != null) {
    try {
      var parsed = JSON.parse(saved);
      for (var type in parsed) {
        if (globalGraphSettings.types[type] == null)
          globalGraphSettings.types[type] = {};
        var style = globalGraphSettings.types[type];
        var savedStyle = parsed[type];
        for (var attr in savedStyle)
          style[attr] = savedStyle[attr];
      }
    } catch (e) { /* ignore corrupt data */ }
  }
}

function resetGraphTypeStyle(type) {
  delete globalGraphSettings.types[type];
  saveGraphTypeStyles();
  renderGraph();
  // Re-open editor with refreshed appearance
  if (globalRecordEditorState.active && globalRecordEditorState.source === "graph")
    renderRecordEditorContent();
}

function displaySelectedNode() {
  if (!globalEnableElementPanel) return;

  globalSelected = globalCy.elements("node:selected");
  if (globalSelected.length < 1) {
    cancelRecordEditor();
    return;
  }

  let data = null;
  if (globalSelected.length == 1) {
    data = globalSelected[0].data();
    openRecordEditor(data.id, data.type, data.properties, "graph");
  }
}

function displaySelectedEdge() {
  globalSelected = globalCy.elements("edge:selected");
  if (globalSelected.length < 1) {
    cancelRecordEditor();
    return;
  }

  if (globalSelected.length == 1) {
    let data = globalSelected[0].data();
    openRecordEditor(data.id, data.type, data.properties, "graph");
  }
}
