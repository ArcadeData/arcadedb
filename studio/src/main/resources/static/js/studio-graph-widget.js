var globalRenderedVerticesRID = {};
var globalTotalEdges = 0;
var globalSelected = null;

function renderGraph() {
  if (globalResultset == null) return;

  globalCy = null;

  loadGraphTypeStyles();
  $("#settingGraphSpacing").val(globalGraphSettings.graphSpacing);
  $("#settingGraphSpacingVal").text(globalGraphSettings.graphSpacing);

  let elements = [];
  globalRenderedVerticesRID = {};

  globalTotalEdges = 0;

  for (let i in globalResultset.vertices) {
    let vertex = globalResultset.vertices[i];
    assignVertexColor(vertex.t);
    assignProperties(vertex);
  }

  for (let i in globalResultset.edges) {
    let edge = globalResultset.edges[i];
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
  });

  initGraph();

  let warning = null;
  if (reachedMax) {
    warning = "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.";
    globalNotify("Warning", warning, "warning");
  }

  updateGraphStatus(warning);
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

  globalCy.layout(globalLayout).run();
}

function createVertex(vertex) {
  let type = vertex["t"];

  getOrCreateStyleTypeAttrib(type, "element", "v");

  let label = "@type";
  if (getOrCreateStyleTypeAttrib(type, "labelText") != null) label = getOrCreateStyleTypeAttrib(type, "labelText");
  if (label == "@type") label = type;
  else label = vertex["p"][label];

  if (label == null) label = "";

  return { id: vertex["r"], label: label, size: 40 + 3 * label.length, type: type, weight: vertex["i"] + vertex["o"], properties: vertex["p"] };
}

function createEdge(edge) {
  let type = edge["t"];

  getOrCreateStyleTypeAttrib(type, "element", "e");

  let label = "@type";
  if (getOrCreateStyleTypeAttrib(type, "labelText") != null) label = getOrCreateStyleTypeAttrib(type, "labelText");
  if (label == "@type") label = type;
  else label = edge["p"][label];

  if (label == null) label = "";

  return { id: edge["r"], label: label, type: type, source: edge["o"], target: edge["i"], properties: edge["p"] };
}

function assignVertexColor(type) {
  let color = getOrCreateStyleTypeAttrib(type, "shapeColor");
  if (color == null) {
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

    if (element == "e") style.style["line-color"] = shapeColor;

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

function loadNodeNeighbors(direction, rid) {
  let database = escapeHtml(getCurrentDatabase());

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery
    .ajax({
      type: "POST",
      url: "api/v1/command/" + database,
      data: JSON.stringify({
        language: "sql",
        command: "select expand( " + direction + "E() ) from " + rid,
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
        if (Object.keys(globalRenderedVerticesRID).length >= globalGraphMaxResult) {
          reachedMax = true;
          break;
        }

        let vertex = data.result.vertices[i];

        assignVertexColor(vertex.t);
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
        url: "api/v1/command/" + escapeHtml(getCurrentDatabase()),
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

function loadGraphTypeStyles() {
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
