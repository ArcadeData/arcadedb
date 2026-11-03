var globalResultset = null;
var globalBgColors = [
  "aqua",
  "orange",
  "green",
  "purple",
  "lime",
  "teal",
  "maroon",
  "navy",
  "olive",
  "silver",
  "blue",
  "yellow",
  "fuchsia",
  "gray",
  "white",
  "black",
];
var globalFgColors = [
  "black",
  "black",
  "white",
  "white",
  "black",
  "black",
  "white",
  "white",
  "black",
  "black",
  "white",
  "black",
  "black",
  "white",
  "black",
  "white",
];
var globalRenderedVerticesRID = {};
var globalTotalEdges = 0;
var globalLastColorIndex = 0;
var globalGraphPropertiesPerType = {};
var globalLayout = null;
var globalCy = null;
var globalSelected = null;
var globalEnableElementPanel = true;
// LIVE PHYSICS: THE DEFAULTS A FRESH GRAPH STARTS WITH, THE RANGE THE SETTINGS PANEL SLIDERS OFFER, AND THE
// CLASS THAT MARKS A PINNED NODE. THEY LIVE HERE, NEXT TO THE SETTINGS MODEL THEY BELONG TO, SO THE SETTINGS
// PANEL, THE GRAPH WIDGET AND THE PHYSICS ENGINE ALL READ THE SAME NUMBERS.
var PHYSICS_DEFAULT_REPULSION = 8;
var PHYSICS_DEFAULT_GRAVITY = 5;
var PHYSICS_DEFAULT_EDGE_LENGTH = 10;
var PHYSICS_DEFAULT_PADDING = 5;
var PHYSICS_MAX_LEVEL = 20;
var PHYSICS_PINNED_CLASS = "physics-pinned";

var globalGraphSettings = {
  graphSpacing: 50,
  physics: true,
  physicsRepulsion: PHYSICS_DEFAULT_REPULSION,
  physicsGravity: PHYSICS_DEFAULT_GRAVITY,
  physicsEdgeLength: PHYSICS_DEFAULT_EDGE_LENGTH,
  physicsPadding: PHYSICS_DEFAULT_PADDING,
  cumulativeSelection: false,
  nodeSize: 25,
  defaultLabel: "",
  maxLabelLength: 45,
  types: {},
};

function importGraph(format) {
  var html = "<center><h5>Copy below the " + format.toUpperCase() + " of the graph to import or upload a file</h5>";
  html += "<input id='uploadFile' type='file' accept='.json, .txt, .js' />";
  html += "<center><textarea id='importContent' rows='30' cols='75'></textarea><br>";
  html += "<button id='importGraph' type='button' class='btn btn-primary'><i class='fa fa-upload'></i> Import the graph</button><br>";
  html += "<div id='importStatus'><span>&nbsp;</span><div></center>";

  $("#popupBody").html(html);

  $("#importGraph").on("click", function () {
    switch (format) {
      case "json":
        globalResultset = JSON.parse($("#importContent").val());

        if (globalResultset.settings) {
          globalGraphSettings = globalResultset.settings;
          delete globalResultset.settings;
        }

        renderGraph();
        break;
    }
    $("#popup").modal("hide");
  });

  const reader = new FileReader();
  const fileUploader = document.getElementById("uploadFile");
  fileUploader.addEventListener("change", (event) => {
    const files = event.target.files;
    const file = files[0];
    console.log("files", files);

    if (file.size > 10 * 1024 * 1024) {
      $("#importStatus").html("<span style='color:red;'>The maximum file size is 10MB.</span>");
      return;
    } else {
      $("#importStatus").html("<span style='color:green;'> The file has been uploaded successfully.</span>");
    }

    reader.onload = function () {
      $("#importContent").val(reader.result);
    };
    reader.onerror = function () {
      $("#importStatus").html("<span style='color:red;'>Error: " + reader.error + ".</span>");
    };
    reader.readAsText(file);
  });

  $("#popupLabel").text("Import Graph");

  $("#popup").on("shown.bs.modal", function () {
    $("#importContent").focus();
  });
  $("#popup").modal("show");
}

function exportGraph(format) {
  switch (format) {
    case "graphml":
      let graphml = buildGraphML(
        globalCy.nodes().map((n) => n.data()),
        globalCy.edges().map((e) => e.data()),
      );

      const blob = new Blob([graphml], { type: "text/xml" });
      saveAs(blob, "arcade.graphml");
      break;

    case "json":
      var json = JSON.parse(JSON.stringify(globalResultset));
      json.records = [];
      json.settings = globalGraphSettings;
      var jsonBlob = new Blob([JSON.stringify(json)], { type: "application/javascript;charset=utf-8" });
      saveAs(jsonBlob, "arcadedb-graph.json");
      break;

    case "png":
      var imgBlob = globalCy.png({ output: "blob" });
      saveAs(imgBlob, "arcadedb-graph.png");
      break;

    case "jpeg":
      var imgBlob = globalCy.jpg({ output: "blob" });
      saveAs(imgBlob, "arcadedb-graph.jpg");
      break;
  }
}

// GraphML export of the graph on screen, in the layout of the server's GraphML exporter (gremlin's ArcadeGraphMLWriter):
// the type under TinkerPop's labelV / labelE keys, every property of the record under a key declared with the GraphML
// type its values share, and a value GraphML has no type for (a map, a list, a number under a text key) written as text
// with its ArcadeDB type in an arcadedb:type attribute, which other readers ignore. A file exported here imports back
// into ArcadeDB with its types, and yEd, Gephi and NetworkX read it as plain GraphML.
// Takes the data of the Cytoscape elements: { id, type, properties } for nodes, plus source and target for edges.
function buildGraphML(nodes, edges) {
  const nodeKeys = new Map();
  const edgeKeys = new Map();
  const collectKeys = (elements, keys) => {
    for (const element of elements) {
      const properties = element.properties;
      for (const name in properties) {
        const value = properties[name];
        if (value != null) keys.set(name, graphMLMergeType(keys.get(name), graphMLValueType(value)));
      }
    }
  };
  collectKeys(nodes, nodeKeys);
  collectKeys(edges, edgeKeys);
  const nodeKeyIds = graphMLKeyIds(nodeKeys, edgeKeys, "V");
  const edgeKeyIds = graphMLKeyIds(edgeKeys, nodeKeys, "E");

  const out = [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<graphml xmlns="http://graphml.graphdrawing.org/xmlns" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"' +
      ' xmlns:arcadedb="https://arcadedb.com/xmlns/graphml"' +
      ' xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns http://graphml.graphdrawing.org/xmlns/1.1/graphml.xsd">',
  ];
  const writeKeys = (forElement, labelKey, keys, keyIds) => {
    out.push('  <key id="' + labelKey + '" for="' + forElement + '" attr.name="' + labelKey + '" attr.type="string"/>');
    for (const [name, type] of keys)
      out.push('  <key id="' + graphMLEscape(keyIds.get(name)) + '" for="' + forElement + '" attr.name="' + graphMLEscape(name) + '" attr.type="' + type + '"/>');
  };
  writeKeys("node", "labelV", nodeKeys, nodeKeyIds);
  writeKeys("edge", "labelE", edgeKeys, edgeKeyIds);

  const writeData = (element, labelKey, keys, keyIds) => {
    if (element.type != null) out.push('      <data key="' + labelKey + '">' + graphMLEscape(element.type) + "</data>");
    const properties = element.properties;
    for (const name in properties) {
      const value = properties[name];
      if (value == null) continue;

      const keyType = keys.get(name);
      const valueType = graphMLValueType(value);
      let hint = null;
      if (typeof value === "object") hint = "JSON";
      else if (valueType !== keyType && keyType === "string") hint = { boolean: "BOOLEAN", int: "INTEGER", long: "LONG", double: "DOUBLE" }[valueType];

      out.push(
        '      <data key="' + graphMLEscape(keyIds.get(name)) + '"' + (hint != null ? ' arcadedb:type="' + hint + '"' : "") + ">" +
          graphMLEscape(hint === "JSON" ? JSON.stringify(value) : value) + "</data>",
      );
    }
  };

  out.push('  <graph id="G" edgedefault="directed">');
  for (const node of nodes) {
    out.push('    <node id="' + graphMLEscape(node.id) + '">');
    writeData(node, "labelV", nodeKeys, nodeKeyIds);
    out.push("    </node>");
  }
  for (const edge of edges) {
    out.push('    <edge id="' + graphMLEscape(edge.id) + '" source="' + graphMLEscape(edge.source) + '" target="' + graphMLEscape(edge.target) + '">');
    writeData(edge, "labelE", edgeKeys, edgeKeyIds);
    out.push("    </edge>");
  }
  out.push("  </graph>", "</graphml>", "");
  return out.join("\n");
}

// The GraphML type of a value. An integer is int or long by its range, as a reader maps them to fixed-width integers;
// a map or a list is string, written as its JSON text.
function graphMLValueType(value) {
  if (typeof value === "boolean") return "boolean";
  if (typeof value !== "number") return "string";
  if (!Number.isInteger(value)) return "double";
  return value >= -2147483648 && value <= 2147483647 ? "int" : "long";
}

// The type of a key that has held both types: the wider number (JavaScript does not tell 2.0 from 2, so a double key
// also holds integers), or else string
function graphMLMergeType(current, next) {
  if (current == null || current === next) return next;
  const numeric = ["int", "long", "double"];
  const a = numeric.indexOf(current);
  const b = numeric.indexOf(next);
  return a >= 0 && b >= 0 ? numeric[Math.max(a, b)] : "string";
}

// Unique key ids: a name nodes and edges share, or the name of a label key, gets the suffix of its element, as the
// server's exporter does
function graphMLKeyIds(keys, otherKeys, suffix) {
  const ids = new Map();
  for (const name of keys.keys())
    ids.set(name, otherKeys.has(name) || name === "labelV" || name === "labelE" ? name + suffix : name);
  return ids;
}

// Escapes a value for XML text and double-quoted attributes, dropping the control characters XML 1.0 refuses
function graphMLEscape(value) {
  return String(value)
    .replace(/[\u0000-\u0008\u000B\u000C\u000E-\u001F￾￿]/g, "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function importSettings() {
  var html = "<center><h5>Copy below the JSON configuration to import or upload a file</h5>";
  html += "<input id='uploadFile' type='file' accept='.json, .txt, .js' />";
  html += "<center><textarea id='importContent' rows='30' cols='90'></textarea><br>";
  html += "<button id='importSettings' type='button' class='btn btn-primary'><i class='fa fa-upload'></i> Import settings</button><br>";
  html += "<div id='importStatus'><span>&nbsp;</span><div></center>";

  $("#popupBody").html(html);

  $("#importSettings").on("click", function () {
    let imported = JSON.parse($("#importContent").val());
    globalGraphSettings = imported;
    renderGraph();
    $("#popup").modal("hide");
  });

  const reader = new FileReader();
  const fileUploader = document.getElementById("uploadFile");
  fileUploader.addEventListener("change", (event) => {
    const files = event.target.files;
    const file = files[0];
    console.log("files", files);

    if (file.size > 1024 * 1024) {
      $("#importStatus").html("<span style='color:red;'>The maximum file size is 1MB.</span>");
      return;
    } else {
      $("#importStatus").html("<span style='color:green;'> The file has been uploaded successfully.</span>");
    }

    reader.onload = function () {
      $("#importContent").val(reader.result);
    };
    reader.onerror = function () {
      $("#importStatus").html("<span style='color:red;'>Error: " + reader.error + ".</span>");
    };
    reader.readAsText(file);
  });

  $("#popupLabel").text("Import Settings");

  $("#popup").on("shown.bs.modal", function () {
    $("#importContent").focus();
  });
  $("#popup").modal("show");
}

function exportSettings() {
  var html = "<center><h5>This is the JSON configuration exported</h5>";
  html += "<center><textarea id='exportContent' rows='30' cols='90' readonly></textarea><br>";
  html += "<button id='popupClipboard' type='button' class='btn btn-primary'>";
  html += "<i class='fa fa-copy'></i> Copy to clipboard and close</button> or ";
  html += "<button id='downloadFile' type='button' class='btn btn-primary'>";
  html += "<i class='fa fa-download'></i> Download it</button></center>";

  $("#popupBody").html(html);

  $("#popupLabel").text("Export Settings");

  $("#exportContent").text(JSON.stringify(globalGraphSettings, null, 2));

  // Use .off() first to prevent multiple bindings if exportSettings is called multiple times
  $("#popupClipboard").off("click").on("click", async function() {
    const text = $("#exportContent").val() || $("#exportContent").text();
    try {
      await copyTextToClipboard(text);
      $("#popup").modal("hide");
    } catch (err) {
      console.error("Failed to copy to clipboard:", err);
      globalNotify("Error", "Failed to copy to clipboard", "danger");
    }
  });

  $("#downloadFile").off("click").on("click", function () {
    var jsonBlob = new Blob([$("#exportContent").val()], { type: "application/javascript;charset=utf-8" });
    saveAs(jsonBlob, "arcadedb-studio-settings.json");
    $("#popup").modal("hide");
  });

  $("#popup").on("shown.bs.modal", function () {
    $("#exportContent").focus();
  });
  $("#popup").modal();
}

function updateGraphStatus(warning) {
  let html = "Displayed <b>" + Object.keys(globalRenderedVerticesRID).length + "</b> vertices and <b>" + globalTotalEdges + "</b> edges.";

  if (warning != null) html += " <b>WARNING</b>: " + warning;

  $("#graphStatus").html(html);
}
