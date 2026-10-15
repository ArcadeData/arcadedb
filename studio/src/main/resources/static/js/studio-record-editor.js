var globalRecordEditorState = {
  active: false,
  rid: null,
  type: null,
  properties: {},
  originalProperties: {},
  source: null
};

function openRecordEditor(rid, type, properties, source) {
  globalRecordEditorState.active = true;
  globalRecordEditorState.rid = rid;
  globalRecordEditorState.type = type;
  globalRecordEditorState.properties = properties || {};
  globalRecordEditorState.originalProperties = JSON.parse(JSON.stringify(globalRecordEditorState.properties));
  globalRecordEditorState.source = source;

  renderRecordEditorContent();

  if (source === "table")
    showTableRecordEditor();
  else
    showGraphRecordEditor();
}

// The text a stored value is shown as in the form. A null is an empty field, an object or an array is pretty-printed JSON.
// Line breaks are normalized to LF because a field cannot hold anything else (a textarea turns CRLF and a lone CR into LF),
// so an untouched multi-line value compares equal to what the field reads back and is not rewritten by a Save. Editing
// such a value does save it with LF line endings: the field the user typed in cannot hold the CRLF it started with.
function recordEditorDisplayValue(value) {
  if (value == null)
    return "";
  if (typeof value === "object")
    return JSON.stringify(value, null, 2);
  return String(value).replace(/\r\n?/g, "\n");
}

function getRecordEditorTarget() {
  if (globalRecordEditorState.source === "table")
    return "#tableRecordEditorContent";
  return "#graphRecordEditorContent";
}

function renderRecordEditorContent() {
  var rid = globalRecordEditorState.rid;
  var type = globalRecordEditorState.type;
  var properties = globalRecordEditorState.properties;
  var source = globalRecordEditorState.source;

  var html = "";

  // Header: type badge + RID + close
  html += "<div class='d-flex align-items-center justify-content-between mb-2'>";
  html += "<div>";
  html += "<span class='badge bg-primary me-2'>" + escapeHtml(type) + "</span>";
  html += "<strong>" + escapeHtml(rid) + "</strong>";
  html += "</div>";
  html += "<button class='btn btn-sm btn-outline-secondary'" + studioAction("cancelRecordEditor") + " title='Close'><i class='fa fa-times'></i></button>";
  html += "</div>";

  // Editable fields
  html += "<div class='record-editor-fields'>";
  for (var p in properties) {
    if (p === "@cat" || p === "@rid" || p === "@type")
      continue;

    var value = properties[p];
    var isComplex = (value != null && typeof value === "object");
    var displayValue = recordEditorDisplayValue(value);
    var isMetadata = p.charAt(0) === "@";
    var isBinary = (!isComplex && typeof displayValue === "string" && /^\[B@[0-9a-fA-F]+$/.test(displayValue));

    html += "<div class='record-editor-prop-row'>";
    html += "<div class='d-flex align-items-center justify-content-between'>";
    html += "<label class='form-label mb-0'>" + escapeHtml(p) + "</label>";
    if (!isMetadata && !isBinary)
      html += "<button class='btn btn-sm record-editor-delete-prop'" + studioAction("deletePropertyFromRecord", [p]) + " title='Remove property'><i class='fa fa-times'></i></button>";
    html += "</div>";
    if (isBinary) {
      var sizeInfo = properties["contentLength"] || properties[p + "Length"] || "";
      var sizeLabel = sizeInfo ? " (" + Number(sizeInfo).toLocaleString() + " bytes)" : "";
      html += "<input type='text' class='form-control' value='binary" + escapeHtml(sizeLabel) + "' disabled />";
    } else if (isComplex || displayValue.length > 80 || displayValue.indexOf("\n") >= 0)
      // the HTML parser drops ONE line feed right after <textarea>, so a value that starts with one needs this extra
      html += "<textarea class='form-control record-editor-field' data-prop='" + escapeHtml(p) + "' rows='3'" + (isMetadata ? " disabled" : "") + ">\n" + escapeHtml(displayValue) + "</textarea>";
    else
      html += "<input type='text' class='form-control record-editor-field' data-prop='" + escapeHtml(p) + "' value='" + escapeHtml(displayValue) + "'" + (isMetadata ? " disabled" : "") + " />";
    html += "</div>";
  }
  html += "<button class='btn btn-sm btn-outline-secondary w-100 mt-1'" + studioAction("addPropertyToRecord") + "><i class='fa fa-plus'></i> Add Property</button>";
  html += "</div>";

  // Action buttons
  html += "<div class='record-editor-buttons'>";
  html += "<button class='btn btn-sm btn-primary'" + studioAction("saveRecordEditor") + "><i class='fa fa-save'></i> Save</button>";
  html += "<button class='btn btn-sm btn-secondary'" + studioAction("cancelRecordEditor") + ">Cancel</button>";
  if (source === "table")
    html += "<button class='btn btn-sm btn-outline-primary'" + studioAction("showRecordInGraph") + "><i class='fa fa-project-diagram'></i> Graph</button>";
  html += "<button class='btn btn-sm btn-outline-danger ms-auto'" + studioAction("deleteRecord") + "><i class='fa fa-trash'></i> Delete</button>";
  html += "</div>";

  // Graph appearance + actions (only for graph source)
  if (source === "graph")
    html += renderGraphAppearanceSection(type, rid);

  $(getRecordEditorTarget()).html(html);

  // Bind graph appearance events after DOM is ready
  if (source === "graph")
    bindGraphAppearanceEvents(type);
}

// The graph panel's Hide button: hides the selected elements.
function hideSelectedGraphElements() {
  removeGraphElement(globalSelected);
}

function renderGraphAppearanceSection(type, rid) {
  var element = getOrCreateStyleTypeAttrib(type, "element");
  var isVertex = (element === "v");
  var properties = globalGraphPropertiesPerType[type] || {};
  var html = "";

  // --- Graph Actions ---
  html += "<div class='record-editor-section'>";
  html += "<div class='record-editor-section-header'><i class='fa fa-bolt'></i> Actions</div>";
  if (isVertex) {
    html += "<div class='d-flex gap-1 mb-2'>";
    html += "<button class='btn btn-sm btn-outline-primary flex-fill'" + studioAction("loadNodeNeighbors", ["in", rid]) + "><i class='fa fa-arrow-left'></i> Incoming</button>";
    html += "<button class='btn btn-sm btn-outline-primary flex-fill'" + studioAction("loadNodeNeighbors", ["both", rid]) + "><i class='fa fa-arrows-left-right'></i> Both</button>";
    html += "<button class='btn btn-sm btn-outline-primary flex-fill'" + studioAction("loadNodeNeighbors", ["out", rid]) + "><i class='fa fa-arrow-right'></i> Outgoing</button>";
    html += "</div>";
  }
  html += "<div class='d-flex flex-wrap gap-1'>";
  html += "<button class='btn btn-sm btn-outline-secondary'" + studioAction("hideSelectedGraphElements") + "><i class='fa fa-eye-slash'></i> Hide</button>";
  html += "<button class='btn btn-sm btn-outline-secondary'" + studioAction("selectGraphElementByType", [type]) + "><i class='fa fa-object-group'></i> Select all " + escapeHtml(type) + "</button>";
  html += "</div></div>";

  // --- Appearance Section ---
  var appearanceCollapsed = globalStorageLoad("graphAppearanceCollapsed", "false") === "true";
  html += "<div class='record-editor-section'>";
  html += "<div class='d-flex align-items-center justify-content-between'>";
  html += "<div class='record-editor-section-header' style='margin-bottom:0'><i class='fa fa-palette'></i> Appearance <small>(" + escapeHtml(type) + ")</small></div>";
  html += "<div class='d-flex gap-1'>";
  html += "<button class='btn btn-sm btn-outline-secondary record-editor-appearance-toggle' style='font-size:0.68rem; padding:1px 6px;'" + studioAction("toggleGraphAppearanceSection") + " title='Show/hide appearance'><i class='fa fa-chevron-" + (appearanceCollapsed ? "down" : "up") + "'></i></button>";
  html += "<button class='btn btn-sm btn-outline-secondary' style='font-size:0.68rem; padding:1px 6px;'" + studioAction("resetGraphTypeStyle", [type]) + " title='Reset to defaults'><i class='fa fa-rotate-left'></i> Reset</button>";
  html += "</div>";
  html += "</div>";
  html += "<div id='geAppearanceBody'" + (appearanceCollapsed ? " style='display:none'" : "") + "><div style='margin-top:10px'></div>";

  // Label
  var labelText = getOrCreateStyleTypeAttrib(type, "labelText");
  if (labelText == null) labelText = globalGraphSettings.defaultLabel != null ? globalGraphSettings.defaultLabel : "";
  html += "<div class='record-editor-appearance-row'>";
  html += "<label class='form-label'>Label</label>";
  html += "<select id='geLabel' class='form-select form-select-sm'>";
  html += "<option value=''" + (labelText === "" ? " selected" : "") + ">(none)</option>";
  html += "<option value='@type'" + (labelText === "@type" ? " selected" : "") + ">@type</option>";
  for (var p in properties) {
    if (p.charAt(0) === '@') continue;
    html += "<option value='" + escapeHtml(p) + "'" + (labelText === p ? " selected" : "") + ">" + escapeHtml(p) + "</option>";
  }
  html += "</select></div>";

  // Label size
  var labelSize = getOrCreateStyleTypeAttrib(type, "labelSize") || 0.7;
  html += "<div class='record-editor-appearance-row'>";
  html += "<label class='form-label'>Label Size</label>";
  html += "<div class='d-flex align-items-center gap-2 flex-grow-1'>";
  html += "<input type='range' class='form-range flex-grow-1' id='geLabelSize' value='" + labelSize + "' min='0.4' max='3' step='0.1'>";
  html += "<span class='record-editor-range-value' id='geLabelSizeVal'>" + labelSize + "</span>";
  html += "</div></div>";

  // Label color
  var labelColor = getOrCreateStyleTypeAttrib(type, "labelColor") || "black";
  html += "<div class='record-editor-appearance-row'>";
  html += "<label class='form-label'>Label Color</label>";
  html += renderColorSelect("geLabelColor", labelColor);
  html += "</div>";

  // Label position
  var labelPosition = getOrCreateStyleTypeAttrib(type, "labelPosition") || "center center";
  html += "<div class='record-editor-appearance-row'>";
  html += "<label class='form-label'>Label Position</label>";
  html += renderPositionSelect("geLabelPosition", labelPosition);
  html += "</div>";

  if (isVertex) {
    // Shape color
    var shapeColor = getOrCreateStyleTypeAttrib(type, "shapeColor");
    html += "<div class='record-editor-appearance-row'>";
    html += "<label class='form-label'>Fill Color</label>";
    html += renderColorSelect("geShapeColor", shapeColor);
    html += "</div>";

    // Shape size
    var shapeSize = getOrCreateStyleTypeAttrib(type, "shapeSize");
    var shapeSizeMode = (shapeSize == null || shapeSize === "data(size)") ? "auto" : "custom";
    var shapeSizeVal = (shapeSizeMode === "custom") ? parseInt(shapeSize) || 100 : 100;
    html += "<div class='record-editor-appearance-row'>";
    html += "<label class='form-label'>Node Size</label>";
    html += "<div class='d-flex align-items-center gap-2 flex-grow-1'>";
    html += "<select id='geShapeSizeMode' class='form-select form-select-sm' style='width: 80px; flex: none;'>";
    html += "<option value='auto'" + (shapeSizeMode === "auto" ? " selected" : "") + ">Auto</option>";
    html += "<option value='custom'" + (shapeSizeMode === "custom" ? " selected" : "") + ">Custom</option>";
    html += "</select>";
    html += "<input type='range' class='form-range flex-grow-1' id='geShapeSize' value='" + shapeSizeVal + "' min='20' max='500' step='10'" + (shapeSizeMode === "auto" ? " disabled" : "") + ">";
    html += "<span class='record-editor-range-value' id='geShapeSizeVal'>" + (shapeSizeMode === "auto" ? "auto" : shapeSizeVal + "px") + "</span>";
    html += "</div></div>";

    // Border
    var borderSize = getOrCreateStyleTypeAttrib(type, "borderSize") || "1";
    var borderColor = getOrCreateStyleTypeAttrib(type, "borderColor") || "gray";
    html += "<div class='record-editor-appearance-row'>";
    html += "<label class='form-label'>Border</label>";
    html += "<div class='d-flex align-items-center gap-2 flex-grow-1'>";
    html += "<input type='range' class='form-range flex-grow-1' id='geBorderSize' value='" + borderSize + "' min='0' max='10' step='1'>";
    html += "<span class='record-editor-range-value' id='geBorderSizeVal'>" + borderSize + "px</span>";
    html += renderColorSelect("geBorderColor", borderColor);
    html += "</div></div>";
  } else {
    // Edge: line width + color
    var shapeSize = getOrCreateStyleTypeAttrib(type, "shapeSize") || 1;
    var shapeColor = getOrCreateStyleTypeAttrib(type, "shapeColor") || "gray";
    html += "<div class='record-editor-appearance-row'>";
    html += "<label class='form-label'>Line</label>";
    html += "<div class='d-flex align-items-center gap-2 flex-grow-1'>";
    html += "<input type='range' class='form-range flex-grow-1' id='geShapeSize' value='" + shapeSize + "' min='0' max='20' step='1'>";
    html += "<span class='record-editor-range-value' id='geShapeSizeVal'>" + shapeSize + "px</span>";
    html += renderColorSelect("geShapeColor", shapeColor);
    html += "</div></div>";
  }

  html += "</div>"; // close geAppearanceBody
  html += "</div>"; // close record-editor-section
  return html;
}

function renderColorSelect(id, selected) {
  var html = "<select id='" + id + "' class='form-select form-select-sm record-editor-color-select'>";
  for (var i = 0; i < globalBgColors.length; i++) {
    var c = globalBgColors[i];
    html += "<option value='" + c + "'" + (selected === c ? " selected" : "") + ">" + c + "</option>";
  }
  html += "</select>";
  return html;
}

function renderPositionSelect(id, selected) {
  var positions = [
    "top left", "top center", "top right",
    "center left", "center center", "center right",
    "bottom left", "bottom center", "bottom right"
  ];
  var html = "<select id='" + id + "' class='form-select form-select-sm'>";
  for (var i = 0; i < positions.length; i++) {
    var p = positions[i];
    var label = p.split(" ").map(function(w) { return w.charAt(0).toUpperCase() + w.slice(1); }).join(" ");
    html += "<option value='" + p + "'" + (selected === p ? " selected" : "") + ">" + label + "</option>";
  }
  html += "</select>";
  return html;
}

function bindGraphAppearanceEvents(type) {
  var element = getOrCreateStyleTypeAttrib(type, "element");
  var isVertex = (element === "v");

  $("#geLabel").change(function () {
    getOrCreateStyleTypeAttrib(type, "labelText", $(this).val());
    updateLabelsForType(type);
  });

  $("#geLabelSize").on("input", function () {
    var val = $(this).val();
    $("#geLabelSizeVal").text(val);
    getOrCreateStyleTypeAttrib(type, "labelSize", val);
    renderGraph();
  });

  $("#geLabelColor").change(function () {
    getOrCreateStyleTypeAttrib(type, "labelColor", $(this).val());
    renderGraph();
  });

  $("#geLabelPosition").change(function () {
    getOrCreateStyleTypeAttrib(type, "labelPosition", $(this).val());
    renderGraph();
  });

  $("#geShapeColor").change(function () {
    getOrCreateStyleTypeAttrib(type, "shapeColor", $(this).val());
    renderGraph();
  });

  if (isVertex) {
    $("#geShapeSizeMode").change(function () {
      var mode = $(this).val();
      if (mode === "auto") {
        $("#geShapeSize").prop("disabled", true);
        $("#geShapeSizeVal").text("auto");
        getOrCreateStyleTypeAttrib(type, "shapeSize", null);
      } else {
        $("#geShapeSize").prop("disabled", false);
        var val = $("#geShapeSize").val();
        $("#geShapeSizeVal").text(val + "px");
        getOrCreateStyleTypeAttrib(type, "shapeSize", val + "px");
      }
      renderGraph();
    });

    $("#geShapeSize").on("input", function () {
      var val = $(this).val();
      $("#geShapeSizeVal").text(val + "px");
      getOrCreateStyleTypeAttrib(type, "shapeSize", val + "px");
      renderGraph();
    });

    $("#geBorderSize").on("input", function () {
      var val = $(this).val();
      $("#geBorderSizeVal").text(val + "px");
      getOrCreateStyleTypeAttrib(type, "borderSize", val);
      renderGraph();
    });

    $("#geBorderColor").change(function () {
      getOrCreateStyleTypeAttrib(type, "borderColor", $(this).val());
      renderGraph();
    });
  } else {
    $("#geShapeSize").on("input", function () {
      var val = $(this).val();
      $("#geShapeSizeVal").text(val + "px");
      getOrCreateStyleTypeAttrib(type, "shapeSize", val);
      renderGraph();
    });
  }
}

function toggleGraphAppearanceSection() {
  var body = $("#geAppearanceBody");
  var btn = $(".record-editor-appearance-toggle i");
  var isVisible = body.is(":visible");
  if (isVisible) {
    body.hide();
    btn.removeClass("fa-chevron-up").addClass("fa-chevron-down");
  } else {
    body.show();
    btn.removeClass("fa-chevron-down").addClass("fa-chevron-up");
  }
  globalStorageSave("graphAppearanceCollapsed", isVisible ? "true" : "false");
}

// The SQL literal of a field the user changed. A property that is stored as text stays text: "12345", "01234" or "true"
// edited must not be retyped to a number or a boolean (and 01234 would lose its zero). Anything else is typed by its look.
function recordEditorSqlValue(current, original) {
  // an empty field and the text "null" both mean SQL null, whatever the property held: there is no way to store the string "null"
  if (current === "" || current === "null")
    return "null";

  if (typeof original !== "string") {
    if (current === "true" || current === "false")
      return current;
    // a decimal number only: isNaN() also let Infinity and hex through, which are not SQL literals
    if (/^-?(\d+(\.\d+)?|\.\d+)([eE][-+]?\d+)?$/.test(current.trim()))
      return current.trim();
    if ((current.startsWith("{") && current.endsWith("}")) || (current.startsWith("[") && current.endsWith("]"))) {
      try {
        JSON.parse(current);
        return current;
      } catch (e) {
        // not JSON: it is text
      }
    }
  }
  return "'" + current.replace(/\\/g, "\\\\").replace(/'/g, "\\'") + "'";
}

// The UPDATE that takes the record as loaded (originalProperties) to the form as it is now (properties, after
// syncRecordEditorFields), or null when there is nothing to write. Both halves derive from the same baseline, so the
// statement does not depend on how many times the form was re-rendered in between.
function buildRecordEditorUpdate(rid, originalProperties, properties) {
  var setParts = [];
  var removeParts = [];
  var has = Object.prototype.hasOwnProperty;

  for (var p in properties) {
    if (p.charAt(0) === "@" || !has.call(properties, p))
      continue;

    // a property that was not loaded (added in this session) starts empty
    var hadOriginal = has.call(originalProperties, p);
    var current = recordEditorDisplayValue(properties[p]);
    if (current === (hadOriginal ? recordEditorDisplayValue(originalProperties[p]) : ""))
      continue;

    setParts.push(quoteSqlName(p) + " = " + recordEditorSqlValue(current, hadOriginal ? originalProperties[p] : undefined));
  }

  for (var o in originalProperties) {
    if (o.charAt(0) !== "@" && has.call(originalProperties, o) && !has.call(properties, o))
      removeParts.push(quoteSqlName(o));
  }

  if (setParts.length === 0 && removeParts.length === 0)
    return null;

  var sql = "UPDATE " + rid;
  if (setParts.length > 0)
    sql += " SET " + setParts.join(", ");
  if (removeParts.length > 0)
    sql += " REMOVE " + removeParts.join(", ");
  return sql;
}

function saveRecordEditor() {
  syncRecordEditorFields();

  var sql = buildRecordEditorUpdate(globalRecordEditorState.rid, globalRecordEditorState.originalProperties, globalRecordEditorState.properties);
  if (sql == null) {
    globalNotify("Info", "No changes to save", "warning");
    return;
  }

  var database = getCurrentDatabase();

  $.ajax({
    type: "POST",
    url: "api/v1/command/" + encodeDatabaseName(database),
    data: JSON.stringify({ language: "sql", command: sql }),
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function () {
    globalNotify("Success", "Record updated", "success", 1000);
    cancelRecordEditor();
  })
  .fail(function (jqXHR) {
    globalNotify("Error", escapeHtml(jqXHR.responseText), "danger");
  });
}

function refreshRecordAfterSave() {
  var rid = globalRecordEditorState.rid;
  var database = getCurrentDatabase();

  $.ajax({
    type: "POST",
    url: "api/v1/command/" + encodeDatabaseName(database),
    data: JSON.stringify({ language: "sql", command: "SELECT FROM " + rid, serializer: "record" }),
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function (data) {
    if (data.result && data.result.length > 0) {
      var record = data.result[0];
      var newProps = {};
      for (var key in record) {
        if (key.charAt(0) !== "@")
          newProps[key] = record[key];
      }

      globalRecordEditorState.properties = newProps;
      globalRecordEditorState.originalProperties = JSON.parse(JSON.stringify(newProps));

      // Update Cytoscape node data if in graph
      if (globalCy) {
        var node = globalCy.nodes("[id = '" + rid + "']");
        if (node.length > 0)
          node.data("properties", newProps);
      }

      renderRecordEditorContent();
    }
  })
  .fail(function (jqXHR) {
    globalNotify("Error", "Failed to refresh record: " + escapeHtml(jqXHR.responseText), "danger");
  });
}

function cancelRecordEditor() {
  globalRecordEditorState.active = false;

  if (globalRecordEditorState.source === "table")
    hideTableRecordEditor();
  else
    hideGraphRecordEditor();
}

function deleteRecord() {
  var rid = globalRecordEditorState.rid;

  globalConfirm("Delete Record", "Are you sure you want to delete record " + rid + "? This action cannot be undone.", "warning",
    function () {
      var database = getCurrentDatabase();

      $.ajax({
        type: "POST",
        url: "api/v1/command/" + encodeDatabaseName(database),
        data: JSON.stringify({ language: "sql", command: "DELETE FROM " + rid }),
        beforeSend: function (xhr) {
          xhr.setRequestHeader("Authorization", globalCredentials);
        }
      })
      .done(function () {
        globalNotify("Success", "Record " + rid + " deleted", "success");

        // Remove from Cytoscape graph if present
        if (globalCy) {
          var node = globalCy.nodes("[id = '" + rid + "']");
          if (node.length > 0)
            removeGraphElement(node);
        }

        globalRecordEditorState.active = false;

        if (globalRecordEditorState.source === "table")
          hideTableRecordEditor();
        else
          hideGraphRecordEditor();
      })
      .fail(function (jqXHR) {
        globalNotify("Error", escapeHtml(jqXHR.responseText), "danger");
      });
    }
  );
}

function addPropertyToRecord() {
  var html = "<div class='mb-3'>";
  html += "<label class='form-label'>Property name</label>";
  html += "<input type='text' class='form-control' id='newPropertyName' placeholder='Enter property name' />";
  html += "</div>";

  globalPrompt("Add Property", html, "Add", function (values) {
    var propName = (values["newPropertyName"] || "").trim();
    if (!propName) {
      globalNotify("Error", "Property name is required", "danger");
      return;
    }
    if (globalRecordEditorState.properties.hasOwnProperty(propName)) {
      globalNotify("Error", "Property already exists", "danger");
      return;
    }
    // the form holds edits that are not in the state yet: take them before the re-render replaces the fields
    syncRecordEditorFields();
    globalRecordEditorState.properties[propName] = "";
    renderRecordEditorContent();
    $(getRecordEditorTarget() + " .record-editor-field").filter(function () {
      return $(this).attr("data-prop") === propName;
    }).focus();
  });
}

function syncRecordEditorFields() {
  $(getRecordEditorTarget() + " .record-editor-field").each(function () {
    if ($(this).prop("disabled"))
      return;
    // .attr(), not .data(): data() coerces "123" or "true" into a number or a boolean
    globalRecordEditorState.properties[$(this).attr("data-prop")] = $(this).val();
  });
}

function deletePropertyFromRecord(propName) {
  syncRecordEditorFields();
  delete globalRecordEditorState.properties[propName];
  renderRecordEditorContent();
}

function showRecordInGraph() {
  var rid = globalRecordEditorState.rid;
  hideTableRecordEditor();
  globalRecordEditorState.active = false;
  addNodeFromRecord(rid);
}

function openRecordEditorFromTable(rid) {
  var database = getCurrentDatabase();

  $.ajax({
    type: "POST",
    url: "api/v1/command/" + encodeDatabaseName(database),
    data: JSON.stringify({ language: "sql", command: "SELECT FROM " + rid, serializer: "record" }),
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function (data) {
    if (data.result && data.result.length > 0) {
      var record = data.result[0];
      var type = record["@type"] || "unknown";
      var props = {};
      for (var key in record) {
        if (key.charAt(0) !== "@")
          props[key] = record[key];
      }
      openRecordEditor(rid, type, props, "table");
    }
  })
  .fail(function (jqXHR) {
    globalNotify("Error", escapeHtml(jqXHR.responseText), "danger");
  });
}

function showTableRecordEditor() {
  $("#tableRecordEditorOverlay").addClass("open");
}

function hideTableRecordEditor() {
  $("#tableRecordEditorOverlay").removeClass("open");
}

function showGraphRecordEditor() {
  $("#graphRecordEditorOverlay").addClass("open");
}

function hideGraphRecordEditor() {
  $("#graphRecordEditorOverlay").removeClass("open");
}
