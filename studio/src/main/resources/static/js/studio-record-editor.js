var globalRecordEditorState = {
  active: false,
  rid: null,
  type: null,
  properties: {},
  source: null
};

function openRecordEditor(rid, type, properties, source) {
  globalRecordEditorState.active = true;
  globalRecordEditorState.rid = rid;
  globalRecordEditorState.type = type;
  globalRecordEditorState.properties = properties || {};
  globalRecordEditorState.source = source;

  renderRecordEditorContent();

  if (source === "table")
    showTableRecordEditor();
  else
    showGraphRecordEditor();
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
  html += "<button class='btn btn-sm btn-outline-secondary' onclick='cancelRecordEditor()' title='Close'><i class='fa fa-times'></i></button>";
  html += "</div>";

  // Editable fields
  html += "<div class='record-editor-fields'>";
  for (var p in properties) {
    if (p === "@cat" || p === "@rid" || p === "@type")
      continue;

    var value = properties[p];
    var isComplex = (Array.isArray(value) || typeof value === "object");
    var displayValue = isComplex ? JSON.stringify(value, null, 2) : (value != null ? String(value) : "");
    var isMetadata = p.charAt(0) === "@";
    var isBinary = (!isComplex && typeof displayValue === "string" && /^\[B@[0-9a-fA-F]+$/.test(displayValue));

    html += "<label class='form-label'>" + escapeHtml(p) + "</label>";
    if (isBinary) {
      var sizeInfo = properties["contentLength"] || properties[p + "Length"] || "";
      var sizeLabel = sizeInfo ? " (" + Number(sizeInfo).toLocaleString() + " bytes)" : "";
      html += "<input type='text' class='form-control' value='binary" + escapeHtml(sizeLabel) + "' disabled />";
    } else if (isComplex || displayValue.length > 80)
      html += "<textarea class='form-control record-editor-field' data-prop='" + escapeHtml(p) + "' data-original='" + escapeHtml(displayValue) + "' rows='3'" + (isMetadata ? " disabled" : "") + ">" + escapeHtml(displayValue) + "</textarea>";
    else
      html += "<input type='text' class='form-control record-editor-field' data-prop='" + escapeHtml(p) + "' data-original='" + escapeHtml(displayValue) + "' value='" + escapeHtml(displayValue) + "'" + (isMetadata ? " disabled" : "") + " />";
  }
  html += "</div>";

  // Action buttons
  html += "<div class='record-editor-buttons'>";
  html += "<button class='btn btn-sm btn-primary' onclick='saveRecordEditor()'><i class='fa fa-save'></i> Save</button>";
  html += "<button class='btn btn-sm btn-secondary' onclick='cancelRecordEditor()'>Cancel</button>";
  if (source === "table")
    html += "<button class='btn btn-sm btn-outline-primary' onclick='showRecordInGraph()'><i class='fa fa-project-diagram'></i> Graph</button>";
  html += "<button class='btn btn-sm btn-outline-danger ms-auto' onclick='deleteRecord()'><i class='fa fa-trash'></i> Delete</button>";
  html += "</div>";

  // Graph appearance + actions (only for graph source)
  if (source === "graph")
    html += renderGraphAppearanceSection(type, rid);

  $(getRecordEditorTarget()).html(html);

  // Bind graph appearance events after DOM is ready
  if (source === "graph")
    bindGraphAppearanceEvents(type);
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
    html += "<button class='btn btn-sm btn-outline-primary flex-fill' onclick='loadNodeNeighbors(\"in\", \"" + rid + "\")'><i class='fa fa-arrow-left'></i> Incoming</button>";
    html += "<button class='btn btn-sm btn-outline-primary flex-fill' onclick='loadNodeNeighbors(\"both\", \"" + rid + "\")'><i class='fa fa-arrows-left-right'></i> Both</button>";
    html += "<button class='btn btn-sm btn-outline-primary flex-fill' onclick='loadNodeNeighbors(\"out\", \"" + rid + "\")'><i class='fa fa-arrow-right'></i> Outgoing</button>";
    html += "</div>";
  }
  html += "<div class='d-flex flex-wrap gap-1'>";
  html += "<button class='btn btn-sm btn-outline-secondary' onclick='removeGraphElement(globalSelected)'><i class='fa fa-eye-slash'></i> Hide</button>";
  html += "<button class='btn btn-sm btn-outline-secondary' onclick='selectGraphElementByType(\"" + escapeHtml(type) + "\")'><i class='fa fa-object-group'></i> Select all " + escapeHtml(type) + "</button>";
  html += "</div></div>";

  // --- Appearance Section ---
  html += "<div class='record-editor-section'>";
  html += "<div class='d-flex align-items-center justify-content-between'>";
  html += "<div class='record-editor-section-header' style='margin-bottom:0'><i class='fa fa-palette'></i> Appearance <small>(" + escapeHtml(type) + ")</small></div>";
  html += "<button class='btn btn-sm btn-outline-secondary' style='font-size:0.68rem; padding:1px 6px;' onclick='resetGraphTypeStyle(\"" + escapeHtml(type).replace(/"/g, "&quot;") + "\")' title='Reset to defaults'><i class='fa fa-rotate-left'></i> Reset</button>";
  html += "</div><div style='margin-top:10px'></div>";

  // Label
  var labelText = getOrCreateStyleTypeAttrib(type, "labelText") || "@type";
  html += "<div class='record-editor-appearance-row'>";
  html += "<label class='form-label'>Label</label>";
  html += "<select id='geLabel' class='form-select form-select-sm'>";
  html += "<option value='@type'" + (labelText === "@type" ? " selected" : "") + ">@type</option>";
  for (var p in properties)
    html += "<option value='" + escapeHtml(p) + "'" + (labelText === p ? " selected" : "") + ">" + escapeHtml(p) + "</option>";
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

  html += "</div>";
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
    renderGraph();
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

function saveRecordEditor() {
  var rid = globalRecordEditorState.rid;
  var setParts = [];

  $(getRecordEditorTarget() + " .record-editor-field").each(function () {
    if ($(this).prop("disabled"))
      return;

    var prop = $(this).data("prop");
    var original = $(this).data("original");
    var current = $(this).val();

    if (current === original)
      return;

    var sqlValue;
    if (current === "")
      sqlValue = "null";
    else if (current === "null")
      sqlValue = "null";
    else if (current === "true" || current === "false")
      sqlValue = current;
    else if (!isNaN(current) && current.trim() !== "")
      sqlValue = current;
    else if ((current.startsWith("{") && current.endsWith("}")) || (current.startsWith("[") && current.endsWith("]"))) {
      try {
        JSON.parse(current);
        sqlValue = current;
      } catch (e) {
        sqlValue = "'" + current.replace(/'/g, "\\'") + "'";
      }
    } else
      sqlValue = "'" + current.replace(/'/g, "\\'") + "'";

    setParts.push("`" + prop + "` = " + sqlValue);
  });

  if (setParts.length === 0) {
    globalNotify("Info", "No changes to save", "warning");
    return;
  }

  var sql = "UPDATE " + rid + " SET " + setParts.join(", ");
  var database = escapeHtml(getCurrentDatabase());

  $.ajax({
    type: "POST",
    url: "api/v1/command/" + database,
    data: JSON.stringify({ language: "sql", command: sql }),
    beforeSend: function (xhr) {
      xhr.setRequestHeader("Authorization", globalCredentials);
    }
  })
  .done(function () {
    globalNotify("Success", "Record updated", "success");
    refreshRecordAfterSave();
  })
  .fail(function (jqXHR) {
    globalNotify("Error", escapeHtml(jqXHR.responseText), "danger");
  });
}

function refreshRecordAfterSave() {
  var rid = globalRecordEditorState.rid;
  var database = escapeHtml(getCurrentDatabase());

  $.ajax({
    type: "POST",
    url: "api/v1/command/" + database,
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
      var database = escapeHtml(getCurrentDatabase());

      $.ajax({
        type: "POST",
        url: "api/v1/command/" + database,
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

function showRecordInGraph() {
  var rid = globalRecordEditorState.rid;
  hideTableRecordEditor();
  globalRecordEditorState.active = false;
  addNodeFromRecord(rid);
}

function openRecordEditorFromTable(rid) {
  var database = escapeHtml(getCurrentDatabase());

  $.ajax({
    type: "POST",
    url: "api/v1/command/" + database,
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
