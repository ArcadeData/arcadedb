var globalRecordEditorState = {
  active: false,
  rid: null,
  type: null,
  properties: {},
  expanded: false,
  source: null
};

function openRecordEditor(rid, type, properties, source) {
  globalRecordEditorState.active = true;
  globalRecordEditorState.rid = rid;
  globalRecordEditorState.type = type;
  globalRecordEditorState.properties = properties || {};
  globalRecordEditorState.source = source;
  globalRecordEditorState.expanded = false;

  renderRecordEditorContent();

  if (source === "table")
    showTableRecordEditor();
}

function getRecordEditorTarget() {
  if (globalRecordEditorState.source === "table")
    return "#tableRecordEditorContent";
  return "#recordEditorBody";
}

function renderRecordEditorContent() {
  var rid = globalRecordEditorState.rid;
  var type = globalRecordEditorState.type;
  var properties = globalRecordEditorState.properties;
  var source = globalRecordEditorState.source;

  var html = "";

  // Header: type badge + RID + expand/close
  html += "<div class='d-flex align-items-center justify-content-between mb-2'>";
  html += "<div>";
  html += "<span class='badge bg-primary me-2'>" + escapeHtml(type) + "</span>";
  html += "<strong>" + escapeHtml(rid) + "</strong>";
  html += "</div>";
  html += "<div class='d-flex gap-1'>";
  if (source === "graph")
    html += "<button class='btn btn-sm btn-outline-secondary' onclick='toggleRecordEditorExpand()' title='Expand/Collapse'><i class='fa fa-expand'></i></button>";
  html += "<button class='btn btn-sm btn-outline-secondary' onclick='cancelRecordEditor()' title='Close'><i class='fa fa-times'></i></button>";
  html += "</div>";
  html += "</div>";

  // Editable fields
  html += "<div class='record-editor-fields'>";
  for (var p in properties) {
    var value = properties[p];
    var isComplex = (Array.isArray(value) || typeof value === "object");
    var displayValue = isComplex ? JSON.stringify(value, null, 2) : (value != null ? String(value) : "");

    html += "<label class='form-label'>" + escapeHtml(p) + "</label>";
    if (isComplex || displayValue.length > 80) {
      html += "<textarea class='form-control record-editor-field' data-prop='" + escapeHtml(p) + "' data-original='" + escapeHtml(displayValue) + "' rows='3'>" + escapeHtml(displayValue) + "</textarea>";
    } else {
      html += "<input type='text' class='form-control record-editor-field' data-prop='" + escapeHtml(p) + "' data-original='" + escapeHtml(displayValue) + "' value='" + escapeHtml(displayValue) + "' />";
    }
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

  $(getRecordEditorTarget()).html(html);
}

function saveRecordEditor() {
  var rid = globalRecordEditorState.rid;
  var setParts = [];

  $(getRecordEditorTarget() + " .record-editor-field").each(function () {
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
  var database = escapeHtml($(".inputDatabase").val());

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
  var database = escapeHtml($(".inputDatabase").val());

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

  if (globalRecordEditorState.source === "table") {
    hideTableRecordEditor();
  } else {
    // Restore collapsed state if expanded
    if (globalRecordEditorState.expanded)
      toggleRecordEditorExpand();

    $("#recordEditorBody").html("<span id='graphPropertiesType'>Select an element to see its properties</span>");
    $("#graphActions").empty();
    $("#graphLayout").empty();
  }
}

function deleteRecord() {
  var rid = globalRecordEditorState.rid;

  globalConfirm("Delete Record", "Are you sure you want to delete record " + rid + "? This action cannot be undone.", "warning",
    function () {
      var database = escapeHtml($(".inputDatabase").val());

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

        if (globalRecordEditorState.source === "table") {
          hideTableRecordEditor();
        } else {
          $("#recordEditorBody").html("<span id='graphPropertiesType'>Select an element to see its properties</span>");
          $("#graphActions").empty();
          $("#graphLayout").empty();
        }
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

function toggleRecordEditorExpand() {
  globalRecordEditorState.expanded = !globalRecordEditorState.expanded;

  if (globalRecordEditorState.expanded) {
    $("#graphMainPanel").addClass("d-none");
    $("#graphPropertiesPanel").removeClass("col-3").addClass("col-12");
  } else {
    $("#graphMainPanel").removeClass("d-none");
    $("#graphPropertiesPanel").removeClass("col-12").addClass("col-3");
  }
}

function openRecordEditorFromTable(rid) {
  var database = escapeHtml($(".inputDatabase").val());

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
