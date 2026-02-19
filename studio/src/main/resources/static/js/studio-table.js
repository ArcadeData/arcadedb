function renderTable() {
  if (globalResultset == null) return;

  let tableTruncateColumns = $("#tableTruncateColumns").prop("checked");
  if (tableTruncateColumns == null) {
    let saved = globalStorageLoad("table.truncateColumns");
    if (saved == null) tableTruncateColumns = true; // DEFAULT
    else tableTruncateColumns = saved == "true";
  }
  let tableTruncateColumnsChecked = tableTruncateColumns;
  if (tableTruncateColumnsChecked == true) tableTruncateColumnsChecked = "checked";
  else tableTruncateColumnsChecked = "";

  let tableFitInPage = $("#tableFitInPage").prop("checked");
  if (tableFitInPage == null) {
    let saved = globalStorageLoad("table.fitInPage");
    if (saved == null) tableFitInPage = true; // DEFAULT
    else tableFitInPage = saved == "true";
  }
  let tableFitInPageChecked = tableFitInPage;
  if (tableFitInPageChecked == true) tableFitInPageChecked = "checked";
  else tableFitInPageChecked = "";

  if (tableFitInPage == true) $("#result").css({ width: "100%", "table-layout": "fixed", "white-space": "normal" });
  else $("#result").css({ width: "100%", "table-layout": "auto", "white-space": "nowrap" });

  if ($.fn.dataTable.isDataTable("#result"))
    try {
      $("#result").DataTable().destroy();
      $("#result").empty();
    } catch (e) {}

  var tableColumns = [];
  var tableRecords = [];
  var metadataColumns = ["@rid", "@type", "@cat", "@in", "@out"];

  if (globalResultset.records.length > 0) {
    let columns = {};
    for (let i in globalResultset.records) {
      let row = globalResultset.records[i];

      for (let p in row) {
        if (!columns[p]) columns[p] = true;
      }
    }

    let orderedColumns = [];
    if (columns["@rid"]) orderedColumns.push("@rid");
    if (columns["@type"]) orderedColumns.push("@type");

    for (let colName in columns) {
      if (!metadataColumns.includes(colName)) orderedColumns.push(colName);
    }

    if (columns["@in"]) orderedColumns.push("@in");
    if (columns["@out"]) orderedColumns.push("@out");

    for (let i in orderedColumns) {
      if (orderedColumns[i] == "@rid")
        tableColumns.push({
          title: escapeHtml(orderedColumns[i]),
          render: function (data, type, full) {
            return $("<div/>").html(data).text();
          },
        });
      else
        tableColumns.push({
          title: escapeHtml(orderedColumns[i]),
          defaultContent: "",
          // Use explicit text rendering to properly display special characters (#1602)
          render: function (data, type, full) {
            if (type === "display" || type === "filter") {
              // For display and filtering, use HTML-escaped text
              return data;
            }
            // For sorting, export, etc., return the raw text
            return $("<div/>").html(data).text();
          },
        });
    }

    if (Object.keys(columns).length == 0) return;

    for (let i in globalResultset.records) {
      let row = globalResultset.records[i];

      let record = [];
      for (let i in orderedColumns) {
        let colName = orderedColumns[i];
        let value = row[colName];

        if (colName == "@rid") {
          //RID
          value = "<a class='link' onclick=\"addNodeFromRecord('" + value + "')\">" + value + "</a>";
        } else if (value != null && (typeof value === "string" || value instanceof String) && value.toString().length > 30) {
          if (tableTruncateColumns) value = value.toString().substr(0, 30) + "...";
        }

        if (value == null) value = "<null>";
        record.push(escapeHtml(value));
      }
      tableRecords.push(record);
    }
  }

  if (globalResultset.records.length > 0) {
    $("#result").DataTable({
      orderCellsTop: true,
      fixedHeader: true,
      paging: true,
      pageLength: 20,
      lengthChange: true,
      columns: tableColumns,
      data: tableRecords,
      deferRender: true,
      dom: "<Blf>rt<ip>",
      order: [],
      lengthMenu: [
        [10, 20, 50, 100, -1],
        ["10", "20", "50", "100", "all"],
      ],
      buttons: [
        { extend: "copy", text: "<i class='fas fa-copy'></i> Copy" },
        { extend: "excel", text: "<i class='fas fa-file-excel'></i> Excel" },
        { extend: "csv", text: "<i class='fas fa-file-csv'></i> CSV" },
        { extend: "pdf", text: "<i class='fas fa-file-pdf'></i> PDF", orientation: "landscape" },
        { extend: "print", text: "<i class='fas fa-print'></i> Print", orientation: "landscape" },
      ],
      initComplete: function () {
        $(this.api().table().container()).find("input").attr("autocomplete", "off");

        let wrapper = $(this.api().table().container());

        // Build custom export dropdown
        let exportHtml = "<div class='dt-export-dropdown'>";
        exportHtml += "<button class='dt-export-btn' onclick='toggleExportMenu(this)' title='Export'><i class='fa fa-arrow-up-from-bracket'></i></button>";
        exportHtml += "<div class='dt-export-menu'></div>";
        exportHtml += "</div>";

        let exportEl = $(exportHtml);
        let menu = exportEl.find(".dt-export-menu");

        // Move DataTables buttons into the dropdown menu
        wrapper.find(".dt-buttons .dt-button").each(function () {
          $(this).removeClass("dt-button btn-secondary").addClass("dt-export-menu-item");
          menu.append($(this));
        });
        wrapper.find(".dt-buttons").remove();

        // Insert export dropdown after search
        let searchBox = wrapper.find(".dt-search");
        if (searchBox.length)
          searchBox.after(exportEl);
        else
          wrapper.prepend(exportEl);
      },
    });

    $(".dt-length").css("padding", "7px");
    $(".dt-search").css("padding", "7px");
  }

  // FORCE RESET OF THE SEARCH FIELD
  $("#result_filter>label>input").val("");
}

function toggleExportMenu(btn) {
  let menu = $(btn).siblings(".dt-export-menu");
  let isOpen = menu.hasClass("open");

  // Close any open export menus
  $(".dt-export-menu").removeClass("open");

  if (!isOpen) {
    // Position menu below the button using fixed positioning
    let rect = btn.getBoundingClientRect();
    menu.css({
      top: rect.bottom + 4 + "px",
      left: (rect.right - menu.outerWidth()) + "px"
    });
    menu.addClass("open");
    // Recalc left after menu is visible and has width
    let menuWidth = menu.outerWidth();
    menu.css("left", (rect.right - menuWidth) + "px");

    // Close on outside click
    $(document).one("click", function (e) {
      if (!$(e.target).closest(".dt-export-dropdown").length)
        $(".dt-export-menu").removeClass("open");
    });
  }
}
