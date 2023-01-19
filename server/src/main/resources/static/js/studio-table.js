function renderTable(){
  if( globalResultset == null )
    return;

  let tableTruncateColumns = $("#tableTruncateColumns").prop("checked");
  if( tableTruncateColumns == null ){
    let saved = globalStorageLoad( "table.truncateColumns" );
    if( saved == null )
      tableTruncateColumns = true; // DEFAULT
    else
      tableTruncateColumns = saved == "true";
  }
  let tableTruncateColumnsChecked = tableTruncateColumns;
  if( tableTruncateColumnsChecked == true )
    tableTruncateColumnsChecked = "checked";
  else
    tableTruncateColumnsChecked = "";

  let tableFitInPage = $("#tableFitInPage").prop("checked");
  if( tableFitInPage == null ) {
    let saved = globalStorageLoad( "table.fitInPage" );
    if( saved == null )
      tableFitInPage = true; // DEFAULT
    else
      tableFitInPage = saved == "true";
  }
  let tableFitInPageChecked = tableFitInPage;
  if( tableFitInPageChecked == true )
    tableFitInPageChecked = "checked";
  else
    tableFitInPageChecked = "";

  if( tableFitInPage == true)
    $("#result").css({"width": "100%", "table-layout": "fixed", "white-space": "normal"});
  else
    $("#result").css({"width": "100%", "table-layout": "auto", "white-space": "nowrap"});

  if ( $.fn.dataTable.isDataTable( '#result' ) )
    try{ $('#result').DataTable().destroy(); $('#result').empty(); } catch(e){};

  var tableColumns = [];
  var tableRecords = [];
  var metadataColumns = ["@rid", "@type", "@cat", "@in", "@out"];

  if( globalResultset.records.length > 0 ) {
    let columns = {};
    for( let i in globalResultset.records ){
      let row = globalResultset.records[i];

      for( let p in row ){
        if( !columns[p] )
          columns[p] = true;
      }
    }

    let orderedColumns = [];
    if( columns["@rid"])
      orderedColumns.push("@rid");
    if( columns["@type"])
      orderedColumns.push("@type");

    for( let colName in columns ){
      if( !metadataColumns.includes(colName) )
        orderedColumns.push(colName);
    }

    if( columns["@in"])
      orderedColumns.push("@in");
    if( columns["@out"])
      orderedColumns.push("@out");

    for( let i in orderedColumns ){
      if( orderedColumns[i] == "@rid" )
        tableColumns.push( { "mRender": function ( data, type, full ) {
                                                   return $("<div/>").html(data).text();
                                                   } } );
      else
        tableColumns.push( { sTitle: escapeHtml( orderedColumns[i] ), "defaultContent": "" } );
    }

    if( Object.keys(columns).length == 0 )
      return;

    for( let i in globalResultset.records ){
      let row = globalResultset.records[i];

      let record = [];
      for( let i in orderedColumns ){
        let colName = orderedColumns[i];
        let value = row[colName];

        if( colName == "@rid" ){
          //RID
          value = "<a class='link' onclick=\"addNodeFromRecord('"+value+"')\">"+value+"</a>";
        } else if ( value != null && ( typeof value === 'string' || value instanceof String) && value.toString().length > 30 ){
          if( tableTruncateColumns )
            value = value.toString().substr( 0, 30) + "...";
        }

        if( value == null )
          value = "<null>";
        record.push( escapeHtml( value ) );
      }
      tableRecords.push( record );
    }
  }

  if( globalResultset.records.length > 0 ) {
    $("#result").DataTable({
      orderCellsTop: true,
      fixedHeader: true,
      paging:   true,
      pageLength: 20,
      bLengthChange: true,
      aoColumns: tableColumns,
      aaData: tableRecords,
      deferRender: true,
      dom: '<Blf>rt<ip>',
      order: [],
      lengthMenu: [
        [ 10, 20, 50, 100, -1 ],
        [ '10', '20', '50', '100', 'all' ]
      ],
      buttons: [
        { extend: 'copy',
          text: "<i class='fas fa-copy'></i> Copy",
          className: 'btn btn-secondary',
        },
        { extend: 'excel',
          text: "<i class='fas fa-file-excel'></i> Excel",
          className: 'btn btn-secondary',
        },
        { extend: 'csv',
          text: "<i class='fas fa-file-csv'></i> CSV",
          className: 'btn btn-secondary',
        },
        { extend: 'pdf',
          text: "<i class='fas fa-file-pdf'></i> PDF",
          className: 'btn btn-secondary',
          orientation: 'landscape',
        },
        { extend: 'print',
          text: "<i class='fas fa-print'></i> Print",
          className: 'btn btn-secondary',
          orientation: 'landscape',
        },
        { text: "<a class='nav-link dropdown-toggle btn btn-secondary' href='#' role='button' aria-haspopup='true' aria-expanded='false' class='dropdown-toggle' data-toggle='dropdown'>" +
                "    <i class='fa fa-sliders-h'></i> Settings" +
                "  </a>" +
                "  <ul class='dropdown-menu dropdown-menu-right' aria-labelledby='navbarDropdown' style='width: 300px'>" +
                "    <li class='dropdown-item'>" +
                "      <div class='form-check'>" +
                "        <input id='tableTruncateColumns' class='form-check-input' type='checkbox' " + tableTruncateColumnsChecked + " onclick='globalCheckboxAndSave(\"#tableTruncateColumns\", \"table.truncateColumns\");renderTable()'>" +
                "        <label for='tableTruncateColumns' class='form-label' onclick='globalToggleCheckboxAndSave(\"#tableTruncateColumns\", \"table.truncateColumns\");renderTable()'>Truncate long values</label>" +
                "      </div>" +
                "    </li>" +
                "    <li class='dropdown-item'>" +
                "      <div class='form-check'>" +
                "        <input id='tableFitInPage' class='form-check-input' type='checkbox' " + tableFitInPageChecked + " onclick='globalCheckboxAndSave(\"#tableFitInPage\", \"table.fitInPage\");renderTable()'>" +
                "        <label for='tableFitInPage' class='form-label' onclick='globalToggleCheckboxAndSave(\"#tableFitInPage\", \"table.fitInPage\");renderTable()'>Fit table in page</label>" +
                "      </div>" +
                "    </li>" +
                "  </ul>"
        },
      ],
      initComplete: function() {
        $(this.api().table().container()).find('input').attr('autocomplete', 'off');
      },
    });

    $('.dt-buttons').css('padding', '7px');
    $('.dataTables_length').css('padding', '7px');
    $('.dataTables_filter').css('padding', '7px');
    $('.buttons-copy').removeClass('buttons-copy').removeClass('buttons-html5');
    $('.buttons-excel').removeClass('buttons-excel').removeClass('buttons-html5');
    $('.buttons-csv').removeClass('buttons-csv').removeClass('buttons-html5');
    $('.buttons-pdf').removeClass('buttons-pdf').removeClass('buttons-html5');
    $('.buttons-print').removeClass('buttons-print').removeClass('buttons-html5');
  }

  // FORCE RESET OF THE SEARCH FIELD
  $("#result_filter>label>input").val("");
}
