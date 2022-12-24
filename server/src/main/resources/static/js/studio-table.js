function renderTable(){
  if( globalResultset == null )
    return;

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

    for( let i in globalResultset.records ){
      let row = globalResultset.records[i];

      let record = [];
      for( let i in orderedColumns ){
        let colName = orderedColumns[i];
        let value = row[colName];

        if( colName == "@rid" ){
          //RID
          value = "<a class='link' onclick=\"addNodeFromRecord('"+value+"')\">"+value+"</a>";
        } else if ( value != null && ( typeof value === 'string' || value instanceof String) && value.toString().length > 30 )
          value = value.toString().substr( 0, 30) + "...";

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
        {
          extend: 'pdf',
          text: "<i class='fas fa-file-pdf'></i> PDF",
          className: 'btn btn-secondary',
          orientation: 'landscape',
        },
        {
          extend: 'print',
          text: "<i class='fas fa-print'></i> Print",
          className: 'btn btn-secondary',
          orientation: 'landscape',
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
