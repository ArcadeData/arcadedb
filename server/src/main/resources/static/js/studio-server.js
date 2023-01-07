function updateServer( callback ){
  jQuery.ajax({
    type: "GET",
    url: "/api/v1/server",
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    let version = data.version;
    let pos = data.version.indexOf("(build");
    if( pos > -1 ) {
      version = version.substring( 0, pos ) + " <span style='font-size: 70%'>" + version.substring( pos ) + "</span>";
    }
    $("#serverVersion").html(version);

    if ( $.fn.dataTable.isDataTable( '#serverMetrics' ) )
      try{ $('#serverMetrics').DataTable().destroy(); $('#serverMetrics').empty(); } catch(e){};

    var tableRecords = [];

    for( let name in data.metrics.timers ){
      let timer = data.metrics.timers[name];

      let record = [];
      record.push( escapeHtml( name ) );
      record.push( escapeHtml( timer.count ) );
      record.push( escapeHtml( timer.oneMinRate ) );
      record.push( escapeHtml( timer.perc99 ) );
      record.push( escapeHtml( timer.min ) );
      record.push( escapeHtml( timer.max ) );
      record.push( escapeHtml( timer.mean ) );
      tableRecords.push( record );
    }

    for( let name in data.metrics.meters ){
      let meter = data.metrics.meters[name];

      let record = [];
      record.push( escapeHtml( name ) );
      record.push( escapeHtml( meter.count ) );
      record.push( escapeHtml( meter.oneMinRate ) );
      tableRecords.push( record );
    }

    $("#serverMetrics").DataTable({
      paging: false,
      ordering: false,
      columns: [
        {title: "Metric Name"},
        {title: "Count"},
        {title: "1 Minute Rate"},
        {title: "99 Percentile"},
        {title: "Minimum"},
        {title: "Maximum"},
        {title: "Mean"},
      ],
      data: tableRecords,
    });

    if( callback )
      callback();
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotifyError( jqXHR.responseText );
  });
}

function renderDatabases(databases){
  let result = '<table cellpadding="1" cellspacing="0" border="0" style="padding-left:50px;">';

  for( let i in databases ){
    let db = databases[i];
    result += "<tr><td>"+db.name+"</td><td><button enabled='false'></td>";
  }
  result += '</table>';

  return result;
}
