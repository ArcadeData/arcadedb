var serverData = {};

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
    $("#serverConnection").html( data.user + "@" + data.serverName + " - v." + version);

    serverData = data;

    displayMetrics();
    displayServerSettings();

    if( callback )
      callback();

    setTimeout(function() {
      if( studioCurrentTab == "server" )
        updateServer();
    }, 60000);
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


function displayServerSettings(){
  if ( $.fn.dataTable.isDataTable( '#serverSettings' ) )
    try{ $('#serverSettings').DataTable().destroy(); $('#serverSettings').empty(); } catch(e){};

  var tableRecords = [];

  for( let i in serverData.settings ){
    let row = serverData.settings[i];

    let record = [];
    record.push( row.key );
    record.push( row.value );
    record.push( row.description );
    record.push( row.overridden );
    tableRecords.push( record );
  }

  $("#serverSettings").DataTable({
    paging: false,
    ordering: false,
    autoWidth: false,
    columns: [
      {title: "Key", width: "30%"},
      {title: "Value", width: "22%"},
      {title: "Description", width: "40%"},
      {title: "Overridden", width: "8%"},
    ],
    data: tableRecords,
  });
}

function displayMetrics(){
  if ( $.fn.dataTable.isDataTable( '#serverMetrics' ) )
    try{ $('#serverMetrics').DataTable().destroy(); $('#serverMetrics').empty(); } catch(e){};

  var tableRecords = [];

  for( let name in serverData.metrics.timers ){
    let timer = serverData.metrics.timers[name];

    let record = [];
    record.push( escapeHtml( name ) );
    record.push( "" );
    record.push( timer.count );
    record.push( globalFormatDouble( timer.oneMinRate ) );
    record.push( globalFormatDouble( timer.mean ) );
    record.push( globalFormatDouble( timer.perc99 ) );
    record.push(  timer.min );
    record.push( timer.max );
    tableRecords.push( record );
  }

  for( let name in serverData.metrics.meters ){
    let meter = serverData.metrics.meters[name];

    let record = [];
    record.push( escapeHtml( name ) );
    record.push( "" );
    record.push( meter.count );
    record.push( globalFormatDouble( meter.oneMinRate ) );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    tableRecords.push( record );
  }

  for( let name in serverData.metrics.profiler ){
    let record = [];

    let entry = serverData.metrics.profiler[name];
    if( entry.count == null || entry.count == 0 )
      continue;

    record.push( escapeHtml( name ) );
    record.push( "" );
    record.push( globalFormatDouble( entry.count, 0 ) );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    record.push( "" );

    tableRecords.push( record );
  }

  for( let name in serverData.metrics.profiler ){
    let record = [];

    let entry = serverData.metrics.profiler[name];
    if( entry.value == null || entry.value == 0 )
      continue;

    record.push( escapeHtml( name ) );
    record.push( globalFormatDouble( entry.value, 0 ) );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    record.push( "" );

    tableRecords.push( record );
  }

  for( let name in serverData.metrics.profiler ){
    let record = [];

    let entry = serverData.metrics.profiler[name];
    if( entry.space == null || entry.space == 0 )
      continue;

    record.push( escapeHtml( name ) );
    record.push( globalFormatSpace( entry.space ) );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    record.push( "" );
    record.push( "" );

    tableRecords.push( record );
  }

  $("#serverMetrics").DataTable({
    paging: false,
    ordering: false,
    columns: [
      {title: "Metric Name"},
      {title: "Value"},
      {title: "Count"},
      {title: "1 Minute Rate"},
      {title: "Mean (ms)"},
      {title: "99 Percentile (ms)"},
      {title: "Minimum (ms)"},
      {title: "Maximum (ms)"},
    ],
    data: tableRecords,
  });
}
