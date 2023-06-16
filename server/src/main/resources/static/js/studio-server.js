var serverData = {};
var serverChartCPU = null;
var serverChartSpace = null;

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

    displayServerSummary();
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
    record.push( row.default );
    record.push( row.overridden );
    tableRecords.push( record );
  }

  $("#serverSettings").DataTable({
    paging: false,
    ordering: false,
    autoWidth: false,
    columns: [
      {title: "Key", width: "25%"},
      {title: "Value", width: "20%",
      render: function ( data, type, row) {
        return "<a href='#' onclick='updateServerSetting(\""+row[0]+"\", \""+row[1]+"\")' style='color: green;'><b>"+data+"</b></a>";
      }},
      {title: "Description", width: "33%"},
      {title: "Default", width: "15%"},
      {title: "Overridden", width: "7%"},
    ],
    data: tableRecords,
  });
}

function displayServerSummary(){
  // CPU CHART
  let cpuLoad = serverData.metrics.profiler.cpuLoad.space;

  var cpuOptions = {
    series: [cpuLoad, 100 - cpuLoad],
    labels: [ 'Used', 'Available' ],
    chart: {
      type: 'donut',
      selection: { enable: false },
      height: 200,
      toolbar: { show: false }
    },
    legend: { show: false },
    tooltip: {
      enabled: false,
    },
    dataLabels: {
      enabled: false,
      formatter: function (val) {
        return globalFormatDouble( val, 0 ) + "%"
      }
    },
    plotOptions: {
      pie: { expandOnClick: false, donut: { labels: { show: true, name: { show: true }, value: { formatter: () => globalFormatDouble( cpuLoad, 0 ) + '%' }, total: { show: true, label: "CPU", formatter: () => globalFormatDouble( cpuLoad, 0 ) + '%' } } } }
    }
  };

  if( serverChartCPU != null )
    serverChartCPU.destroy();

  serverChartCPU = new ApexCharts(document.querySelector("#serverChartCPU"), cpuOptions);
  serverChartCPU.render();

  // SPACE CHART
  let ramHeapUsed = serverData.metrics.profiler.ramHeapUsed.space;
  let ramHeapMax = serverData.metrics.profiler.ramHeapMax.space;

  let ramOsUsed = serverData.metrics.profiler.ramOsUsed.space;
  let ramOsTotal = serverData.metrics.profiler.ramOsTotal.space;

  let readCacheUsed = serverData.metrics.profiler.readCacheUsed.space;
  let cacheMax = serverData.metrics.profiler.cacheMax.space;

  let diskFreeSpace = serverData.metrics.profiler.diskFreeSpace.space;
  let diskTotalSpace = serverData.metrics.profiler.diskTotalSpace.space;

  var spaceOptions = {
    series: [{
      name: 'Used',
      data: [ramHeapUsed, ramOsUsed, readCacheUsed, diskTotalSpace - diskFreeSpace]
    }, {
      name: 'Available',
      data: [ramHeapMax - ramHeapUsed, ramOsTotal - ramOsUsed, cacheMax - readCacheUsed, diskFreeSpace ]
    }],
    chart: {
      type: 'bar',
      height: 400,
      stacked: true,
      stackType: '100%',
      toolbar: { show: false }
    },
    legend: { show: false },
    plotOptions: {
      bar: {
        horizontal: true,
      },
    },
    stroke: {
      width: 1,
      colors: ['#fff']
    },
    xaxis: {
      categories: ["Server RAM", "OS RAM", "Cache", "Disk"],
    },
    tooltip: {
      y: {
        formatter: function (val) {
          return globalFormatDouble( val / ( 1024 * 1024 * 1024 ), 2 ) + "GB"
        }
      }
    },
    fill: {
      opacity: 1
    }
  };

  if( serverChartSpace != null )
    serverChartSpace.destroy();

  serverChartSpace = new ApexCharts(document.querySelector("#serverChartSpace"), spaceOptions);
  serverChartSpace.render();
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

function updateServerSetting(key, value){
  let html = "<b>" + key + "</b> = <input id='updateSettingInput' value='"+value+"'>";
  html += "<br><p><i>The update will not be persistent and will be reset at the next restart of the server.</i></p>";

  Swal.fire({
    title: "Update Server Setting",
    html: html,
    showCancelButton: true,
    width: 600,
    confirmButtonColor: '#3ac47d',
    cancelButtonColor: 'red',
  }).then((result) => {
    if (result.value) {
      jQuery.ajax({
       type: "POST",
       url: "/api/v1/server",
       data: JSON.stringify(
         {
           language: "sql",
           command: "set server setting " + key + " " +$("#updateSettingInput").val()
         }
       ),
       beforeSend: function (xhr){
         xhr.setRequestHeader('Authorization', globalCredentials);
       }
      })
      .done(function(data){
        if( data.error ) {
          $("#authorizationCodeMessage").html(data.error);
          return false;
        }
        displayServerSettings();
        return true;
      });
    }
  });
}
