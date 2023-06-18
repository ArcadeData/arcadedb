var GB_SIZE = 1024 * 1024 * 1024;

var lastUpdate = null;
var refreshTimeout = null;
var serverData = {};
var serverChartOSCPU = null;
var serverChartOSRAM = null;
var serverChartOSDisk = null;
var serverChartServerRAM = null;
var serverChartCache = null;
var serverChartCommands = null;
var reqPerSecLastMinute = {};

function updateServer( callback ){
  let currentDate = new Date();
  let currentSecond = currentDate.getHours() + ":" + currentDate.getMinutes() + ":" + currentDate.getSeconds();
  if( currentSecond == lastUpdate )
    // SKIP SAME SECOND
    return;

  lastUpdate = currentSecond;

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

    let serverInfo = "Connected to <b>" + data.user + "@" + data.serverName + "</b> - v." + version;
    if( data.metrics.profiler.configuration.description )
      serverInfo += "<br>Runs on " + data.metrics.profiler.configuration.description;

    $("#serverConnection").html( serverInfo );

    serverData = data;

    displayServerSummary();
    displayMetrics();
    displayServerSettings();

    if( callback )
      callback();

    if( refreshTimeout != null )
      clearTimeout( refreshTimeout );

    refreshTimeout = setTimeout(function() {
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
  // COMMANDS
  let currentDate = new Date();
  let x = currentDate.getHours() + ":" + currentDate.getMinutes() + ":" + currentDate.getSeconds();
  if( reqPerSecLastMinute.length > 0 && x == reqPerSecLastMinute[0].x )
    // SKIP SAME SECOND
    return;

  let series = [];
  for( commandsMetricName in serverData.metrics.meters ) {
    let metric = serverData.metrics.meters[commandsMetricName];
    let array = reqPerSecLastMinute[commandsMetricName];
    if( !array ) {
      array = [];
      reqPerSecLastMinute[commandsMetricName] = array;
    }
    array.unshift( { x: x, y: metric.reqPerSecSinceLastTime } );

    if( array.length > 50 )
      // KEEP ONLY THE LATEST 50 VALUES
      array.pop();

    series.push( { name: commandsMetricName, data: array } );
  }

  var serverCommandsOptions = {
    series: series,
    labels: [ 'Used', 'Available' ],
    chart: { type: 'line', height: 300, animations: { enabled: false, speed: 50 } },
    legend: { show: false },
    tooltip: { enabled: true },
    fill: {  opacity: [0.24, 1, 1] },
    dataLabels: { enabled: true },
    stroke: { curve: 'smooth' },
    grid: {
      borderColor: '#e7e7e7',
      row: {
        colors: ['#f3f3f3', 'transparent'], // takes an array which will be repeated on columns
        opacity: 0.5
      },
    },
    markers: { size: 1 },
    yaxis: { title: { text: 'Req/Sec' } },
  };

  if( serverChartCommands != null )
    serverChartCommands.destroy();

  serverChartCommands = new ApexCharts(document.querySelector("#serverChartCommands"), serverCommandsOptions);
  serverChartCommands.render();

  // CPU CHART
  let cpuLoad = serverData.metrics.profiler.cpuLoad.space;

  var cpuOptions = {
    series: [cpuLoad, 100 - cpuLoad],
    labels: [ 'Used', 'Available' ],
    chart: { type: 'donut', selection: { enable: false }, height: 300, toolbar: { show: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: { enabled: false, formatter: function (val) { return globalFormatDouble( val, 0 ) + "%" } },
    plotOptions: {
      pie: { expandOnClick: false, donut: { labels: { show: true, name: { show: true }, value: { formatter: () => globalFormatDouble( cpuLoad, 0 ) + '%' }, total: { show: true, label: "OS CPU", formatter: () => globalFormatDouble( cpuLoad, 0 ) + '%' } } } }
    }
  };

  if( serverChartOSCPU != null )
    serverChartOSCPU.destroy();

  serverChartOSCPU = new ApexCharts(document.querySelector("#serverChartOSCPU"), cpuOptions);
  serverChartOSCPU.render();

  // OS RAM
  let ramOsUsed = serverData.metrics.profiler.ramOsUsed.space;
  let ramOsTotal = serverData.metrics.profiler.ramOsTotal.space;

  var serverRamOSOptions = {
    series: [ramOsUsed, ramOsTotal - ramOsUsed],
    labels: [ 'Used', 'Available' ],
    chart: { type: 'donut', selection: { enable: false }, height: 300, toolbar: { show: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: { enabled: false, formatter: function (val) { return globalFormatDouble( val, 0 ) + "%" } },
    plotOptions: {
      pie: { expandOnClick: false, donut: { labels: { show: true, name: { show: true }, value: { formatter: (val) => globalFormatDouble( val / GB_SIZE, 2 ) + "GB" }, total: { show: true, label: "OS RAM", formatter: () => globalFormatDouble( ramOsUsed / GB_SIZE, 2 ) + 'GB' } } } }
    }
  };

  if( serverChartOSRAM != null )
    serverChartOSRAM.destroy();

  serverChartOSRAM = new ApexCharts(document.querySelector("#serverChartOSRAM"), serverRamOSOptions);
  serverChartOSRAM.render();

  // OS DISK
  let diskFreeSpace = serverData.metrics.profiler.diskFreeSpace.space;
  let diskTotalSpace = serverData.metrics.profiler.diskTotalSpace.space;

  var serverDiskOSOptions = {
    series: [diskTotalSpace - diskFreeSpace, diskFreeSpace],
    labels: [ 'Used', 'Available' ],
    chart: { type: 'donut', selection: { enable: false }, height: 300, toolbar: { show: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: { enabled: false, formatter: function (val) { return globalFormatDouble( val, 0 ) + "%" } },
    plotOptions: {
      pie: { expandOnClick: false, donut: { labels: { show: true, name: { show: true }, value: { formatter: (val) => globalFormatDouble( val / GB_SIZE, 2 ) + "GB" }, total: { show: true, label: "OS DISK", formatter: () => globalFormatDouble( ( diskTotalSpace - diskFreeSpace ) / GB_SIZE, 2 ) + 'GB' } } } }
    }
  };

  if( serverChartOSDisk != null )
    serverChartOSDisk.destroy();

  serverChartOSDisk = new ApexCharts(document.querySelector("#serverChartOSDisk"), serverDiskOSOptions);
  serverChartOSDisk.render();

  // SERVER RAM
  let ramHeapUsed = serverData.metrics.profiler.ramHeapUsed.space;
  let ramHeapMax = serverData.metrics.profiler.ramHeapMax.space;

  var serverRamOptions = {
    series: [ramHeapUsed, ramHeapMax - ramHeapUsed],
    labels: [ 'Used', 'Available' ],
    chart: { type: 'donut', selection: { enable: false }, height: 300, toolbar: { show: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: { enabled: false, formatter: function (val) { return globalFormatDouble( val, 0 ) + "%" } },
    plotOptions: {
      pie: { expandOnClick: false, donut: { labels: { show: true, name: { show: true }, value: { formatter: (val) => globalFormatDouble( val / GB_SIZE, 2 ) + "GB" }, total: { show: true, label: "Server RAM", formatter: () => globalFormatDouble( ramHeapUsed / GB_SIZE, 2 ) + 'GB' } } } }
    }
  };

  if( serverChartServerRAM != null )
    serverChartServerRAM.destroy();

  serverChartServerRAM = new ApexCharts(document.querySelector("#serverChartServerRAM"), serverRamOptions);
  serverChartServerRAM.render();

  // CACHE
  let readCacheUsed = serverData.metrics.profiler.readCacheUsed.space;
  let cacheMax = serverData.metrics.profiler.cacheMax.space;

  var serverCacheOptions = {
    series: [readCacheUsed, cacheMax - readCacheUsed],
    labels: [ 'Used', 'Available' ],
    chart: { type: 'donut', selection: { enable: false }, height: 300, toolbar: { show: false } },
    legend: { show: false },
    tooltip: { enabled: false },
    dataLabels: { enabled: false, formatter: function (val) { return globalFormatDouble( val, 0 ) + "%" } },
    plotOptions: {
      pie: { expandOnClick: false, donut: { labels: { show: true, name: { show: true }, value: { formatter: (val) => globalFormatDouble( val / GB_SIZE, 2 ) + "GB" }, total: { show: true, label: "Server Cache", formatter: () => globalFormatDouble( readCacheUsed / GB_SIZE, 2 ) + 'GB' } } } }
    }
  };

  if( serverChartCache != null )
    serverChartCache.destroy();

  serverChartCache = new ApexCharts(document.querySelector("#serverChartCache"), serverCacheOptions);
  serverChartCache.render();
}

function displayMetrics(){
  if ( $.fn.dataTable.isDataTable( '#serverMetrics' ) )
    try{ $('#serverMetrics').DataTable().destroy(); $('#serverMetrics').empty(); } catch(e){};

  var tableRecords = [];

  for( let name in serverData.metrics.meters ){
    let meter = serverData.metrics.meters[name];

    let record = [];
    record.push( escapeHtml( name ) );
    record.push( "" );
    record.push( meter.count );
    record.push( globalFormatDouble( meter.reqPerSecLastMinute ) );
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
