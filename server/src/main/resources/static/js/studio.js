var globalColors = ['aqua', 'orange', 'gray', 'green', 'lime', 'teal', 'maroon', 'navy', 'olive', 'purple', 'red', 'silver', 'blue', 'yellow', 'fuchsia', 'white'];
var globalColorsByType = {};
var globalLastColorIndex = 0;
var globalTableResult = null;
var globalGraphResult = null;
var globalGraphMaxResult = 1000;
var globalGraphEdgeLength = 100;
var globalGraphLabelPerType = {};
var globalGraphPropertiesPerType = {};
var globalCy = null;

function showLoginPopup(){
  $("#loginPopup").modal("show");
}

function updateDatabases(){
  jQuery.ajax({
    type: "GET",
    url: "/api/v1/databases",
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    $("#loginPanel").hide();
    $("#databasePanel").show();

    let databases = "";
    for( i in data.result ){
      let dbName = data.result[i];
      databases += "<option value='"+dbName+"'>"+dbName+"</option>";
    }
    $("#inputDatabase").html(databases);

    $("#user").html(data.user);

    let version = data.version;
    let pos = data.version.indexOf("(build");
    if( pos > -1 ) {
      version = version.substring( 0, pos ) + "<br><span style='font-size: 70%'>" + version.substring( pos ) + "</span>";
    }
    $("#version").html(version);

    $("#loginPopup").modal("hide");
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
  })
  .always(function(data) {
    $("#loginSpinner").hide();
  });
}

function importDatabase(){
  let html = "<label for='inputImportDatabaseName'>Enter the database name:&nbsp;&nbsp;</label><input id='inputImportDatabaseName'>";
  html += "<label for='inputImportDatabaseURL'>Enter the URL of the database:&nbsp;&nbsp;</label><input id='inputImportDatabaseURL'>";

  Swal.fire({
    title: 'Import a database database',
    html: html,
    inputAttributes: {
      autocapitalize: 'off'
    },
    confirmButtonColor: '#3ac47d',
    cancelButtonColor: 'red',
    showCancelButton: true,
    confirmButtonText: 'Send',
  }).then((result) => {
    if (result.value) {
      let database = escapeHtml( $("#inputImportDatabaseName").val().trim() );
      if( database == "" ){
        globalNotify( "Error", "Database name empty", "danger");
        return;
      }

      let url = escapeHtml( $("#inputImportDatabaseURL").val().trim() );
      if( url == "" ){
        globalNotify( "Error", "Database URL empty", "danger");
        return;
      }

      jQuery.ajax({
        type: "POST",
        url: "/api/v1/import",
        data: JSON.stringify( { name: database, url: url, limit: 20000 } ),
        beforeSend: function (xhr){
          xhr.setRequestHeader('Authorization', globalCredentials);
        }
      })
      .done(function(data){
        updateDatabases();
      })
      .fail(function( jqXHR, textStatus, errorThrown ){
        globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
      });
    }
  });
}

function createDatabase(){
  let html = "<label for='inputCreateDatabaseName'>Enter the database name:&nbsp;&nbsp;</label><input id='inputCreateDatabaseName'>";

  Swal.fire({
    title: 'Create a new database',
    html: html,
    inputAttributes: {
      autocapitalize: 'off'
    },
    confirmButtonColor: '#3ac47d',
    cancelButtonColor: 'red',
    showCancelButton: true,
    confirmButtonText: 'Send',
  }).then((result) => {
    if (result.value) {
      let database = escapeHtml( $("#inputCreateDatabaseName").val().trim() );
      if( database == "" ){
        globalNotify( "Error", "Database name empty", "danger");
        return;
      }

      jQuery.ajax({
        type: "POST",
        url: "/api/v1/create/" + database,
        data: "",
        beforeSend: function (xhr){
          xhr.setRequestHeader('Authorization', globalCredentials);
        }
      })
      .done(function(data){
        updateDatabases();
      })
      .fail(function( jqXHR, textStatus, errorThrown ){
        globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
      });
    }
  });
}

function dropDatabase(){
  let database = escapeHtml( $("#inputDatabase").val().trim() );
  if( database == "" ){
    globalNotify( "Error", "Database not selected", "danger");
    return;
  }

  globalConfirm("Drop database", "Are you sure you want to drop the database '"+database+"'?<br>WARNING: The operation cannot be undone.", "warning", function(){
    jQuery.ajax({
      type: "POST",
      url: "/api/v1/drop/" + database,
      data: "",
      beforeSend: function (xhr){
        xhr.setRequestHeader('Authorization', globalCredentials);
      }
    })
    .done(function(data){
      updateDatabases();
    })
    .fail(function( jqXHR, textStatus, errorThrown ){
      globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
    });
  });
}

function executeCommand(reset){
  if( reset ){
    globalTableResult = null;
    globalGraphResult = null;
  }

  let activeTab = $("#tabs-command .active").attr("id");
  if( activeTab == "tab-table-sel" && globalTableResult == null )
    executeCommandTable();
  else if( activeTab == "tab-graph-sel" && globalGraphResult == null )
    executeCommandGraph();
}

function executeCommandTable(){
  let database = escapeHtml( $("#inputDatabase").val() );
  let language = escapeHtml( $("#inputLanguage").val() );
  let command = escapeHtml( $("#inputCommand").val() );

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery.ajax({
    type: "POST",
    url: "/api/v1/command/" + database,
    data: JSON.stringify(
      {
        language: language,
        command: command,
        serializer: "default"
      }
    ),
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    globalTableResult = data.result;

    $("#resultJson").val( JSON.stringify(data, null, 2) );

    if ( $.fn.dataTable.isDataTable( '#result' ) )
      try{ $('#result').DataTable().destroy(); $('#result').empty(); } catch(e){};

    var tableColumns = [];
    var tableRecords = [];

    if( data.result.length > 0 ) {
      let columns = {};
      for( i in data.result ){
        let row = data.result[i];

        for( p in row ){
          if( !columns[p] )
            columns[p] = true;
        }
      }

      orderedColumns = [];
      if( columns["@rid"])
        orderedColumns.push("@rid");
      if( columns["@type"])
        orderedColumns.push("@type");

      for( colName in columns ){
        if( !colName.startsWith("@"))
          orderedColumns.push(colName);
      }

      if( columns["@in"])
        orderedColumns.push("@in");
      if( columns["@out"])
        orderedColumns.push("@out");

      for( i in orderedColumns )
        tableColumns.push( { sTitle: escapeHtml( orderedColumns[i] ), "defaultContent": "" } );

      for( i in data.result ){
        let row = data.result[i];


        let record = [];
        for( i in orderedColumns )
          record.push( escapeHtml( row[orderedColumns[i]] ) );
        tableRecords.push( record );
      }
    }

    $( "#result-num" ).html( data.result.length );

    let elapsed = new Date() - beginTime;
    $("#result-elapsed").html( elapsed );

    if( data.result.length > 0 ) {
      $("#result").DataTable({
        orderCellsTop: true,
        fixedHeader: true,
        paging:   true,
        pageLength: 20,
        bLengthChange: true,
        initComplete: function() {
          $('div.dataTables_filter input').attr('autocomplete', 'off')
        },
        aoColumns: tableColumns,
        aaData: tableRecords,
        deferRender: true,
        dom: '<Blf>rt<ip>',
        lengthMenu: [
          [ 10, 20, 50, 100, -1 ],
          [ '10', '20', '50', '100', 'all' ]
        ],
      });
    }
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}

function executeCommandGraph(){
  let database = escapeHtml( $("#inputDatabase").val() );
  let language = escapeHtml( $("#inputLanguage").val() );
  let command = escapeHtml( $("#inputCommand").val() );

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery.ajax({
    type: "POST",
    url: "/api/v1/command/" + database,
    data: JSON.stringify(
      {
        language: language,
        command: command,
        serializer: "graph"
      }
    ),
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    $("#resultJson").val( JSON.stringify(data, null, 2) );
    globalGraphResult = data.result;
    renderGraph();
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}

function renderGraph(){
  let elements = [];

  globalLastColorIndex = 0;
  globalColorsByType = {};
  for( i in globalGraphResult.vertices ){
    let vertex = globalGraphResult.vertices[i];
    let type = vertex["t"];

    let color = globalColorsByType[type];
    if( color == null ){
      if( globalLastColorIndex >= globalColors.length )
        globalLastColorIndex = 0;
      color = globalColors[ globalLastColorIndex++ ];
      globalColorsByType[type] = color;
    }

    let properties = globalGraphPropertiesPerType[type];
    if( properties == null ) {
      properties = {};
      globalGraphPropertiesPerType[type] = properties;
    }

    for( p in vertex.p ){
      properties[ p ] = true;;
    }
  }

  for( i in globalGraphResult.edges ){
    let edge = globalGraphResult.edges[i];
    let type = edge.t;

    let properties = globalGraphPropertiesPerType[type];
    if( properties == null ) {
      properties = {};
      globalGraphPropertiesPerType[type] = properties;
    }

    for( p in edge.p ){
      properties[ p ] = true;;
    }
  }

  let reachedMax = false;
  for( i in globalGraphResult.vertices ){
    let vertex = globalGraphResult.vertices[i];

    let rid = vertex["r"];
    if( rid == null )
      continue;

    let label = "@type";
    if( globalGraphLabelPerType[vertex["t"]] != null )
      label = globalGraphLabelPerType[vertex["t"]];
    if( label == "@type" )
      label = vertex["t"];
    else
      label = vertex["p"][label];

    elements.push( { data: { id: rid, label: label, type: vertex["t"], color: globalColorsByType[ vertex["t"] ], properties: vertex["p"] } } );
    if( elements.length > globalGraphMaxResult ){
      reachedMax = true;
      break;
    }
  }

  if( !reachedMax ) {
    for( i in globalGraphResult.edges ){
      let edge = globalGraphResult.edges[i];
      let rid = edge["r"];

      let label = "@type";
      if( globalGraphLabelPerType[edge["edge"]] != null )
        label = globalGraphLabelPerType[vertex["t"]];
      if( label == "@type" )
        label = edge["t"];
      else
        label = edge["p"][label];

      elements.push( { data: { id: rid, label: label, type: edge["t"], source: edge["o"], target: edge["i"], properties: edge["p"] } } );

      if( elements.length > globalGraphMaxResult ){
        reachedMax = true;
        break;
      }
    }
  }

  globalLayout = {
     name: 'cola',
     refresh: 2,
     maxSimulationTime: 10000,
     ungrabifyWhileSimulating: true,
     edgeLength: parseInt( $("#globalGraphEdgeLength").val() ),
     nodeSpacing: function( node ){ return 30; }
   };

  globalCy = cytoscape({
    container: $('#graph'),
    elements: elements,

    style: [ // the stylesheet for the graph
      {
        selector: 'node',
        selectionType: 'single',
        style: {
          'background-color': 'data(color)',
          'label': 'data(label)',
          'width': '100px',
          'height': '100px',
          'border-color': 'gray',
          'border-width': 1,
          'text-valign': "center",
          'text-halign': "center"
        }
      },

      {
        selector: 'edge',
        style: {
          'width': 1,
          'label': 'data(label)',
          'line-color': '#ccc',
          'target-arrow-color': '#ccc',
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier',
          'edge-text-rotation': 'autorotate',
          'target-arrow-shape': 'triangle',
          'text-outline-color': "#F7F7F7",
          'text-outline-width': 8,
        }
      }
    ],

    layout: globalLayout,
  });

  globalCy.cxtmenu({
    selector: 'node',
    menuRadius: function(ele){ return 50; },
    adaptativeNodeSpotlightRadius: true,
    openMenuEvents: 'taphold',
    commands: [
      {
        content: '<span class="fa fa-eye-slash fa-2x"></span>',
        select: function(ele){
          globalCy.remove( ele );
        }
      }, {
        content: '<span class="fa fa-project-diagram fa-2x"></span>',
        select: function(ele){
          loadNodeNeighbors( ele.data('id') );
        },
      }, {
        content: 'Text',
        select: function(ele){
          console.log( ele.position() );
        }
      }
    ]
  });

  globalCy.cxtmenu({
    selector: 'edge',
    adaptativeNodeSpotlightRadius: false,
    commands: [
      {
        content: '<span class="fa fa-eye-slash fa-2x"></span>',
        select: function(ele){
          globalCy.remove( ele );
        }
      },
    ]
  });

//  globalCy.cxtmenu({
//    selector: 'core',
//    commands: [
//      {
//        content: 'bg1',
//        select: function(){
//          console.log( 'bg1' );
//        }
//      },
//      {
//        content: 'bg2',
//        select: function(){
//          console.log( 'bg2' );
//        }
//      }
//    ]
//  });

  globalCy.on('select', 'node', function(event){
    let selected = globalCy.$('node:selected');
    if( selected.length != 1 ) {
      $("#customToolbar").empty();
    } else {
      let type = selected[0].data()["type"];
      let customToolbar = "Selected Type: " + type + " label: ";

      properties = globalGraphPropertiesPerType[type];

      let sel = globalGraphLabelPerType[type];
      if( sel == null )
        sel = "@type";

      customToolbar += "<select id='customToolbarLabel'>";
      customToolbar += "<option value='@type'"+(sel == "@type" ? " selected": "" )+">@type</option>" ;
      for( p in properties ){
        customToolbar += "<option value='"+p+"'"+(sel == p ? " selected": "" )+">" + p + "</option>" ;
      }
      customToolbar += "</select>";

      $("#customToolbar").html(customToolbar);

      $("#customToolbarLabel").change( function(){
        globalGraphLabelPerType[type] = $("#customToolbarLabel").val();
        renderGraph();
      });
    }
  });

  if( reachedMax ){
    globalNotify( "Warning", "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.", "warning");
  }
}

function exportGraph(){
  globalCy.graphml({
    node: {
      css: false,
      data: true,
      position: false,
      discludeds: []
    },
    edge: {
      css: false,
      data: true,
      discludeds: []
    },
    layoutBy: "cola"
  });

  let graphml = globalCy.graphml();

  const blob = new Blob([graphml], {type: 'text/xml'});
  if(window.navigator.msSaveOrOpenBlob) {
    window.navigator.msSaveBlob(blob, filename);
  } else {
    const elem = window.document.createElement('a');
    elem.href = window.URL.createObjectURL(blob);
    elem.download = "arcade.graphml";
    document.body.appendChild(elem);
    elem.click();
    document.body.removeChild(elem);
  }
}

function loadNodeNeighbors( rid ){
  let database = escapeHtml( $("#inputDatabase").val() );

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery.ajax({
    type: "POST",
    url: "/api/v1/command/" + database,
    data: JSON.stringify(
      {
        language: "sql",
        command: "select bothE() from " + rid,
        serializer: "graph"
      }
    ),
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    globalCy.startBatch();

    for( i in data.result.vertices ){
      let vertex = data.result.vertices[i];

      globalGraphResult.vertices.push( vertex );
      let type = vertex["t"];

      let color = globalColorsByType[type];
      if( color == null ){
        if( globalLastColorIndex >= globalColors.length )
          globalLastColorIndex = 0;
        color = globalColors[ globalLastColorIndex++ ];
        globalColorsByType[type] = color;
      }

      globalCy.add([
        {
          group: 'nodes',
          data: { id: vertex['r'], label: vertex["t"], properties: vertex['p'] }
        }
      ]);
    }

    for( i in data.result.edges ){
      let edge = data.result.edges[i];
      globalGraphResult.edges.push( edge );
      globalCy.add([
        {
          group: 'edges',
          data: { id: edge['r'], source: edge["o"], target: edge["i"], label: edge["t"], properties: edge['p'] }
        }
      ]);
    }
    globalCy.endBatch();

    let layout = globalCy.makeLayout(globalLayout);
    layout.run();

  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}

function globalAlert(title, text, icon, callback ){
  if( !icon )
    icon = "error";

  let swal = Swal.fire({
    title: title,
    html: text,
    icon: icon,
  }).then((result) => {
    if( callback )
      callback();
  });
}

function globalConfirm(title, text, icon, yes, no ){
  let swal = Swal.fire({
    title: title,
    html: text,
    icon: icon,
    showCancelButton: true,
    confirmButtonColor: '#3ac47d',
    cancelButtonColor: 'red',
  }).then((result) => {
    if (result.value ) {
      if( yes )
        yes();
    } else {
      if( no )
        no();
    }
  });
}

var globalCredentials;

$(function(){
  $('#loginForm').keypress(function(e){
    if(e.which == 13) {
      login();
    }
  })
})

function globalNotify(title, message, type){
  $.notify({
    title: "<strong>"+title+"</strong>",
    message: message,
    z_index: 100000,
    placement: {
      from: "bottom",
      align: "right"
    },
  },{
    type: type
  });
}

function make_base_auth(user, password) {
  var tok = user + ':' + password;
  var hash = btoa(tok);
  return "Basic " + hash;
}

function login(){
  var userName = escapeHtml( $("#inputUserName").val().trim() );
  if( userName.length == 0 )
    return;

  var userPassword = escapeHtml( $("#inputUserPassword").val().trim() );
  if( userPassword.length == 0 )
    return;

  $( "#loginSpinner" ).show();

  globalCredentials = make_base_auth(userName, userPassword);

  updateDatabases();
}

function globalSetCookie(key, value, expiry) {
  var expires = new Date();
  expires.setTime(expires.getTime() + (expiry * 24 * 60 * 60 * 1000));
  document.cookie = key + '=' + value + ';expires=' + expires.toUTCString()+';path=/';
}

function globalGetCookie(key) {
  var keyValue = document.cookie.match('(^|;) ?' + key + '=([^;]*)(;|$)');
  return keyValue ? keyValue[2] : null;
}

function globalEraseCookie(key) {
  var keyValue = globalGetCookie(key);
  globalSetCookie(key, keyValue, '-1');
  return keyValue;
}

function escapeHtml(unsafe) {
  if( unsafe == null )
    return null;

  if( typeof unsafe === 'object' )
    unsafe = JSON.stringify(unsafe);
  else
    unsafe = unsafe.toString();

  return unsafe
       .replace(/&/g, "&amp;")
       .replace(/</g, "&lt;")
       .replace(/>/g, "&gt;")
       .replace(/"/g, "&quot;")
       .replace(/'/g, "&#039;");
}
