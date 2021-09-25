var editor = null;
var globalResultset = null;
var globalGraphMaxResult = 1000;
var globalCredentials = null;

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

  updateDatabases( function(){
    initQuery();
  });
}

function showLoginPopup(){
  $("#loginPopup").modal("show");
}

function editorFocus(){
  let value = editor.getValue();
  editor.setValue("");
  editor.setValue(value);

  editor.setCursor(editor.lineCount(), 0);
  editor.focus();
}

function updateDatabases( callback ){
  jQuery.ajax({
    type: "GET",
    url: "/api/v1/databases",
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    let databases = "";
    for( i in data.result ){
      let dbName = data.result[i];
      databases += "<option value='"+dbName+"'>"+dbName+"</option>";
    }
    $("#inputDatabase").html(databases);

    $("#currentDatabase").html( $("#inputDatabase").val() );

    $("#user").html(data.user);

    let version = data.version;
    let pos = data.version.indexOf("(build");
    if( pos > -1 ) {
      version = version.substring( 0, pos ) + " <span style='font-size: 70%'>" + version.substring( pos ) + "</span>";
    }
    $("#version").html(version);

    $("#loginPopup").modal("hide");
    $("#welcomePanel").hide();
    $("#studioPanel").show();

    displaySchema();

    if( callback )
      callback();
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
        url: "/api/v1/command/" + database,
        data: JSON.stringify(
          {
            language: "sql",
            command: "import database " + url,
            serializer: "record"
          }
        ),
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


function backupDatabase(){
  let database = escapeHtml( $("#inputDatabase").val().trim() );
  if( database == "" ){
    globalNotify( "Error", "Database not selected", "danger");
    return;
  }

  globalConfirm("Backup database", "Are you sure you want to backup the database '"+database+"'?<br>The database archive will be created under the 'backup' directory of the server.", "info", function(){
    jQuery.ajax({
      type: "POST",
      url: "/api/v1/command/" + database,
      data: JSON.stringify(
        {
          language: "sql",
          command: "backup database",
          serializer: "record"
        }
      ),
      beforeSend: function (xhr){
        xhr.setRequestHeader('Authorization', globalCredentials);
      }
    })
    .done(function(data){
      globalNotify( "Backup completed", "File: " + escapeHtml( data.result[0].backupFile ), "success");
    })
    .fail(function( jqXHR, textStatus, errorThrown ){
      globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
    });
  });
}

function executeCommand(language, query){
  globalResultset = null;

  if( language != null )
    $("#inputLanguage").val( language );
  if( query != null )
    editor.setValue( query );

  if( escapeHtml( $("#inputDatabase").val() ) == "" )
    return;
  if( escapeHtml( $("#inputLanguage").val() ) == "" )
    return;
  if( escapeHtml( editor.getValue() ) == "" )
    return;

  globalActivateTab("tab-query");

  let activeTab = $("#tabs-command .active").attr("id");
  if( activeTab == "tab-table-sel" )
    executeCommandTable();
  else if( activeTab == "tab-graph-sel" )
    executeCommandGraph();
}

function executeCommandTable(){
  let database = escapeHtml( $("#inputDatabase").val() );
  let language = escapeHtml( $("#inputLanguage").val() );
  let command = escapeHtml( editor.getValue() );

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
    let elapsed = new Date() - beginTime;
    $("#result-elapsed").html( elapsed );

    $("#result-num").html( data.result.records.length );
    $("#resultJson").val( JSON.stringify(data, null, 2) );

    globalResultset = data.result;
    globalCy = null;
    renderTable();
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
  let command = escapeHtml( editor.getValue() );

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
    let elapsed = new Date() - beginTime;
    $("#result-elapsed").html( elapsed );

    $("#result-num").html( data.result.vertices.length + data.result.edges.length );
    $("#resultJson").val( JSON.stringify(data, null, 2) );

    globalResultset = data.result;
    globalCy = null;

    let activeTab = $("#tabs-command .active").attr("id");

    if( data.result.vertices.length == 0 && data.result.records.length > 0 ){
      if( activeTab == "tab-table-sel" )
        renderTable();
      else
        globalActivateTab("tab-table");
    } else {
      if( activeTab == "tab-graph-sel" )
        renderGraph();
      else
        globalActivateTab("tab-graph");
    }

    // FORCE RESET OF THE SEARCH FIELD
    $("#inputGraphSearch").val("");
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}

function displaySchema(){
  let database = escapeHtml( $("#inputDatabase").val() );
  if( database == null || database == "" )
    return;

  jQuery.ajax({
    type: "POST",
    url: "/api/v1/query/" + database,
    data: JSON.stringify(
      {
        language: "sql",
        command: "select from schema:types"
      }
    ),
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){

    let tabVHtml = "";
    let tabEHtml = "";
    let tabDHtml = "";

    let panelVHtml = "";
    let panelEHtml = "";
    let panelDHtml = "";

    for( i in data.result ){
      let row = data.result[i];

      let tabHtml = "<li class='nav-item'><a data-toggle='tab' href='#tab-" + row.name + "' class='nav-link vertical-tab" + (i == 0 ? " active show" : "");
      tabHtml += "' id='tab-" + row.name + "-sel'>" + row.name + " (" + row.properties.length + ")</a></li>";

      let panelHtml = "<div class='tab-pane fade"+(i == 0 ? " active show" : "") +"' id='tab-"+row.name+"' role='tabpanel'>";

      panelHtml += "<br>Type: <b>" + row.name + "</b>";
      if( row.parentTypes != "" )
       ", Super Types: <b>" + row.parentTypes + "</b>";
      if( row.indexes != "" ){
        panelHtml += ", Indexes: <b>";
        panelHtml += row.indexes.map(i => i.name);
        panelHtml += "</b>";
      }

      panelHtml += "<br><br><h6>Properties</h6><table class='table table-striped table-sm' style='border: 0px; width: 100%'>";
      panelHtml += "<thead><tr><th scope='col'>Name</th><th scope='col'>Type</th><th scope='col'>Indexed</th>";
      panelHtml += "<tbody>";

      for( k in row.properties ) {
        let property = row.properties[k];
        panelHtml += "<tr><td>"+property.name+"</td><td>" + property.type + "</td>";

        let propIndexes = [];
        if( row.indexes != null && row.indexes.length > 0 ) {
          propIndexes.push( row.indexes.filter(i => i.properties.includes( property.name )).map(i => (i.unique ? "" : "Not ") + "Unique, Type(" + i.type + ")" + ( i.properties.length > 1 ? ", on multi properties " + i.properties : "" )) );
        }
        panelHtml += "<td>" + ( propIndexes.length > 0 ? propIndexes : "" ) + "</td>";
      }

      panelHtml += "</tbody></table>";

      panelHtml += "<br><h6>Actions</h6>";
      panelHtml += "<ul>";
      panelHtml += "<li><a class='link' href='#' onclick='executeCommand(\"sql\", \"select from "+row.name+" limit 30\")'>Display the first 30 records of "+row.name+"</a>";
      if( row.type == "vertex" )
        panelHtml += "<li><a class='link' href='#' onclick='executeCommand(\"sql\", \"select $this, bothE() from "+row.name+" limit 30\")'>Display the first 30 records of "+row.name+" together with all the vertices that are directly connected</a>";
      panelHtml += "</ul>";

      panelHtml += "</div>";

      if( row.type == "vertex" ) {
        tabVHtml += tabHtml;
        panelVHtml += panelHtml;
      } else if( row.type == "edge" ) {
        tabEHtml += tabHtml;
        panelEHtml += panelHtml;
      } else {
        tabDHtml += tabHtml;
        panelDHtml += panelHtml;
      }
    }

    $("#vTypesTabs").html(tabVHtml);
    $("#eTypesTabs").html(tabEHtml);
    $("#dTypesTabs").html(tabDHtml);

    $("#vTypesPanels").html(panelVHtml != "" ? panelVHtml : "Not defined." );
    $("#eTypesPanels").html(panelEHtml != "" ? panelEHtml : "Not defined." );
    $("#dTypesPanels").html(panelDHtml != "" ? panelDHtml : "Not defined." );
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}
