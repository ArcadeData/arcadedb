var editor = null;
var globalResultset = null;
var globalGraphMaxResult = 1000;
var globalCredentials = null;

/* API Gateway route */
// TODO pull window relative path, or env var.
var basePath = "/api/v1/arcadedb/app";

function make_base_auth(user, password) {
  var tok = user + ':' + password;
  var hash = btoa(tok);
  return "Basic " + hash;
}

function login(){
  var userName = $("#inputUserName").val().trim();
  if( userName.length == 0 )
    return;

  var userPassword = $("#inputUserPassword").val().trim();
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
  let selected = getCurrentDatabase();
  if( selected == null || selected == "" )
    selected = globalStorageLoad("database.current");

  jQuery.ajax({
    type: "POST",
    url: basePath + "/api/v1/server",
    data: "{ command: 'list databases' }",
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    let databases = "";
    for( let i in data.result ){
      let dbName = data.result[i];
      databases += "<option value='"+dbName+"'>"+dbName+"</option>";
    }
    $("#inputDatabase").html(databases);

    if( selected != null && selected != "" )
      $("#inputDatabase").val(selected);

    $("#currentDatabase").html( getCurrentDatabase() );

    $("#user").html(data.user);

    $("#loginPopup").modal("hide");
    $("#welcomePanel").hide();
    $("#studioPanel").show();

    displaySchema();
    displayDatabaseSettings();

    if( callback )
      callback();
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotifyError( jqXHR.responseText );
  })
  .always(function(data) {
    $("#loginSpinner").hide();
  });
}

function createDatabase(){
  let html = "<label for='inputCreateDatabaseName'>Enter the database name:&nbsp;&nbsp;</label><input onkeydown='if (event.which === 13) Swal.clickConfirm()' id='inputCreateDatabaseName'>";

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
      let database = encodeURI( $("#inputCreateDatabaseName").val().trim() );
      if( database == "" ){
        globalNotify( "Error", "Database name empty", "danger");
        return;
      }

      jQuery.ajax({
        type: "POST",
        url: basePath + "/api/v1/server",
        data: "{ 'command': 'create database " + database + "' }",
        beforeSend: function (xhr){
          xhr.setRequestHeader('Authorization', globalCredentials);
        }
      })
      .done(function(data){
        $("#inputDatabase").val(database);
        updateDatabases();
      })
      .fail(function( jqXHR, textStatus, errorThrown ){
        globalNotifyError( jqXHR.responseText );
      });
    }
  });

  $("#inputCreateDatabaseName").focus();
}

function dropDatabase(){
  let database = escapeHtml( getCurrentDatabase() );
  if( database == "" ){
    globalNotify( "Error", "Database not selected", "danger");
    return;
  }

  globalConfirm("Drop database", "Are you sure you want to drop the database '"+database+"'?<br>WARNING: The operation cannot be undone.", "warning", function(){
    jQuery.ajax({
      type: "POST",
      url: basePath + "/api/v1/server",
      data: "{ 'command': 'drop database " + database + "' }",
      beforeSend: function (xhr){
        xhr.setRequestHeader('Authorization', globalCredentials);
      }
    })
    .done(function(data){
      updateDatabases();
    })
    .fail(function( jqXHR, textStatus, errorThrown ){
      globalNotifyError( jqXHR.responseText );
    });
  });
}

function backupDatabase(){
  let database = getCurrentDatabase();
  if( database == "" ){
    globalNotify( "Error", "Database not selected", "danger");
    return;
  }

  globalConfirm("Backup database", "Are you sure you want to backup the database '"+database+"'?<br>The database archive will be created under the 'backup' directory of the server.", "info", function(){
    jQuery.ajax({
      type: "POST",
      url: basePath + "/api/v1/command/" + database,
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
      globalNotifyError( jqXHR.responseText );
    });
  });
}

function dropProperty(type, property){
  let database = getCurrentDatabase();
  if( database == "" ){
    globalNotify( "Error", "Database not selected", "danger");
    return;
  }

  globalConfirm("Drop property", "Are you sure you want to drop the property '"+property+"' on type '"+type+"'?<br>WARNING: The operation cannot be undone.", "warning", function(){
    jQuery.ajax({
      type: "POST",
      url: basePath + "/api/v1/command/" + database,
      data: JSON.stringify(
        {
          language: "sql",
          command: "drop property `" + type + "`.`" + property + "`",
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
      globalNotifyError( jqXHR.responseText );
    });
  });
}


function dropIndex(indexName){
  let database = getCurrentDatabase();
  if( database == "" ){
    globalNotify( "Error", "Database not selected", "danger");
    return;
  }

  globalConfirm("Drop index", "Are you sure you want to drop the index '"+indexName+"'?<br>WARNING: The operation cannot be undone.", "warning", function(){
    jQuery.ajax({
      type: "POST",
      url: basePath + "/api/v1/command/" + database,
      data: JSON.stringify(
        {
          language: "sql",
          command: "drop index `" + indexName + "`",
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
      globalNotifyError( jqXHR.responseText );
    });
  });
}

function getCurrentDatabase(){
  let db = $("#inputDatabase").val();
  return db != null ? db.trim() : null;
}

function setCurrentDatabase( dbName ){
  $("#currentDatabase").html( dbName );
  $("#inputDatabase").val( dbName );
  globalStorageSave("database.current", dbName);
}

function getQueryHistory(){
  let queryHistory = globalStorageLoad("database.query.history");
  if( queryHistory == null )
    queryHistory = [];
  else {
    try{
      queryHistory = JSON.parse(queryHistory);
    } catch(e) {
      // RESET HISTORY
      globalStorageSave("database.query.history", "[]");
      queryHistory = [];
    }
  }

  return queryHistory;
}

function loadQueryHistory(){
  $("#inputHistory").html("");
  $("#inputHistory").append( "<option value=''></option>" );

  let queryHistory = getQueryHistory();
  if( queryHistory != null && queryHistory.length > 0 ){
    let database = escapeHtml( getCurrentDatabase() );
    for( let index = 0; index < queryHistory.length; ++index ) {
      let q = queryHistory[index];
      if( q != null && q.d == database && q.l != null && q.c != null )
        $("#inputHistory").append( "<option value='" + index + "'>("+q.l+") "+q.c+"</option>" );
    }
  }
}

function copyQueryFromHistory(){
  let index = $("#inputHistory").val();
  if( index != "" ){
    let queryHistory = getQueryHistory();
    let q = queryHistory[index];
    if( q != null ) {
      setCurrentDatabase( q.d );
       $("#inputLanguage").val( q.l );
      editor.setValue( q.c );
    }
  }
}

function executeCommand(language, query){
  globalResultset = null;

  if( language != null )
    $("#inputLanguage").val( language );
  else
    language = $("#inputLanguage").val();

  if( query != null )
    editor.setValue( query );
  else
    query = editor.getValue();

  let database = getCurrentDatabase();

  if( database == "" )
    return;
  if( escapeHtml( $("#inputLanguage").val() ) == "" )
    return;
  if( escapeHtml( editor.getValue() ) == "" )
    return;

  globalActivateTab("tab-query");

  let activeTab = $("#tabs-command .active").attr("id");
  if( activeTab == "tab-graph-sel" )
    executeCommandGraph();
  else
    executeCommandTable();

  let queryHistory = getQueryHistory();

  for( index in queryHistory ){
    let q = queryHistory[index];
    if( q == null ||
        ( q.d == database &&
          q.l == language &&
          q.c == query ) ) {
      // RE-EXECUTED OLD QUERY, REMOVE OLD ENTRY AND INSERT AT THE TOP OF THE LIST
      queryHistory.splice(index,1);
    }
  }

  // REMOVE OLD QUERIES
  while( queryHistory.length > 20 )
    queryHistory.pop();

  queryHistory = [ {"d": database, "l": language, "c": query} ].concat(queryHistory);
  globalStorageSave("database.query.history", JSON.stringify( queryHistory ) );

  loadQueryHistory();
}


function executeCommandTable(){
  let database = getCurrentDatabase();
  let language = escapeHtml( $("#inputLanguage").val() );
  let command = escapeHtml( editor.getValue() );
  let limit = parseInt( $("#inputLimit").val() );
  let profileExecution = $('#profileCommand').prop('checked') ? "detailed" : "basic";

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery.ajax({
    type: "POST",
    url: basePath + "/api/v1/command/" + database,
    data: JSON.stringify(
      {
        language: language,
        command: command,
        limit: limit,
        profileExecution: profileExecution,
        serializer: "studio"
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
    $("#resultExplain").val( data.explain != null ? data.explain : "'" );

    globalResultset = data.result;
    globalCy = null;
    renderTable();
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotifyError( jqXHR.responseText );
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}

function executeCommandGraph(){
  let database = getCurrentDatabase();
  let language = escapeHtml( $("#inputLanguage").val() );
  let command = escapeHtml( editor.getValue() );
  let limit = parseInt( $("#inputLimit").val() );
  let profileExecution = $('#profileCommand').prop('checked') ? "detailed" : "basic";

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery.ajax({
    type: "POST",
    url: basePath + "/api/v1/command/" + database,
    data: JSON.stringify(
      {
        language: language,
        command: command,
        limit: limit,
        profileExecution: profileExecution,
        serializer: "studio"
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
    $("#resultExplain").val( data.explain != null ? data.explain : "'" );

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
    globalNotifyError( jqXHR.responseText );
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}

function displaySchema(){
  let database = getCurrentDatabase();
  if( database == null || database == "" )
    return;

  jQuery.ajax({
    type: "POST",
    url: basePath + "/api/v1/query/" + database,
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

    for( let i in data.result ){
      let row = data.result[i];

      let tabHtml = "<li class='nav-item'><a data-toggle='tab' href='#tab-" + row.name + "' class='nav-link vertical-tab" + (i == 0 ? " active show" : "");
      tabHtml += "' id='tab-" + row.name + "-sel'>" + row.name + "</a></li>";

      let panelHtml = "<div class='tab-pane fade"+(i == 0 ? " active show" : "") +"' id='tab-"+row.name+"' role='tabpanel'>";

      panelHtml += "<br>Type: <b>" + row.name + "</b>";
      if( row.parentTypes != "" )
        panelHtml += ", Super Types: <b>" + row.parentTypes + "</b>";
      if( row.indexes != "" ){
        panelHtml += "<br>Indexes: <b>";
        panelHtml += row.indexes.map(i => " " + i.name );
        panelHtml += "</b>";
      }

      panelHtml += "<br><br><h6>Properties</h6>";
      //panelHtml += "<button class='btn btn-pill' onclick='createProperty()'><i class='fa fa-plus'></i> Create Property</button>";
      panelHtml += "<table class='table table-striped table-sm' style='border: 0px; width: 100%'>";
      panelHtml += "<thead><tr><th scope='col'>Name</th><th scope='col'>Type</th><th scope='col'>Indexed</th><th scope='col'>Actions</th>";
      panelHtml += "<tbody>";

      for( let k in row.properties ) {
        let property = row.properties[k];
        panelHtml += "<tr><td>"+property.name+"</td><td>" + property.type + "</td>";

        let actionHtml = "<button class='btn btn-pill' onclick='dropProperty(\""+row.name+"\", \""+property.name+"\")'><i class='fa fa-minus'></i> Drop Property</button>";

        let propIndexes = [];
        if( row.indexes != null && row.indexes.length > 0 ) {
          propIndexes.push( row.indexes.filter(i => i.properties.includes( property.name )).map(i => (i.name + " " + i.unique ? "" : "Not ") + "Unique, Type(" + i.type + ")" + ( i.properties.length > 1 ? ", on multi properties " + i.properties : "" )) );
          actionHtml += row.indexes.filter(i => i.properties.includes( property.name )).map(i => ( "<button class='btn btn-pill' onclick='dropIndex(\"" + i.name + "\")'><i class='fa fa-minus'></i> Drop Index</button>" ) );
        }
        panelHtml += "<td>" + ( propIndexes.length > 0 ? propIndexes : "" ) + "</td>";
        panelHtml += "<td>" + actionHtml + "</td>";
      }

      panelHtml += "</tbody></table>";

      panelHtml += "<br><h6>Actions</h6>";
      panelHtml += "<ul>";
      panelHtml += "<li><a class='link' href='#' onclick='executeCommand(\"sql\", \"select from `"+row.name+"` limit 30\")'>Display the first 30 records of "+row.name+"</a>";
      if( row.type == "vertex" )
        panelHtml += "<li><a class='link' href='#' onclick='executeCommand(\"sql\", \"select *, bothE() as `@edges` from `"+row.name+"` limit 30\")'>Display the first 30 records of "+row.name+" together with all the vertices that are directly connected</a>";
      panelHtml += "<li><a class='link' href='#' onclick='executeCommand(\"sql\", \"select count(*) from `"+row.name+"`\")'>Count the records of type "+row.name+"</a>";
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
    globalNotifyError( jqXHR.responseText );
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}

function displayDatabaseSettings(){
  let database = getCurrentDatabase();
  if( database == null || database == "" )
    return;

  jQuery.ajax({
    type: "POST",
    url: basePath + "/api/v1/query/" + database,
    data: JSON.stringify(
      {
        language: "sql",
        command: "select expand( settings ) from schema:database"
      }
    ),
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    if ( $.fn.dataTable.isDataTable( '#dbSettings' ) )
      try{ $('#dbSettings').DataTable().destroy(); $('#serverMetrics').empty(); } catch(e){};

    var tableRecords = [];

    for( let i in data.result ){
      let row = data.result[i];

      let record = [];
      record.push( row.key );
      record.push( row.value );
      record.push( row.description );
      record.push( row.default );
      record.push( row.overridden );
      tableRecords.push( record );
    }

   $("#dbSettings").DataTable({
      paging: false,
      ordering: false,
      autoWidth: false,
      columns: [
        {title: "Key", width: "25%"},
        {title: "Value", width: "20%",
         render: function ( data, type, row) {
           return "<a href='#' onclick='updateDatabaseSetting(\""+row[0]+"\", \""+row[1]+"\")' style='color: green;'><b>"+data+"</b></a>";
         }},
        {title: "Description", width: "33%"},
        {title: "Default", width: "15%"},
        {title: "Overridden", width: "7%"},
      ],
      data: tableRecords,
    });
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotifyError( jqXHR.responseText );
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}

function updateDatabaseSetting(key, value){
  let html = "<b>" + key + "</b> = <input id='updateSettingInput' value='"+value+"'>";
  html += "<br><p><i>The setting will be saved in the database configuration.</i></p>";

  Swal.fire({
    title: "Update Database Setting",
    html: html,
    showCancelButton: true,
    width: 600,
    confirmButtonColor: '#3ac47d',
    cancelButtonColor: 'red',
  }).then((result) => {
    if (result.value) {
      jQuery.ajax({
       type: "POST",
       url: basePath + "/api/v1/server",
       data: JSON.stringify(
         {
           language: "sql",
           command: "set database setting " + getCurrentDatabase() + " " + key + " " +$("#updateSettingInput").val()
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
        displayDatabaseSettings();
        return true;
      });
    }
  });
}
