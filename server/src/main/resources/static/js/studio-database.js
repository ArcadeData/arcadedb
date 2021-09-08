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

  updateDatabases();
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

function updateDatabases(){
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

    initQuery();
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
  if( reset )
    globalResultset = null;
  else
    if( globalResultset != null )
      return;

  if( escapeHtml( $("#inputDatabase").val() ) == "" )
    return;
  if( escapeHtml( $("#inputLanguage").val() ) == "" )
    return;
  if( escapeHtml( editor.getValue() ) == "" )
    return;

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
        serializer: "default"
      }
    ),
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    $("#resultJson").val( JSON.stringify(data, null, 2) );

    $( "#result-num" ).html( data.result.length );
    let elapsed = new Date() - beginTime;
    $("#result-elapsed").html( elapsed );

    globalResultset = { records: data.result };
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
    $("#resultJson").val( JSON.stringify(data, null, 2) );

    $( "#result-num" ).html( data.result.vertices.length + data.result.edges.length );
    let elapsed = new Date() - beginTime;
    $("#result-elapsed").html( elapsed );

    if( data.result.vertices.length == 0 && data.result.records.length > 0 ){
      globalResultset = data.result;
      globalActivateTab("tab-table");
      renderTable();
    }

    globalResultset = data.result;
    renderGraph();

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
