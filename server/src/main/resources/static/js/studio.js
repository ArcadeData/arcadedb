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

function executeCommand(){
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
        command: command
      }
    ),
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    if ( $.fn.dataTable.isDataTable( '#result' ) )
      $('#result').DataTable().destroy();

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
    //dom: "Bfrtilp",

    var cy = cytoscape({
      container: $('#graph'),
      elements: [ // list of graph elements to start with
        { // node a
          data: { id: 'a' }
        },
        { // node b
          data: { id: 'b' }
        },
        { // edge ab
          data: { id: 'ab', source: 'a', target: 'b' }
        }
      ],

      style: [ // the stylesheet for the graph
        {
          selector: 'node',
          style: {
            'background-color': '#666',
            'label': 'data(id)'
          }
        },

        {
          selector: 'edge',
          style: {
            'width': 3,
            'line-color': '#ccc',
            'target-arrow-color': '#ccc',
            'target-arrow-shape': 'triangle',
            'curve-style': 'bezier'
          }
        }
      ],

      layout: {
        name: 'grid',
        rows: 1
      }
    });

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
