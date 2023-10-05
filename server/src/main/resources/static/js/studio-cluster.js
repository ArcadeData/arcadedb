var clusterRefreshTimer = null;

function updateCluster( callback ){
  jQuery.ajax({
    type: "GET",
    url: "/api/v1/server?mode=cluster",
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    if( data.ha != null ){
      $("#serverInfo").html("Server <b>" + data.ha.network.current.name + "</b> works as <b>" + data.ha.network.current.role + "</b> in cluster <b>" + data.ha.clusterName +
      "</b> joined on <b>" + data.ha.network.current.joinedOn + "</b> (election=<b>" + data.ha.electionStatus + "</b>)" );

      if ( $.fn.dataTable.isDataTable( '#serverOnlineReplicaTable' ) )
        try{ $('#serverOnlineReplicaTable').DataTable().destroy(); $('#serverOnlineReplicaTable').empty(); } catch(e){};

      var tableRecords = [];

      if( data.ha.network.replicas.length > 0 ) {
        $("#clusterConnectButton").hide();
        $("#clusterDisconnectButton").show();

        for( let i in data.ha.network.replicas ){
          let row = data.ha.network.replicas[i];

          let record = [];
          record.push( escapeHtml( row.name ) );
          record.push( escapeHtml( row.address ) );
          record.push( escapeHtml( row.status ) );
          record.push( escapeHtml( row.joinedOn ) );
          record.push( escapeHtml( row.leftOn ) );
          record.push( escapeHtml( row.throughput ) );
          record.push( escapeHtml( row.latency ) );
          record.push( "<button class='btn' onclick='shutdownServer(\""+ row.name + "\")'><i class='fas fa-power-off' style='color: red;'></i></button>" );

          tableRecords.push( record );
        }

        $("#serverOnlineReplicaTable").DataTable({
          searching: false,
          paging: false,
          ordering: false,
          columns: [
            {title: "Server Name"},
            {title: "Server Address"},
            {title: "Status"},
            {title: "Joined On"},
            {title: "Left On"},
            {title: "Throughput"},
            {title: "Latency"},
            {title: "Commands", width: "7%"},
          ],
          data: tableRecords,
        });
      } else {
        $("#clusterConnectButton").show();
        $("#clusterDisconnectButton").hide();
      }

      if ( $.fn.dataTable.isDataTable( '#replicatedDatabasesTable' ) )
        try{ $('#replicatedDatabasesTable').DataTable().destroy(); $('#replicatedDatabasesTable').empty(); } catch(e){};

      tableRecords = [];

      if( data.ha.databases.length > 0 ) {
        for( let i in data.ha.databases ){
          let row = data.ha.databases[i];

          let record = [];
          record.push( escapeHtml( row.name ) );
          record.push( escapeHtml( row.quorum ) );
          record.push( "<button class='btn' onclick='alignDatabase(\""+ row.name + "\")'><i class='fas fa-sync' style='color: green;'></i></button>" );

          tableRecords.push( record );
        }

        $("#replicatedDatabasesTable").DataTable({
          searching: false,
          paging: false,
          ordering: false,
          columns: [
            {title: "Database Name"},
            {title: "Quorum"},
            {title: "Commands", width: "7%"},
          ],
          data: tableRecords,
        });
      }
    }

    if( callback )
      callback();

    startClusterRefreshTimer();

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

function shutdownServer(serverName){
  let command = serverName != null ? "shutdown " + serverName : "shutdown";
  let message = serverName != null ? "Are you sure to shut down the server '"+serverName+"'?" : "Are you sure to shut down the current server?";
  globalConfirm( "Shutdown Server", message, "warning", function(){ executeServerCommand(command, "Server shutdown request sent successfully");} );
}

function disconnectFromCluster(){
  globalConfirm( "Shutdown Server", "Are you sure to disconnect current server from the cluster?", "warning", function(){
    executeServerCommand("disconnect cluster", "Disconnection from the cluster request sent successfully");
  });
}

function alignDatabase(dbName){
  let message = "Are you sure to realign the database '"+dbName+"' from the leader to all the replicas?";
  globalConfirm( "Align Database", message, "warning", function( result ){
    executeServerCommand("align database " + dbName, "Align Database executed");
  });
}

function connectToCluster(){
  let lastClusterServerAddress = globalStorageLoad("lastClusterServerAddress", "" );

  let html = "<label for='clusterServerAddress'>Enter the server name/ip-address and the optional port with the format &lt;ip&gt;[:&lt;port&gt;].<br>The default port for replication is 2424.&nbsp;&nbsp;</label><input onkeydown='if (event.which === 13) Swal.clickConfirm()' id='clusterServerAddress' value='"+lastClusterServerAddress+"'>";

  Swal.fire({
    title: 'Connect to a cluster',
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
      let serverAddress = encodeURI( $("#clusterServerAddress").val().trim() );
      if( serverAddress == "" ){
        globalNotify( "Error", "Server address is empty", "danger");
        return;
      }

      globalStorageSave("lastClusterServerAddress", serverAddress );

      jQuery.ajax({
        type: "POST",
        url: "/api/v1/server",
        data: "{ 'command': 'connect cluster " + serverAddress + "' }",
        beforeSend: function (xhr){
          xhr.setRequestHeader('Authorization', globalCredentials);
        }
      })
      .done(function(data){
        globalNotify( "Connection to the cluster", "The command was correctly sent to the server", "success");
        updateCluster();
      })
      .fail(function( jqXHR, textStatus, errorThrown ){
        globalNotifyError( jqXHR.responseText );
      });
    }
  });

  $("#clusterServerAddress").focus();
}

function executeServerCommand(command, successMessage) {
  if( command == null || command == "" )
    return;

  jQuery.ajax({
    type: "POST",
    url: "/api/v1/server",
    data: JSON.stringify({
      command: command,
    }),
    beforeSend: function (xhr){
      xhr.setRequestHeader('Authorization', globalCredentials);
    }
  })
  .done(function(data){
    globalNotify( successMessage, data.result, "success");
  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotify( "Error", jqXHR.responseJSON.detail, "danger");
  });
}

function startClusterRefreshTimer(userChange){
  if( clusterRefreshTimer != null )
    clearTimeout(clusterRefreshTimer);

  const clusterRefreshTimeoutInSecs = $("#clusterRefreshTimeout").val();
  if( clusterRefreshTimeoutInSecs > 0 ) {
    clusterRefreshTimer = setTimeout( function(){
      if( studioCurrentTab == "cluster" )
        updateCluster();
    }, clusterRefreshTimeoutInSecs * 1000 );
  }

  if( userChange )
    globalSetCookie("clusterRefreshTimeoutInSecs", clusterRefreshTimeoutInSecs, 365);
}

document.addEventListener("DOMContentLoaded", function(event) {
  let clusterRefreshTimeoutInSecs = globalGetCookie("clusterRefreshTimeoutInSecs");
  if( clusterRefreshTimeoutInSecs == null )
    serverRefreshTimeoutInSecs = 0;
  $("#clusterRefreshTimeout").val(clusterRefreshTimeoutInSecs);
});
