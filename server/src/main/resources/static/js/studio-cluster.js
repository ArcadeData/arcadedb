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
            {title: "Commands"},
          ],
          data: tableRecords,
        });
      }
    }

    if( callback )
      callback();

    setTimeout(function() {
      if( studioCurrentTab == "cluster" )
        updateCluster();
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

function shutdownServer(serverName){
  let command = serverName != null ? "shutdown " + serverName : "shutdown";
  let message = serverName != null ? "Are you sure to shut down the server '"+serverName+"'?" : "Are you sure to shut down the current server?";
  globalConfirm( "Shutdown Server", message, "warning", function(){ executeServerCommand(command, "Server shutdown request sent successfully");} );
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
