function updateServers( callback ){
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
    $("#serverVersion").html(version);

    if( data.ha != null ){
      $("#serverInfo").html("Server <b>" + data.ha.network.leader.serverName + "</b> in cluster <b>" + data.ha.clusterName +
      "</b> joined on <b>" + data.ha.network.leader.joinedOn + "</b> (election=<b>" + data.ha.electionStatus + "</b>)" );

      if ( $.fn.dataTable.isDataTable( '#serverOnlineReplicaTable' ) )
        try{ $('#serverOnlineReplicaTable').DataTable().destroy(); $('#serverOnlineReplicaTable').empty(); } catch(e){};

      var tableRecords = [];

      if( data.ha.network.replicas.length > 0 ) {
        for( let i in data.ha.network.replicas ){
          let row = data.ha.network.replicas[i];

          let record = [];
          record.push( escapeHtml( row.serverName ) );
          record.push( escapeHtml( row.serverAddress ) );
          record.push( escapeHtml( row.status ) );
          record.push( escapeHtml( row.joinedOn ) );
          record.push( escapeHtml( row.leftOn ) );
          record.push( escapeHtml( row.throughput ) );
          record.push( escapeHtml( row.latency ) );
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
            {title: "Latency"}
          ],
          data: tableRecords,
        });
      }
    }

    $("#serverCfg").html(JSON.stringify( data ) );

    if( callback )
      callback();
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
