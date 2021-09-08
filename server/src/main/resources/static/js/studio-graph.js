var globalBgColors = ['aqua', 'orange', 'green', 'purple', 'red', 'lime', 'teal', 'maroon', 'navy', 'olive', 'silver', 'blue', 'yellow', 'fuchsia', 'gray', 'white'];
var globalFgColors = ['black', 'black', 'white', 'white', 'black', 'black', 'black', 'white', 'white', 'black', 'black', 'white', 'black', 'black', 'white', 'black'];
var globalRenderedVerticesRID = {};
var globalColorsIndexByType = {};
var globalLastColorIndex = 0;
var globalGraphSpacing = 20;
var globalGraphLabelPerType = {};
var globalGraphPropertiesPerType = {};
var globalCy = null;
var globalSelected = null;

function renderGraph(){
  let elements = [];
  globalRenderedVerticesRID = {};

  globalLastColorIndex = 0;
  globalColorsIndexByType = {};
  for( i in globalResultset.vertices ){
    let vertex = globalResultset.vertices[i];
    assignVertexColor(vertex.t);
    assignProperties(vertex);
  }

  for( i in globalResultset.edges ){
    let edge = globalResultset.edges[i];
    assignProperties(edge);
  }

  let reachedMax = false;
  for( i in globalResultset.vertices ){
    let vertex = globalResultset.vertices[i];

    let rid = vertex["r"];
    if( rid == null )
      continue;

    elements.push( { data: createVertex(vertex) } );
    globalRenderedVerticesRID[rid] = true;

    if( elements.length > globalGraphMaxResult ){
      reachedMax = true;
      break;
    }
  }

  for( i in globalResultset.edges ) {
    let edge = globalResultset.edges[i];
    if( globalRenderedVerticesRID[edge.i] && globalRenderedVerticesRID[edge.o]  )
      // DISPLAY ONLY EDGES RELATIVE TO VERTICES THAT ARE PART OF THE GRAPH
      elements.push( { data: createEdge( edge ) } );
  }

  globalLayout = {
     name: 'cola',
     animate: true,
     refresh: 2,
     ungrabifyWhileSimulating: true,
     nodeSpacing: function( node ){ return globalGraphSpacing },
     spacingFactor: 1.75
   };

  globalCy = cytoscape({
    container: $('#graph'),
    elements: elements,

    style: [ // the stylesheet for the graph
      {
        selector: 'node',
        selectionType: 'single',
        style: {
          'color': 'data(fgColor)',
          'background-color': 'data(bgColor)',
          'label': 'data(label)',
          'width': 'data(size)',
          'height': 'data(size)',
          'border-color': 'gray',
          'border-width': 1,
          'text-valign': "center",
          'text-halign': "center",
          'text-wrap': 'wrap',
          'text-max-width': 100
        }
      },
      {
        selector: 'node:selected',
        selectionType: 'single',
        style: {
          'color': 'data(fgColor)',
          'background-color': 'data(bgColor)',
          'label': 'data(label)',
          'width': 'data(size)',
          'height': 'data(size)',
          'border-color': 'red',
          'border-width': 5,
          'text-valign': "center",
          'text-halign': "center",
          'text-wrap': 'wrap',
          'text-max-width': 100
        }
      },
      {
        selector: 'edge',
        style: {
          'width': 1,
          'label': 'data(label)',
          'color': 'black',
          'line-color': 'black',
          'target-arrow-color': 'black',
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
          removeGraphElement(ele);
        }
      }, {
        content: '<span class="fa fa-project-diagram fa-2x"></span>',
        select: function(ele){
          loadNodeNeighbors( "both", ele.data('id') );
        },
      }, {
        content: '<span class="fa fa-arrow-circle-right fa-2x"></span>',
        select: function(ele){
          loadNodeNeighbors( "out", ele.data('id') );
        },
      }, {
        content: '<span class="fa fa-arrow-circle-left fa-2x"></span>',
        select: function(ele){
          loadNodeNeighbors( "in", ele.data('id') );
        },
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
          removeGraphElement( ele );
        }
      },
    ]
  });

  globalCy.on('select', 'node', function(event){
    globalSelected = globalCy.elements('node:selected');
    if( globalSelected.length < 1 ) {
      $("#customToolbar").empty();
      $("#graphPropertiesTable").empty();
      $("#graphPropertiesType").html("Select an element to see its properties");
      return;
    }

    if( globalSelected.length == 1 ) {
      let data = globalSelected[0].data();

      let summary = "<label class='form-label'>RID&nbsp</label><label class='form-label'><b>" + data.id + "</b></label>&nbsp;";
      summary += "<label class='form-label'>Type&nbsp</label><label class='form-label'><b>" + data.type+ "</b></label><br>";
      summary += "<br><h5>Properties</h5>";

      $("#graphPropertiesType").html( summary );

      let table = "<thead><tr><th scope='col'>Name</th><th scope='col'>Value</th></tr>";
      table += "<tbody>";
      for( let p in data.properties )
        table += "<tr><td>"+p+"</td><td>" + data.properties[p]+ "</td>";
      table += "</tbody>";

      $("#graphPropertiesTable").html(table);

      let actions = "<br><h5>Actions</h5>";
      actions += "<ul>";
      actions += "<li>Load adjacent <a class='link' href='#' onclick='loadNodeNeighbors(\"out\", \""+data.id+"\")'>outgoing</a>, ";
      actions += "<a class='link' href='#' onclick='loadNodeNeighbors(\"in\", \""+data.id+"\")'>incoming</a> or ";
      actions += "<a class='link' href='#' onclick='loadNodeNeighbors(\"both\", \""+data.id+"\")'>both</a></li>";
      actions += "<li><a class='link' href='#' onclick='removeGraphElement(globalSelected)'>Hide</a> selected elements</li>";
      actions += "</ul>";

      $("#graphActions").html( actions );
    }

    let types = {};

    for( i = 0; i < globalSelected.length; ++i ){
      let type = globalSelected[i].data()["type"];
      types[type] = true;
    }

    if( Object.keys(types).length > 1 ){
      $("#customToolbar").empty();
      return;
    }

    let type = globalSelected[0].data()["type"];
    let customToolbar = "<label for='customToolbarLabel' class='form-label'><b>" + type + "</b> label</label>";

    properties = globalGraphPropertiesPerType[type];

    let sel = globalGraphLabelPerType[type];
    if( sel == null )
      sel = "@type";

    customToolbar += "<select id='customToolbarLabel' class='form-control'>";
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
  });

  if( reachedMax )
    globalNotify( "Warning", "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.", "warning");
}

function createVertex(vertex){
  let label = "@type";
  if( globalGraphLabelPerType[vertex["t"]] != null )
    label = globalGraphLabelPerType[vertex["t"]];
  if( label == "@type" )
    label = vertex["t"];
  else
    label = vertex["p"][label];

  let colorIndex = globalColorsIndexByType[ vertex["t"] ];

  return { id: vertex["r"], label: label, size: (70 + ( 2 * label.length ) ), type: vertex["t"],
           fgColor: globalFgColors[ colorIndex ], bgColor: globalBgColors[ colorIndex ],
           weight: vertex["i"] + vertex["o"],
           properties: vertex["p"] };
}

function createEdge(edge){
  let label = "@type";
  if( globalGraphLabelPerType[edge["edge"]] != null )
    label = globalGraphLabelPerType[vertex["t"]];
  if( label == "@type" )
    label = edge["t"];
  else
    label = edge["p"][label];

  return { id: edge["r"], label: label, type: edge["t"], source: edge["o"], target: edge["i"], properties: edge["p"] };
}

function assignVertexColor(type){
  let color = globalColorsIndexByType[type];
  if( color == null ){
    if( globalLastColorIndex >= globalBgColors.length )
      globalLastColorIndex = 0;
    globalColorsIndexByType[type] = globalLastColorIndex++;
  }
}

function assignProperties(element){
  let type = element["t"];
  let properties = globalGraphPropertiesPerType[type];
  if( properties == null ) {
    properties = {};
    globalGraphPropertiesPerType[type] = properties;
  }

  for( p in element.p )
    properties[ p ] = true;;
}

function removeGraphElement( ele ) {
  if( ele == null )
    return;

  globalCy.remove( ele );

  let elements;
  if( ele instanceof Array )
    elements = ele;
  else {
    elements = [];
    elements.push(ele.data().id );
  }

  for( i in elements ) {
    let rid = elements[i].data().id;

    arrayRemoveAll(globalResultset.vertices, row => row.r == rid );
    arrayRemoveAll(globalResultset.edges, row => row.r == rid || row.i == rid || row.o == rid );
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

function loadNodeNeighbors( direction, rid ){
  let database = escapeHtml( $("#inputDatabase").val() );

  $("#executeSpinner").show();

  let beginTime = new Date();

  jQuery.ajax({
    type: "POST",
    url: "/api/v1/command/" + database,
    data: JSON.stringify(
      {
        language: "sql",
        command: "select "+direction+"E() from " + rid,
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
      if( globalResultset.vertices.length > globalGraphMaxResult ){
        reachedMax = true;
        break;
      }

      let vertex = data.result.vertices[i];

      assignVertexColor(vertex.t);
      assignProperties(vertex);

      globalResultset.vertices.push( vertex );
      globalResultset.records.push( vertex );

      globalCy.add([
        {
          group: 'nodes',
          data: createVertex( vertex )
        }
      ]);

      globalRenderedVerticesRID[vertex.r] = true;
    }

    for( i in data.result.edges ){
      let edge = data.result.edges[i];

      if( !globalRenderedVerticesRID[edge.i] || !globalRenderedVerticesRID[edge.o] )
        continue;

      assignProperties(edge);

      globalResultset.edges.push( edge );
      globalResultset.records.push( edge );
      globalCy.add([
        {
          group: 'edges',
          data: createEdge( edge )
        }
      ]);
    }

    globalCy.endBatch();

    globalCy.makeLayout(globalLayout).run();

    if( reachedMax )
      globalNotify( "Warning", "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.", "warning");

  })
  .fail(function( jqXHR, textStatus, errorThrown ){
    globalNotify( "Error", escapeHtml( jqXHR.responseText ), "danger");
  })
  .always(function(data) {
    $("#executeSpinner").hide();
  });
}

function cutSelection(){
  let selected = globalCy.elements(':selected');
  if( selected.length == 0 )
    return;

  removeGraphElement( selected );
  globalCy.makeLayout(globalLayout).run();
}

function cropSelection(){
  let selected = globalCy.elements().not(globalCy.elements(':selected'));
  if( selected.length == 0 )
    return;

  removeGraphElement( selected );
  globalCy.makeLayout(globalLayout).run();
}

function toggleSidebar(){
  $("#graphPropertiesPanel").toggleClass("collapsed");
  $("#graphMainPanel").toggleClass("col-md-12 col-md-9");
}

function searchInGraph(){
  let text = $("#inputGraphSearch").val().trim();
  if( text == "" )
    return;

  for( let i in globalCy.elements() ){
    let el = globalCy.elements()[i];
    if( !el.data )
      continue;

    let data = el.data();

    if( data.label != null && data.label.indexOf( text ) > -1 )
      el.select();

    if( data.t != null && data.t.indexOf( text ) > -1 )
      el.select();

    for( let prop in data.properties ){
      let value = data.properties[prop];
      if( value != null && value.toString().indexOf( text ) > -1 ){
        el.select();
        break;
      }
    }
  }
}
