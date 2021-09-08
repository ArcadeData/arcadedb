var globalBgColors = ['aqua', 'orange', 'green', 'purple', 'red', 'lime', 'teal', 'maroon', 'navy', 'olive', 'silver', 'blue', 'yellow', 'fuchsia', 'gray', 'white'];
var globalFgColors = ['black', 'black', 'white', 'white', 'black', 'black', 'black', 'white', 'white', 'black', 'black', 'white', 'black', 'black', 'white', 'black'];
var globalColorsIndexByType = {};
var globalLastColorIndex = 0;
var globalGraphSpacing = 20;
var globalGraphLabelPerType = {};
var globalGraphPropertiesPerType = {};
var globalCy = null;

function renderGraph(){
  let elements = [];

  globalLastColorIndex = 0;
  globalColorsIndexByType = {};
  for( i in globalGraphResult.vertices ){
    let vertex = globalGraphResult.vertices[i];
    assignVertexColor(vertex.t);
    assignProperties(vertex);
  }

  for( i in globalGraphResult.edges ){
    let edge = globalGraphResult.edges[i];
    assignProperties(edge);
  }

  let reachedMax = false;
  for( i in globalGraphResult.vertices ){
    let vertex = globalGraphResult.vertices[i];

    let rid = vertex["r"];
    if( rid == null )
      continue;

    elements.push( { data: createVertex(vertex) } );
    if( elements.length > globalGraphMaxResult ){
      reachedMax = true;
      break;
    }
  }

  if( !reachedMax ) {
    for( i in globalGraphResult.edges ){
      elements.push( { data: createEdge( globalGraphResult.edges[i] ) } );
      if( elements.length > globalGraphMaxResult ){
        reachedMax = true;
        break;
      }
    }
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
          globalCy.remove( ele );
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
          globalCy.remove( ele );
        }
      },
    ]
  });

  globalCy.on('select', 'node', function(event){
    let selected = globalCy.elements('node:selected');
    if( selected.length < 1 ) {
      $("#customToolbar").empty();
      $("#graphPropertiesTable").empty();
      $("#graphPropertiesType").html("Select an element to see its properties");
      return;
    }

    if( selected.length == 1 ) {
      let data = selected[0].data();

      let summary = "<label class='form-label'>RID&nbsp</label><label class='form-label'><b>" + data.id + "</b></label>&nbsp;";
      summary += "<label class='form-label'>Type&nbsp</label><label class='form-label'><b>" + data.type+ "</b></label><br>";
      summary += "<hr><h5>Properties</h5>";

      $("#graphPropertiesType").html( summary );

      let table = "<thead><tr><th scope='col'>Name</th><th scope='col'>Value</th></tr>";
      table += "<tbody>";
      for( let p in data.properties )
        table += "<tr><td>"+p+"</td><td>" + data.properties[p]+ "</td>";
      table += "</tbody>";

      $("#graphPropertiesTable").html(table);
    }

    let types = {};

    for( i = 0; i < selected.length; ++i ){
      let type = selected[i].data()["type"];
      types[type] = true;
    }

    if( Object.keys(types).length > 1 ){
      $("#customToolbar").empty();
      return;
    }

    let type = selected[0].data()["type"];
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

  if( reachedMax ){
    globalNotify( "Warning", "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.", "warning");
  }
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
      let vertex = data.result.vertices[i];

      assignVertexColor(vertex.t);
      assignProperties(vertex);

      globalGraphResult.vertices.push( vertex );
      globalCy.add([
        {
          group: 'nodes',
          data: createVertex( vertex )
        }
      ]);
    }

    for( i in data.result.edges ){
      let edge = data.result.edges[i];

      assignProperties(edge);

      globalGraphResult.edges.push( edge );
      globalCy.add([
        {
          group: 'edges',
          data: createEdge( edge )
        }
      ]);
    }
    globalCy.endBatch();

    globalCy.makeLayout(globalLayout).run();

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

  globalCy.remove( selected );
  globalCy.makeLayout(globalLayout).run();
}

function cropSelection(){
  let selected = globalCy.elements().not(globalCy.elements(':selected'));
  if( selected.length == 0 )
    return;

  globalCy.remove( selected );
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
