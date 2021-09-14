var globalBgColors = ['aqua', 'orange', 'green', 'purple', 'red', 'lime', 'teal', 'maroon', 'navy', 'olive', 'silver', 'blue', 'yellow', 'fuchsia', 'gray', 'white'];
var globalFgColors = ['black', 'black', 'white', 'white', 'black', 'black', 'black', 'white', 'white', 'black', 'black', 'white', 'black', 'black', 'white', 'black'];
var globalRenderedVerticesRID = {};
var globalTotalEdges = 0;
var globalLastColorIndex = 0;
var globalGraphPropertiesPerType = {};
var globalCy = null;
var globalSelected = null;
var globalGraphSettings = {
  graphSpacing: 50,
  types: {},
};

function renderGraph(){
  if( globalResultset == null )
    return;

  $("#graphSpacing").val( globalGraphSettings.graphSpacing );

  let elements = [];
  globalRenderedVerticesRID = {};

  globalLastColorIndex = 0;
  for( type in globalGraphSettings.types )
    globalGraphSettings.types[type].color = null;
  globalTotalEdges = 0;

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

    let v = { data: createVertex(vertex), classes: vertex["t"] };
    elements.push( v );

    globalRenderedVerticesRID[rid] = true;

    if( elements.length >= globalGraphMaxResult ){
      reachedMax = true;
      break;
    }
  }

  for( i in globalResultset.edges ) {
    let edge = globalResultset.edges[i];
    if( globalRenderedVerticesRID[edge.i] && globalRenderedVerticesRID[edge.o]  ){
      // DISPLAY ONLY EDGES RELATIVE TO VERTICES THAT ARE PART OF THE GRAPH
      elements.push( { data: createEdge( edge ), classes: edge["t"] } );
      ++globalTotalEdges;
    }
  }

  globalLayout = {
    name: 'cola',
    animate: true,
    refresh: 2,
    ungrabifyWhileSimulating: true,
    nodeSpacing: function( node ){ return globalGraphSettings.graphSpacing },
    spacingFactor: 1.75
  };

  let styles = [
    {
     selector: 'node',
     selectionType: 'single',
     style: {
      'label': 'data(label)',
      'width': 'data(size)',
      'height': 'data(size)',
      'border-color': 'gray',
      'border-width': 0,
      'text-valign': "center",
      'text-halign': "center",
      'text-wrap': 'wrap',
      'text-max-width': 100,
     }
    }, {
     selector: 'node:selected',
     selectionType: 'single',
     style: {
      'label': 'data(label)',
      'width': 'data(size)',
      'height': 'data(size)',
      'border-color': 'red',
      'border-width': 5,
      'text-valign': "center",
      'text-halign': "center",
      'text-wrap': 'wrap',
      'text-max-width': 100,
     }
    }, {
     selector: 'edge',
     style: {
      'width': 1,
      'label': 'data(label)',
      'color': 'gray',
      'line-color': 'gray',
      'target-arrow-color': 'gray',
      'target-arrow-shape': 'triangle',
      'curve-style': 'bezier',
      'edge-text-rotation': 'autorotate',
      'target-arrow-shape': 'triangle',
      'text-outline-color': "#F7F7F7",
      'text-outline-width': 8,
     }
    },
  ];

  assignStyles(styles);

  globalCy = cytoscape({
    container: $('#graph'),
    elements: elements,
    style: styles,
    layout: globalLayout,
  });

  setGraphStyles();

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

    let data = null;
    if( globalSelected.length == 1 ) {
      data = globalSelected[0].data();

      let summary = "<label class='form-label'>Node&nbsp</label><label class='form-label'><b>" + data.id + "</b></label>&nbsp;";
      summary += "<label class='form-label'>Type&nbsp</label><label class='form-label'><b>" + data.type+ "</b></label><br>";
      summary += "<br><h5>Properties</h5>";

      $("#graphPropertiesType").html( summary );

      let table = "<thead><tr><th scope='col'>Name</th><th scope='col'>Value</th></tr>";
      table += "<tbody>";
      for( let p in data.properties )
        table += "<tr><td>"+p+"</td><td>" + data.properties[p]+ "</td>";
      table += "</tbody>";

      $("#graphPropertiesTable").html(table);
    }

    let selectedElementTypes = {};
    for( i = 0; i < globalSelected.length; ++i ){
      let type = globalSelected[i].data()["type"];
      selectedElementTypes[type] = true;
    }

    let type = null;
    if( Object.keys(selectedElementTypes).length == 1 ){
      type = globalSelected[0].data()["type"];
      properties = globalGraphPropertiesPerType[type];

      let sel = getOrCreateStyleTypeAttrib(type, "label");
      if( sel == null )
        sel = "@type";

      layout = "<br><h5>Layout for type "+type+"</h5>";
      layout += "<div class='row'><div class='col-3'><label for='graphLabel' class='form-label'>Label</label></div>";
      layout += "<div class='col-9'><select id='graphLabel' class='form-control'>";
      layout += "<option value='@type'"+(sel == "@type" ? " selected": "" )+">@type</option>" ;
      for( p in properties ){
        layout += "<option value='"+p+"'"+(sel == p ? " selected": "" )+">" + p + "</option>" ;
      }
      layout += "</select></div></div>";

      layout += "<div class='row'><div class='col-3'><label for='graphIcon' class='form-label'>icon</label></div>";
      layout += "<div class='col-9'><div class='form-group'><div class='btn-group'>";

      layout += "<button id='graphIcon' data-selected='graduation-cap' type='button' class='icp icp-dd btn btn-default dropdown-toggle iconpicker-component' data-toggle='dropdown'>";

      let icon = getOrCreateStyleTypeAttrib( type, "icon");
      if( icon == null )
        icon = "";

      layout += "<i class='fa fa-fw "+icon+"'></i><span class='caret'></span></button>";
      layout += "<div class='dropdown-menu'></div>";
      layout += "</div></div></div></div>";

      $("#graphLayout").html( layout );
      $("#graphLabel").change( function(){
        getOrCreateStyleTypeAttrib( type, "label", $("#graphLabel").val() );
        renderGraph();
      });
      $('#graphIcon').iconpicker();
      $("#graphIcon").on( "iconpickerSelected", function(event){
        getOrCreateStyleTypeAttrib( type, "icon", event.iconpickerValue );
        renderGraph();
      });
    }

    if( globalSelected.length == 1 ) {
      let actions = "<br><h5>Actions</h5>";
      actions += "<ul>";
      actions += "<li>Load adjacent <a class='link' href='#' onclick='loadNodeNeighbors(\"out\", \""+data.id+"\")'>outgoing</a>, ";
      actions += "<a class='link' href='#' onclick='loadNodeNeighbors(\"in\", \""+data.id+"\")'>incoming</a> or ";
      actions += "<a class='link' href='#' onclick='loadNodeNeighbors(\"both\", \""+data.id+"\")'>both</a></li>";
      actions += "<li><a class='link' href='#' onclick='removeGraphElement(globalSelected)'>Hide</a> selected elements</li>";
      actions += "<li>Select all the element of type <a class='link' href='#' onclick='selectGraphElementByType(\"" + type+ "\")'>" + type+ "</a></li>";
      actions += "</ul>";

      $("#graphActions").html( actions );
    }
  });

  globalCy.on('select', 'edge', function(event){
    globalSelected = globalCy.elements('edge:selected');
    if( globalSelected.length < 1 ) {
      $("#customToolbar").empty();
      $("#graphPropertiesTable").empty();
      $("#graphPropertiesType").html("Select an element to see its properties");
      return;
    }

    let data = null;
    if( globalSelected.length == 1 ) {
      data = globalSelected[0].data();

      let summary = "<label class='form-label'>Edge&nbsp</label><label class='form-label'><b>" + data.id + "</b></label>&nbsp;";
      summary += "<label class='form-label'>Type&nbsp</label><label class='form-label'><b>" + data.type+ "</b></label><br>";
      summary += "<br><h5>Properties</h5>";

      $("#graphPropertiesType").html( summary );

      let table = "<thead><tr><th scope='col'>Name</th><th scope='col'>Value</th></tr>";
      table += "<tbody>";
      for( let p in data.properties )
        table += "<tr><td>"+p+"</td><td>" + data.properties[p]+ "</td>";
      table += "</tbody>";

      $("#graphPropertiesTable").html(table);
    }

    let selectedElementTypes = {};
    for( i = 0; i < globalSelected.length; ++i ){
      let type = globalSelected[i].data()["type"];
      selectedElementTypes[type] = true;
    }

    let type = null;
    if( Object.keys(selectedElementTypes).length == 1 ){
      type = globalSelected[0].data()["type"];
      properties = globalGraphPropertiesPerType[type];

      let sel = getOrCreateStyleTypeAttrib(type, "label");
      if( sel == null )
        sel = "@type";

      layout = "<br><h5>Layout for type "+type+"</h5>";
      layout += "<div class='row'><div class='col-3'><label for='graphLabel' class='form-label'>Label</label></div>";
      layout += "<div class='col-9'><select id='graphLabel' class='form-control'>";
      layout += "<option value='@type'"+(sel == "@type" ? " selected": "" )+">@type</option>" ;
      for( p in properties ){
        layout += "<option value='"+p+"'"+(sel == p ? " selected": "" )+">" + p + "</option>" ;
      }
      layout += "</select></div></div>";

      $("#graphLayout").html( layout );
      $("#graphLabel").change( function(){
        getOrCreateStyleTypeAttrib(type, "label", $("#graphLabel").val() );
        renderGraph();
      });
    }

    if( globalSelected.length == 1 ) {
      let actions = "<br><h5>Actions</h5>";
      actions += "<ul>";
      actions += "<li><a class='link' href='#' onclick='removeGraphElement(globalSelected)'>Hide</a> selected elements</li>";
      actions += "<li>Select all the element of type <a class='link' href='#' onclick='selectGraphElementByType(\"" + data.type+ "\")'>" + data.type+ "</a></li>";
      actions += "</ul>";

      $("#graphActions").html( actions );
    }
  });

  let warning = null;
  if( reachedMax ){
    warning = "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.";
    globalNotify( "Warning", warning, "warning");
  }

  updateGraphStatus(warning);
}

function createVertex(vertex){
  let type = vertex["t"];
  let label = "@type";
  if( getOrCreateStyleTypeAttrib( type, "label" ) != null )
    label =getOrCreateStyleTypeAttrib( type, "label" );
  if( label == "@type" )
    label = type;
  else
    label = vertex["p"][label];

  return { id: vertex["r"], label: label, size: (70 + ( 2 * label.length ) ), type: type,
           weight: vertex["i"] + vertex["o"],
           properties: vertex["p"] };
}

function createEdge(edge){
  let label = "@type";
  if( getOrCreateStyleTypeAttrib( edge["t"], "label" ) != null )
    label = getOrCreateStyleTypeAttrib( edge["t"], "label" );
  if( label == "@type" )
    label = edge["t"];
  else
    label = edge["p"][label];

  return { id: edge["r"], label: label, type: edge["t"], source: edge["o"], target: edge["i"], properties: edge["p"] };
}

function assignVertexColor(type){
  let color = getOrCreateStyleTypeAttrib(type, "color");
  if( color == null ){
    if( globalLastColorIndex >= globalBgColors.length )
      globalLastColorIndex = 0;
    getOrCreateStyleTypeAttrib(type, "color", globalLastColorIndex++ );
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

function assignStyles(styles){
  if( styles == null )
    styles = [];

  for( type in globalGraphSettings.types ) {
    let colorIndex = getOrCreateStyleTypeAttrib( type, "color" );
    let icon = getOrCreateStyleTypeAttrib( type, "icon" );

    let style = { selector: '.' + type,
                  style: {
                  }};

    if( icon != null ){
      style.style['background-opacity'] = 0;
      style.style['text-valign'] = "bottom";
      style.style['text-max-width'] = 200;
    } else {
      style.style['color'] = globalFgColors[ colorIndex ];
      style.style['background-color'] = globalBgColors[ colorIndex ];
      style.style['border-width'] = 1;
      style.style['text-valign'] = "center";
      style.style['text-max-width'] = 100;
    }

    styles.push( style );
  }

  return styles;
}

function setGraphStyles(){
  let nodeHtmlStyles = [];
  for( type in globalGraphSettings.types ) {
    let colorIndex = getOrCreateStyleTypeAttrib( type, "color" );
    let icon = getOrCreateStyleTypeAttrib( type, "icon" );
    if( icon != null ) {
      nodeHtmlStyles.push(
        {
          query: '.' + type,
          valign: "center",
          halign: "center",
          valignBox: "center",
          tpl: function (data) {
            return "<span style='font-size: 2.2em; color: "+globalBgColors[ colorIndex ]+ "'><i class='"+icon+"'></i></span>";
          }
        }
      );
    }
  }
  globalCy.nodeHtmlLabel(nodeHtmlStyles);
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

  try{
    for( i in elements ) {
      if( !elements[i].data )
        continue;

      let rid = elements[i].data().id;

      arrayRemoveAll(globalResultset.vertices, row => row.r == rid );
      let edgeRemoved = arrayRemoveAll(globalResultset.edges, row => row.r == rid || row.i == rid || row.o == rid );
      globalTotalEdges -= edgeRemoved.length;
      delete globalRenderedVerticesRID[rid];
    }
  } finally {
    updateGraphStatus();
  }
}

function exportGraph(format){
  switch( format ) {
    case "graphml":
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
      break;
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

    let reachedMax = false;
    for( i in data.result.vertices ){
      if( Object.keys(globalRenderedVerticesRID).length >= globalGraphMaxResult ){
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
          data: createVertex( vertex ),
          classes: vertex["t"]
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
          data: createEdge( edge ),
          classes: edge["t"]
        }
      ]);

      ++globalTotalEdges;
    }

    let typeStyles = assignStyles();
    for( i in typeStyles ){
      let s = typeStyles[i];
      globalCy.style().selector(s.selector).style( s.style );
    }

    setGraphStyles();

    globalCy.endBatch();

    globalCy.makeLayout(globalLayout).run();

    let warning = null;
    if( reachedMax ){
      warning = "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.";
      globalNotify( "Warning", warning, "warning");
    }

    updateGraphStatus(warning);
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

function importSettings(){
  var html = "<center><h5>Copy below the JSON configuration to import</h5>";
  html += "<center><textarea id='importContent' rows='30' cols='90'></textarea><br>";
  html += "<button id='importSettings' type='button' class='btn btn-primary'>";
  html += "<i class='fa fa-download'></i> Import settings</button></center>";

  $("#popupBody").html(html);

  $("#importSettings").on( "click", function(){
    let imported = JSON.parse( $("#importContent").val() );
    globalGraphSettings = imported;
    renderGraph();
    $('#popup').modal("hide");
  });

  $("#popupLabel").text("Import Settings");

  $('#popup').on('shown.bs.modal', function () {
    $("#importContent").focus();
  });
  $('#popup').modal("show");
}


function exportSettings(){
  var html = "<center><h5>This is the JSON configuration exported</h5>";
  html += "<center><textarea id='exportContent' rows='30' cols='90'></textarea><br>";
  html += "<button id='popupClipboard' type='button' data-clipboard-target='#exportContent' class='clipboard-trigger btn btn-primary'>";
  html += "<i class='fa fa-copy'></i> Copy to clipboard and close</button></center>";
  $("#popupBody").html(html);

  $("#popupLabel").text("Export Settings");

  $("#exportContent").text( JSON.stringify( globalGraphSettings, null, 2 ) );
  new ClipboardJS("#popupClipboard")
    .on('success', function(e) {
      $('#popup').modal("hide")
    });;

  $('#popup').on('shown.bs.modal', function () {
    $("#exportContent").focus();
  });
  $('#popup').modal();
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

    if( text == data.id )
      el.select();

    if( data.label != null && data.label.indexOf( text ) > -1 )
      el.select();

    if( data.type != null && data.type.indexOf( text ) > -1 )
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

function selectGraphElementByType(type){
  globalCy.elements("[type = '"+type+"']").select();
}

function selectOrphanVertices(){
  globalCy.nodes().filter(function( ele ){
    return ele.outgoers().length == 0 && ele.incomers().length == 0;
  }).select();
}

function invertVertexSelection(){
  let selected = globalCy.nodes(":selected");
  let notSelected = globalCy.nodes().not(":selected");
  selected.unselect();
  notSelected.select();
}

function updateGraphStatus(warning){
  let html = "Displayed <b>" + Object.keys(globalRenderedVerticesRID).length + "</b> vertices and <b>"+globalTotalEdges+"</b> edges.";

  if( warning != null )
    html += " <b>WARNING</b>: " + warning;

  $("#graphStatus").html( html );
}

function getOrCreateStyleTypeAttrib( type, attrib, value ){
  let style = globalGraphSettings.types[type];
  if( style == null ) {
    style = {};
    globalGraphSettings.types[type] = style;
  }

  if( typeof(value) !== 'undefined' )
    style[attrib] = value;

  return style[attrib];
}
