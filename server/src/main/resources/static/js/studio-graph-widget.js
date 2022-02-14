var globalRenderedVerticesRID = {};
var globalTotalEdges = 0;
var globalSelected = null;

function renderGraph(){
  if( globalResultset == null )
    return;

  globalCy = null;

  $("#graphSpacing").val( globalGraphSettings.graphSpacing );

  let elements = [];
  globalRenderedVerticesRID = {};

  globalTotalEdges = 0;

  for( let i in globalResultset.vertices ){
    let vertex = globalResultset.vertices[i];
    assignVertexColor(vertex.t);
    assignProperties(vertex);
  }

  for( let i in globalResultset.edges ){
    let edge = globalResultset.edges[i];
    assignProperties(edge);
  }

  let reachedMax = false;
  for( let i in globalResultset.vertices ){
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

  for( let i in globalResultset.edges ) {
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
      'z-index-compare': 'manual',
      'z-index': 2,
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
      'text-outline-color': "#F7F7F7",
      'text-outline-width': 8,
      'z-index-compare': 'manual',
      'z-index': 1,
     }
    },
  ];

  assignStyles(styles);

  // ADD SELECTED STYLES TO GET PRIORITY OVER OTHER STYLES
  styles.push( {
    selector: 'node:selected',
    style: {
      'color': 'red',
      'border-color': 'red',
      'border-width': 10,
      'z-index-compare': 'manual',
      'z-index': 3,
    }
  });

  styles.push( {
    selector: 'edge:selected',
    style: {
      'color': 'red',
      'line-color': 'red',
      'width': 10,
      'z-index-compare': 'manual',
      'z-index': 3,
    }
  });

  globalCy = cytoscape({
    container: $('#graph'),
    elements: elements,
    style: styles
  });

  initGraph();

  let warning = null;
  if( reachedMax ){
    warning = "Returned more than " + globalGraphMaxResult + " items, partial results will be returned. Consider setting a limit in the query.";
    globalNotify( "Warning", warning, "warning");
  }

  updateGraphStatus(warning);
}

function initGraph(){
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
    displaySelectedNode();
  });

  globalCy.on('select', 'edge', function(event){
    displaySelectedEdge();
  });

  globalCy.makeLayout(globalLayout).run();
}

function createVertex(vertex){
  let type = vertex["t"];

  getOrCreateStyleTypeAttrib( type, "element", "v" );

  let label = "@type";
  if( getOrCreateStyleTypeAttrib( type, "labelText" ) != null )
    label =getOrCreateStyleTypeAttrib( type, "labelText" );
  if( label == "@type" )
    label = type;
  else
    label = vertex["p"][label];

  if( label == null )
    label = "";

  return { id: vertex["r"], label: label, size: (70 + ( 2 * label.length ) ), type: type,
           weight: vertex["i"] + vertex["o"],
           properties: vertex["p"] };
}

function createEdge(edge){
  let type = edge["t"];

  getOrCreateStyleTypeAttrib(type, "element", "e" );

  let label = "@type";
  if( getOrCreateStyleTypeAttrib( type, "labelText" ) != null )
    label = getOrCreateStyleTypeAttrib( type, "labelText" );
  if( label == "@type" )
    label = type;
  else
    label = edge["p"][label];

  if( label == null )
    label = "";

  return { id: edge["r"], label: label, type: type, source: edge["o"], target: edge["i"], properties: edge["p"] };
}

function assignVertexColor(type){
  let color = getOrCreateStyleTypeAttrib(type, "shapeColor");
  if( color == null ){
    if( globalLastColorIndex >= globalBgColors.length )
      globalLastColorIndex = 0;
    getOrCreateStyleTypeAttrib(type, "labelColor", globalFgColors[globalLastColorIndex] );
    getOrCreateStyleTypeAttrib(type, "shapeColor", globalBgColors[globalLastColorIndex] );
    ++globalLastColorIndex;
  }
}

function assignProperties(element){
  let type = element["t"];
  let properties = globalGraphPropertiesPerType[type];
  if( properties == null ) {
    properties = {};
    globalGraphPropertiesPerType[type] = properties;
  }

  for( let p in element.p )
    properties[ p ] = true;;
}

function assignStyles(styles){
  if( styles == null )
    styles = [];

  for( let type in globalGraphSettings.types ) {
    let element = getOrCreateStyleTypeAttrib( type, "element");

    let labelColor = getOrCreateStyleTypeAttrib( type, "labelColor" );
    if( labelColor == null )
      labelColor = "black";

    let borderColor = getOrCreateStyleTypeAttrib( type, "borderColor" );
    if( borderColor == null )
      borderColor = "gray";

    let shapeColor = getOrCreateStyleTypeAttrib( type, "shapeColor" );
    let icon = getOrCreateStyleTypeAttrib( type, "icon" );

    let shapeSize = getOrCreateStyleTypeAttrib( type, "shapeSize");
    if( shapeSize == null )
      shapeSize = element == "v" ? "data(size)" : 1;

    let labelSize = getOrCreateStyleTypeAttrib( type, "labelSize" );
    if( labelSize == null )
      labelSize = 1;

    let style = { selector: '.' + type,
                  style: {
                    'width': shapeSize,
                    'height': shapeSize,
                    'color': labelColor,
                    'background-color': shapeColor,
                    'font-size': labelSize + "em",
                    'z-index': element == "v" ? 2 : 1,
                  }};

    if( element == "e")
      style.style['line-color'] = shapeColor;

    let labelPosition = getOrCreateStyleTypeAttrib( type, "labelPosition");
    let borderSize = getOrCreateStyleTypeAttrib( type, "borderSize" );

    if( icon != null ){
      if( labelPosition == null ){
        labelPosition = "bottom center";
        getOrCreateStyleTypeAttrib( type, "labelPosition", labelPosition);
      }
      labelPosition = labelPosition.split(" ");

      if( borderSize == null )
        borderSize = 0;

      style.style['background-opacity'] = 0;
      style.style['text-max-width'] = 200;
    } else {

      if( labelPosition == null )
        labelPosition = "center center";
      labelPosition = labelPosition.split(" ");

      if( borderSize == null )
        borderSize = 1;

      style.style['text-max-width'] = 100;
    }

    style.style['border-color'] = borderColor;
    style.style['border-width'] = borderSize;
    style.style['text-valign'] = labelPosition[0];
    style.style['text-halign'] = labelPosition[1];

    styles.push( style );
    styles.push( {
      selector: '.' + type + ':selected',
      style: {
        'border-color': 'red',
        'border-width': 5,
      }
    });
  }

  return styles;
}

function setGraphStyles(){
  let nodeHtmlStyles = [];
  for( let type in globalGraphSettings.types ) {
    let iconColor = getOrCreateStyleTypeAttrib( type, "iconColor" );
    if( iconColor == null )
      iconColor = "black";

    let icon = getOrCreateStyleTypeAttrib( type, "icon" );
    if( icon != null ) {
      let iconSize = getOrCreateStyleTypeAttrib( type, "iconSize" );
      if( iconSize == null )
        iconSize = 2;

      let iconPosition = getOrCreateStyleTypeAttrib( type, "iconPosition");
      if( iconPosition == null )
        iconPosition = "center center";
      iconPosition = iconPosition.split(" ");

      nodeHtmlStyles.push(
        {
          query: '.' + type,
          valign: iconPosition[0],
          halign: iconPosition[1],
          valignBox: "center",
          tpl: function (data) {
            return "<span style='font-size: "+iconSize+"em; color: "+iconColor+ "'><i class='"+icon+"'></i></span>";
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
    for( let i in elements ) {
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
    for( let i in data.result.vertices ){
      if( Object.keys(globalRenderedVerticesRID).length >= globalGraphMaxResult ){
        reachedMax = true;
        break;
      }

      let vertex = data.result.vertices[i];

      assignVertexColor(vertex.t);
      assignProperties(vertex);

      globalResultset.vertices.push( vertex );

      globalCy.add([
        {
          group: 'nodes',
          data: createVertex( vertex ),
          classes: vertex["t"]
        }
      ]);

      globalRenderedVerticesRID[vertex.r] = true;
    }

    for( let i in data.result.edges ){
      let edge = data.result.edges[i];

      if( !globalRenderedVerticesRID[edge.i] || !globalRenderedVerticesRID[edge.o] )
        continue;

      assignProperties(edge);

      globalResultset.edges.push( edge );
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

    globalCy.makeLayout(globalLayout).run();

    globalCy.endBatch();

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

function addNodeFromRecord( rid ){
  if( globalResultset == null )
    globalResultset = {};
  if( globalResultset.vertices == null )
    globalResultset.vertices = [];
  if( globalResultset.edges == null )
    globalResultset.edges = [];
  if( globalResultset.records == null )
    globalResultset.records = [];

  let vertex = null;
  for( let i in globalResultset.vertices ){
    let v = globalResultset.vertices[i];
    if( v.r == rid ){
      vertex = v;
      break;
    }
  }

  if( vertex == null ){
    // LOAD FROM THE DATABASE
    jQuery.ajax({
      type: "POST",
      url: "/api/v1/command/" + escapeHtml( $("#inputDatabase").val() ),
      async: false,
      data: JSON.stringify(
        {
          language: "sql",
          command: "select from " + rid,
          serializer: "graph"
        }
      ),
      beforeSend: function (xhr){
        xhr.setRequestHeader('Authorization', globalCredentials);
      }
    }).done(function(data){
      vertex = data.result.vertices[0];
      globalResultset.vertices.push( vertex );
    });
  }

  if( globalCy != null )
    globalCy.elements('node:selected').unselect();
  else
    renderGraph();

  globalActivateTab('tab-graph');

  let node = globalCy.nodes("[id = '"+rid+"']")[0];

  node.select();

  globalCy.makeLayout(globalLayout).run();
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

function displaySelectedNode(){
  if( !globalEnableElementPanel )
    return;

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
  for( let i = 0; i < globalSelected.length; ++i ){
    let type = globalSelected[i].data()["type"];
    selectedElementTypes[type] = true;
  }

  let type = null;
  if( Object.keys(selectedElementTypes).length == 1 ){
    type = globalSelected[0].data()["type"];
    let properties = globalGraphPropertiesPerType[type];

    let labelText = getOrCreateStyleTypeAttrib(type, "labelText");
    if( labelText == null )
      labelText = "@type";

    let labelColor = getOrCreateStyleTypeAttrib(type, "labelColor");
    if( labelColor == null )
      labelColor = 'black';

    let layout = "<div class='row'><div class='col-12'><h5>Layout for type "+type;
    layout += "<button class='btn-pill btn-transition btn text-right' style='padding: 1px 5px;' onclick='globalToggleWidget(\"layoutPanel\", \"layoutExpand\")'><i id='layoutExpand' class='fa fa-"+(globalWidgetExpanded["layoutPanel"]?"minus":"plus")+"'></i></button></h5></div></div>";
    layout += "<div class='p-2 "+(globalWidgetExpanded["layoutPanel"]?"show":"collapse")+"' id='layoutPanel'>";
    layout += "<div class='row'>";

    // LABEL
    layout += "<div class='col-12'><label class='form-label'>Label</label></div>";
    layout += "<div class='col-9'><select id='graphLabel' class='form-control'>";
    layout += "<option value='@type'"+(labelText == "@type" ? " selected": "" )+">@type</option>" ;
    for( let p in properties ){
      layout += "<option value='"+p+"'"+(labelText == p ? " selected": "" )+">" + p + "</option>" ;
    }
    layout += "</select></div>";
    layout += "<div class='col-3'><select id='labelColor' class='form-control'>";
    layout += "  <option value='black'"+(labelColor=='black'?" selected":"")+" data-color='black'>Black</option>";
    layout += "  <option value='white'"+(labelColor=='white'?" selected":"")+" data-color='white'>White</option>";
    layout += "</select></div>";

    let labelPosition = getOrCreateStyleTypeAttrib( type, "labelPosition");
    if( labelPosition == null )
      labelPosition = "center center";

    layout += "<div class='col-6'><select id='graphLabelPosition' class='form-control' oninput='getOrCreateStyleTypeAttrib(\""+type+"\", \"labelPosition\", this.value);renderGraph()'>";
    layout += "  <option value='top left'"+(labelPosition=="top left"?" selected":"")+">Top Left</option>";
    layout += "  <option value='top center'"+(labelPosition=="top center"?" selected":"")+">Top Center</option>";
    layout += "  <option value='top right'"+(labelPosition=="top right"?" selected":"")+">Top Right</option>";
    layout += "  <option value='center left'"+(labelPosition=="center left"?" selected":"")+">Center Left</option>";
    layout += "  <option value='center center'"+(labelPosition=="center center"?" selected":"")+">Center Center</option>";
    layout += "  <option value='center right'"+(labelPosition=="center right"?" selected":"")+">Center Right</option>";
    layout += "  <option value='bottom left'"+(labelPosition=="bottom left"?" selected":"")+">Bottom Left</option>";
    layout += "  <option value='bottom center'"+(labelPosition=="bottom center"?" selected":"")+">Bottom Center</option>";
    layout += "  <option value='bottom right'"+(labelPosition=="bottom right"?" selected":"")+">Bottom Right</option>";
    layout += "</select></div>";

    let labelSize = getOrCreateStyleTypeAttrib( type, "labelSize");
    if( labelSize == null )
      labelSize = 1;

    layout += "<div class='col-6'><input type='range' class='form-control-range form-control' id='labelSize' value='"+labelSize+"' min='1' max='5' step='0.2' oninput='getOrCreateStyleTypeAttrib(\""+type+"\", \"labelSize\", this.value);renderGraph()'></div>";

    // ICON
    let icon = getOrCreateStyleTypeAttrib( type, "icon");
    if( icon == null )
      icon = "";

    let iconSize = getOrCreateStyleTypeAttrib( type, "iconSize");
    if( iconSize == null )
      iconSize = 2;

    let iconColor = getOrCreateStyleTypeAttrib( type, "iconColor");
    if( iconColor == null )
      iconColor = "black";

    layout += "<div class='col-12'><label for='graphIcon' class='form-label'>Icon</label></div>";
    layout += "<div class='col-9'><div class='btn-group'>";
    layout += "<button id='graphIcon' data-selected='graduation-cap' type='button' class='icp icp-dd btn btn-default dropdown-toggle iconpicker-component' data-toggle='dropdown'>";
    layout += "<i class='fa fa-fw "+icon+"'></i><span class='caret'></span></button>";
    layout += "<div class='dropdown-menu'></div>";
    layout += "<button class='btn' onclick='getOrCreateStyleTypeAttrib(\""+type+"\", \"icon\", null);$(\"#graphIcon>svg\").attr(\"class\",\"svg-inline--fa fa-w-16 fa-fw iconpicker-component\").remove();renderGraph()'><i class='fa fa-times'></i></button></div></div>";

    layout += "<div class='col-3'><select id='iconColor' class='form-control'>";
    for( let i in globalBgColors ) {
      let color = globalBgColors[i];
      layout += "  <option value='"+color+"'"+(iconColor==color?" selected":"")+" data-color='"+color+"'>"+color+"</option>";
    }
    layout += "</select></div>";

    let iconPosition = getOrCreateStyleTypeAttrib( type, "iconPosition");
    if( iconPosition == null )
      iconPosition = "center center";

    layout += "<div class='col-6'><select id='graphIconPosition' class='form-control' oninput='getOrCreateStyleTypeAttrib(\""+type+"\", \"iconPosition\", this.value);renderGraph()'>";
    layout += "  <option value='top left'"+(iconPosition=="top left"?" selected":"")+">Top Left</option>";
    layout += "  <option value='top center'"+(iconPosition=="top center"?" selected":"")+">Top Center</option>";
    layout += "  <option value='top right'"+(iconPosition=="top right"?" selected":"")+">Top Right</option>";
    layout += "  <option value='center left'"+(iconPosition=="center left"?" selected":"")+">Center Left</option>";
    layout += "  <option value='center center'"+(iconPosition=="center center"?" selected":"")+">Center Center</option>";
    layout += "  <option value='center right'"+(iconPosition=="center right"?" selected":"")+">Center Right</option>";
    layout += "  <option value='bottom left'"+(iconPosition=="bottom left"?" selected":"")+">Bottom Left</option>";
    layout += "  <option value='bottom center'"+(iconPosition=="bottom center"?" selected":"")+">Bottom Center</option>";
    layout += "  <option value='bottom right'"+(iconPosition=="bottom right"?" selected":"")+">Bottom Right</option>";
    layout += "</select></div>";

    layout += "<div class='col-6'><input type='range' class='form-control-range form-control' id='iconSize' value='"+iconSize+"' min='1' max='5' step='0.2' oninput='getOrCreateStyleTypeAttrib(\""+type+"\", \"iconSize\", this.value);renderGraph()'></div>";

    // BORDER
    let borderSize = getOrCreateStyleTypeAttrib( type, "borderSize");
    if( borderSize == null )
      borderSize = "1";

    let borderColor = getOrCreateStyleTypeAttrib( type, "borderColor");
    if( borderColor == null )
      borderColor = "gray";

    layout += "<div class='col-12'><label for='graphBorder' class='form-label'>Border</label></div>";
    layout += "<div class='col-9'><input type='range' class='form-control-range form-control' id='borderSize' value='"+borderSize+"' min='0' max='10' step='1' oninput='getOrCreateStyleTypeAttrib(\""+type+"\", \"borderSize\", this.value);renderGraph()'></div>";
    layout += "<div class='col-3'><select id='borderColor' class='form-control'>";
    for( let i in globalBgColors ) {
      let color = globalBgColors[i];
      layout += "  <option value='"+color+"'"+(borderColor==color?" selected":"")+" data-color='"+color+"'>"+color+"</option>";
    }
    layout += "</select></div>";

    // SHAPE
    let shapeSize = getOrCreateStyleTypeAttrib( type, "shapeSize");
    if( shapeSize == null )
      shapeSize = "auto";

    let shapeColor = getOrCreateStyleTypeAttrib( type, "shapeColor");
    if( shapeColor == null )
      shapeColor = null;

    layout += "<div class='col-12'><label for='shapeColor' class='form-label'>Shape</label></div>";

    layout += "<div class='col-6'><select id='shapeSizeType' class='form-control' oninput='getOrCreateStyleTypeAttrib(\""+type+"\", \"shapeSize\", this.value == \"auto\" ? null : $(\"#shapeSize\").val() + \"px\" );renderGraph()'>";
    layout += "  <option value='auto'"+(shapeSize=='auto'?" selected":"")+">Auto</option>";
    layout += "  <option value='custom'"+(shapeSize!='auto'?" selected":"")+">Custom</option>";
    layout += "</select></div>";
    layout += "<div class='col-3'><input type='range' class='form-control-range form-control' id='shapeSize' value='"+shapeSize+"' min='100' max='500' step='20' oninput='getOrCreateStyleTypeAttrib(\""+type+"\", \"shapeSize\", this.value + \"px\");renderGraph()'></div>";
    layout += "<div class='col-3'><select id='shapeColor' class='form-control colorselector'>";
    for( let i in globalBgColors ) {
      let color = globalBgColors[i];
      layout += "  <option value='"+color+"'"+(shapeColor==color?" selected":"")+" data-color='"+color+"'>"+color+"</option>";
    }
    layout += "</select></div>";

    layout += "</div></div>";

    $("#graphLayout").html( layout );
    $("#graphLabel").change( function(){
      getOrCreateStyleTypeAttrib( type, "labelText", $("#graphLabel").val() );
      renderGraph();
    });
    $('#graphIcon').iconpicker();
    $("#graphIcon").on( "iconpickerSelected", function(event){
      getOrCreateStyleTypeAttrib( type, "icon", event.iconpickerValue );
      renderGraph();
    });

    $('#labelColor').colorselector({
      callback: function (value, color, title) {
        getOrCreateStyleTypeAttrib( type, "labelColor", value );
        renderGraph();
      }
    });

    $('#iconColor').colorselector({
      callback: function (value, color, title) {
        getOrCreateStyleTypeAttrib( type, "iconColor", value );
        renderGraph();
      }
    });

    $('#borderColor').colorselector({
      callback: function (value, color, title) {
        getOrCreateStyleTypeAttrib( type, "borderColor", value );
        renderGraph();
      }
    });

    $('#shapeColor').colorselector({
      callback: function (value, color, title) {
        getOrCreateStyleTypeAttrib( type, "shapeColor", value );
        renderGraph();
      }
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
}

function displaySelectedEdge(){
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
  for( let i = 0; i < globalSelected.length; ++i ){
    let type = globalSelected[i].data()["type"];
    selectedElementTypes[type] = true;
  }

  let type = null;
  if( Object.keys(selectedElementTypes).length == 1 ){
    type = globalSelected[0].data()["type"];
    let properties = globalGraphPropertiesPerType[type];

    let sel = getOrCreateStyleTypeAttrib(type, "labelText");
    if( sel == null )
      sel = "@type";

    let layout = "<div class='row'><div class='col-12'><h5>Layout for type "+type;
    layout += "<button class='btn-pill btn-transition btn text-right' style='padding: 1px 5px;' onclick='globalToggleWidget(\"layoutPanel\", \"layoutExpand\")'><i id='layoutExpand' class='fa fa-"+(globalWidgetExpanded["layoutPanel"]?"minus":"plus")+"'></i></button></h5></div></div>";

    layout += "<div class='p-2 "+(globalWidgetExpanded["layoutPanel"]?"show":"collapse")+"' id='layoutPanel'>";
    layout += "<div class='row'><div class='col-3'><label for='graphLabel' class='form-label'>Label</label></div>";
    layout += "<div class='col-9'><select id='graphLabel' class='form-control'>";
    layout += "<option value='@type'"+(sel == "@type" ? " selected": "" )+">@type</option>" ;
    for( let p in properties ){
      layout += "<option value='"+p+"'"+(sel == p ? " selected": "" )+">" + p + "</option>" ;
    }
    layout += "</select></div>";

    // SHAPE
    let shapeSize = getOrCreateStyleTypeAttrib( type, "shapeSize");
    if( shapeSize == null )
      shapeSize = 1;

    let shapeColor = getOrCreateStyleTypeAttrib( type, "shapeColor");
    if( shapeColor == null )
      shapeColor = "gray";

    layout += "<div class='col-12'><label for='shapeColor' class='form-label'>Line</label></div>";
    layout += "<div class='col-9'><input type='range' class='form-control-range form-control' id='shapeSize' value='"+shapeSize+"' min='0' max='20' step='1' oninput='getOrCreateStyleTypeAttrib(\""+type+"\", \"shapeSize\", this.value);renderGraph()'></div>";
    layout += "<div class='col-3'><select id='shapeColor' class='form-control colorselector'>";
    for( let i in globalBgColors ) {
      let color = globalBgColors[i];
      layout += "  <option value='"+color+"'"+(shapeColor==color?" selected":"")+" data-color='"+color+"'>"+color+"</option>";
    }
    layout += "</select></div>";

    layout += "</div></div>";

    $("#graphLayout").html( layout );

    $('#shapeColor').colorselector({
      callback: function (value, color, title) {
        getOrCreateStyleTypeAttrib( type, "shapeColor", value );
        renderGraph();
      }
    });

    $("#graphLabel").change( function(){
      getOrCreateStyleTypeAttrib(type, "labelText", $("#graphLabel").val() );
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
}
