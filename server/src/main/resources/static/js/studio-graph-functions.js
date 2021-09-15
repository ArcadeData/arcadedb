function cutSelection(){
  let selected = globalCy.elements(':selected');
  if( selected.length == 0 )
    return;

  removeGraphElement( selected );
  globalCy.makeLayout(globalLayout).run();
}

function cropSelection(){
  let selected = globalCy.nodes().not(globalCy.nodes(':selected'));
  if( selected.length == 0 )
    return;

  removeGraphElement( selected );
  globalCy.makeLayout(globalLayout).run();
}

function searchInGraph(){
  let text = $("#inputGraphSearch").val().trim();
  if( text == "" )
    return;

  let selected = globalCy.nodes(":selected");

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

  if( !globalGraphSettings.cumulativeSelection )
    selected.unselect();
}

function selectGraphElementByType(type){
  let selected = globalCy.nodes(":selected");

  globalCy.elements("[type = '"+type+"']").select();

  if( !globalGraphSettings.cumulativeSelection )
    selected.unselect();
}

function selectOrphanVertices(){
  let selected = globalCy.nodes(":selected");

  globalCy.nodes().filter(function( ele ){
    return ele.outgoers().length == 0 && ele.incomers().length == 0;
  }).select();

  if( !globalGraphSettings.cumulativeSelection )
    selected.unselect();
}

function invertVertexSelection(){
  let selected = globalCy.nodes(":selected");
  let notSelected = globalCy.nodes().not(":selected");
  selected.unselect();
  notSelected.select();
}

function selectNeighbors(depth){
  if( depth < 1 )
    depth = 1;

  let selected = globalCy.nodes(":selected");

  let out = selected.outgoers();
  let inc = selected.incomers();

  for( let i = 1; i < depth; ++i ){
    out = out.outgoers();
    inc = inc.incomers();
  }
  out.select();
  inc.select();

  if( !globalGraphSettings.cumulativeSelection )
    selected.unselect();
}

function shortestPath(){
  let selected = globalCy.nodes(":selected");
  if( selected.length != 2 ){
    globalAlert("Select 2 nodes");
    return;
  }
  var dijkstra = globalCy.elements().dijkstra( selected[0] );
  var pathToJ = dijkstra.pathTo( selected[1] );

  pathToJ.select();
}
