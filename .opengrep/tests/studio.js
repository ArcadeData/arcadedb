function render(name, row, x, el, container, staticTemplate) {
  // ruleid: studio-html-sink-with-concatenation
  $("#x").html("<b>" + name + "</b>");
  // ruleid: studio-html-sink-with-concatenation
  el.innerHTML += row;
  // ruleid: studio-html-sink-with-concatenation
  container.append(`<div>${x}</div>`);
  // ok: studio-html-sink-with-concatenation
  $("#x").text(name);
  // ok: studio-html-sink-with-concatenation
  $("#x").html(staticTemplate);
}
