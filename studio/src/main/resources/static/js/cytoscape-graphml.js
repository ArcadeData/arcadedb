(function (f) {
  if (typeof exports === "object" && typeof module !== "undefined") {
    module.exports = f();
  } else if (typeof define === "function" && define.amd) {
    define([], f);
  } else {
    var g;
    if (typeof window !== "undefined") {
      g = window;
    } else if (typeof global !== "undefined") {
      g = global;
    } else if (typeof self !== "undefined") {
      g = self;
    } else {
      g = this;
    }
    g.cytoscapeGraphml = f();
  }
})(function () {
  var define, module, exports;
  return (function e(t, n, r) {
    function s(o, u) {
      if (!n[o]) {
        if (!t[o]) {
          var a = typeof require == "function" && require;
          if (!u && a) return a(o, !0);
          if (i) return i(o, !0);
          var f = new Error("Cannot find module '" + o + "'");
          throw ((f.code = "MODULE_NOT_FOUND"), f);
        }
        var l = (n[o] = { exports: {} });
        t[o][0].call(
          l.exports,
          function (e) {
            var n = t[o][1][e];
            return s(n ? n : e);
          },
          l,
          l.exports,
          e,
          t,
          n,
          r,
        );
      }
      return n[o].exports;
    }
    var i = typeof require == "function" && require;
    for (var o = 0; o < r.length; o++) s(r[o]);
    return s;
  })(
    {
      1: [
        function (_dereq_, module, exports) {
          module.exports = function (cy, $, options) {
            function xmlToString(xmlData) {
              var xmlString;
              //IE
              if (window.ActiveXObject) {
                xmlString = xmlData.xml;
              }
              // code for Mozilla, Firefox, Opera, etc.
              else {
                xmlString = new XMLSerializer().serializeToString(xmlData);
              }
              return xmlString;
            }

            function getEleData(ele) {
              var type = ele.isNode() ? "node" : "edge";
              var attrs = ["css", "data", "position"];
              var result = {};

              for (var i = 0; i < attrs.length; i++) {
                var attr = attrs[i];
                var opt = options[type][attr];
                if (!opt) result[attr] = {};
                else if ($.isArray(opt)) {
                  result[attr] = {};
                  for (var j = 0; j < opt.length; j++) {
                    var el = opt[i];
                    if (ele[attr](el)) result[attr][el] = ele[attr](el);
                  }
                } else {
                  var eleAttr = ele[attr]();
                  result[attr] = {};
                  for (var key in eleAttr)
                    if ($.inArray(key, options[type].discludeds) < 0 && key != "parent") result[attr][key] = { value: eleAttr[key], attrType: attr };
                }
              }

              return $.extend(result.css, result.data, result.position);
            }

            function parseNode(ele, xml) {
              var node = $("<node />", xml).attr({ id: ele.id() }).appendTo(xml);

              var eleData = getEleData(ele);
              for (var key in eleData) $("<data />", node).attr({ type: eleData[key].attrType, key: key }).text(eleData[key].value).appendTo(node);

              if (ele.isParent()) {
                var subgraph = $("<graph />", node)
                  .attr({ id: ele.id() + ":" })
                  .appendTo(node);
                ele.children().each(function (child) {
                  parseNode(child, subgraph);
                });
              }

              return node;
            }

            options.node.discludeds.push("id");
            options.edge.discludeds.push("id", "source", "target");

            var xmlDoc = $.parseXML(
              '<?xml version="1.0" encoding="UTF-8"?>\n' +
                '<graphml xmlns="http://graphml.graphdrawing.org/xmlns"\n' +
                'xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"\n' +
                'xsi:schemaLocation="http://graphml.graphdrawing.org/xmlns\n' +
                'http://graphml.graphdrawing.org/xmlns/1.0/graphml.xsd">\n' +
                "  <graph>\n" +
                " </graph>\n" +
                " </graphml>\n",
            );
            var $xml = $(xmlDoc);

            var $graph = $xml.find("graph");

            cy.nodes()
              .orphans()
              .forEach(function (ele) {
                parseNode(ele, $graph);
              });

            cy.edges().forEach(function (ele) {
              var edge = $("<edge />", $graph).attr({ id: ele.id(), source: ele.source().id(), target: ele.target().id() }).appendTo($graph);

              var eleData = getEleData(ele);
              for (var key in eleData) $("<data />", edge).attr({ key: key }).text(eleData[key].value).appendTo(edge);
            });

            return xmlToString(xmlDoc);
          };
        },
        {},
      ],
      2: [
        function (_dereq_, module, exports) {
          module.exports = function (cy, $, options, cyGraphML) {
            function renderNode($graph, $parent) {
              $graph.children("node").each(function () {
                var $node = $(this);

                var settings = {
                  data: { id: $node.attr("id") },
                  css: {},
                  position: {},
                };

                if ($parent != null) settings["data"]["parent"] = $parent.attr("id");

                $node.children("data").each(function () {
                  var $data = $(this);
                  settings["data"][$data.attr("key")] = $data.text();
                });

                cy.add({
                  group: "nodes",
                  data: settings.data,
                  css: settings.css,
                  position: settings.position,
                });

                $node.children("graph").each(function () {
                  var $graph = $(this);

                  renderNode($graph, $node);
                });
              });
            }

            cy.batch(function () {
              xml = $.parseXML(cyGraphML);
              $xml = $(xml);

              $graphs = $xml.find("graph").first();

              $graphs.each(function () {
                var $graph = $(this);

                renderNode($graph, null);

                $graph.find("edge").each(function () {
                  var $edge = $(this);

                  var settings = {
                    data: { id: $edge.attr("id"), source: $edge.attr("source"), target: $edge.attr("target") },
                    css: {},
                    position: {},
                  };

                  $edge.find("data").each(function () {
                    var $data = $(this);
                    settings["data"][$data.attr("key")] = $data.text();
                  });

                  cy.add({
                    group: "edges",
                    data: settings.data,
                    css: settings.css,
                  });
                });
              });
              var layoutOptT = typeof options.layoutBy;
              if (layoutOptT == "string") cy.layout({ name: options.layoutBy }).run();
              else if (layoutOptT == "function") options.layoutBy();
            });
          };
        },
        {},
      ],
      3: [
        function (_dereq_, module, exports) {
          (function () {
            "use strict";

            // registers the extension on a cytoscape lib ref
            var register = function (cytoscape, $) {
              if (!cytoscape || !$) {
                return;
              } // can't register if cytoscape unspecified

              var exporter = _dereq_("./exporter");
              var importer = _dereq_("./importer");

              var options = {
                node: {
                  css: false,
                  data: true,
                  position: true,
                  discludeds: [],
                },
                edge: {
                  css: false,
                  data: true,
                  discludeds: [],
                },
                layoutBy: "cose", // string of layout name or layout function
              };

              cytoscape("core", "graphml", function (cyGraphML) {
                var cy = this;
                var res;

                switch (typeof cyGraphML) {
                  case "string": // import
                    res = importer(cy, $, options, cyGraphML);
                    break;
                  case "object": // set options
                    $.extend(true, options, cyGraphML);
                    res = cy;
                    break;
                  case "undefined": // export
                    res = exporter(cy, $, options);
                    break;
                  default:
                    console.log("Functionality(argument) of .graphml() is not recognized.");
                }

                return res;
              });
            };

            if (typeof module !== "undefined" && module.exports) {
              // expose as a commonjs module
              module.exports = register;
            }

            if (typeof define !== "undefined" && define.amd) {
              // expose as an amd/requirejs module
              define("cytoscape-graphml", function () {
                return register;
              });
            }

            if (typeof cytoscape !== "undefined" && typeof $ !== "undefined") {
              // expose to global cytoscape (i.e. window.cytoscape)
              register(cytoscape, $);
            }
          })();
        },
        { "./exporter": 1, "./importer": 2 },
      ],
    },
    {},
    [3],
  )(3);
});

//# sourceMappingURL=data:application/json;charset:utf-8;base64,eyJ2ZXJzaW9uIjozLCJzb3VyY2VzIjpbIm5vZGVfbW9kdWxlcy9icm93c2VyLXBhY2svX3ByZWx1ZGUuanMiLCJzcmMvZXhwb3J0ZXIuanMiLCJzcmMvaW1wb3J0ZXIuanMiLCJzcmMvaW5kZXguanMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6IkFBQUE7QUNBQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTs7QUN0R0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBOztBQzFFQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQTtBQUNBO0FBQ0E7QUFDQSIsImZpbGUiOiJnZW5lcmF0ZWQuanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlc0NvbnRlbnQiOlsiKGZ1bmN0aW9uIGUodCxuLHIpe2Z1bmN0aW9uIHMobyx1KXtpZighbltvXSl7aWYoIXRbb10pe3ZhciBhPXR5cGVvZiByZXF1aXJlPT1cImZ1bmN0aW9uXCImJnJlcXVpcmU7aWYoIXUmJmEpcmV0dXJuIGEobywhMCk7aWYoaSlyZXR1cm4gaShvLCEwKTt2YXIgZj1uZXcgRXJyb3IoXCJDYW5ub3QgZmluZCBtb2R1bGUgJ1wiK28rXCInXCIpO3Rocm93IGYuY29kZT1cIk1PRFVMRV9OT1RfRk9VTkRcIixmfXZhciBsPW5bb109e2V4cG9ydHM6e319O3Rbb11bMF0uY2FsbChsLmV4cG9ydHMsZnVuY3Rpb24oZSl7dmFyIG49dFtvXVsxXVtlXTtyZXR1cm4gcyhuP246ZSl9LGwsbC5leHBvcnRzLGUsdCxuLHIpfXJldHVybiBuW29dLmV4cG9ydHN9dmFyIGk9dHlwZW9mIHJlcXVpcmU9PVwiZnVuY3Rpb25cIiYmcmVxdWlyZTtmb3IodmFyIG89MDtvPHIubGVuZ3RoO28rKylzKHJbb10pO3JldHVybiBzfSkiLCJcclxubW9kdWxlLmV4cG9ydHMgPSBmdW5jdGlvbiAoY3ksICQsIG9wdGlvbnMpIHtcclxuXHJcbiAgZnVuY3Rpb24geG1sVG9TdHJpbmcoeG1sRGF0YSkge1xyXG5cclxuICAgIHZhciB4bWxTdHJpbmc7XHJcbiAgICAvL0lFXHJcbiAgICBpZiAod2luZG93LkFjdGl2ZVhPYmplY3QpIHtcclxuICAgICAgeG1sU3RyaW5nID0geG1sRGF0YS54bWw7XHJcbiAgICB9XHJcbiAgICAvLyBjb2RlIGZvciBNb3ppbGxhLCBGaXJlZm94LCBPcGVyYSwgZXRjLlxyXG4gICAgZWxzZSB7XHJcbiAgICAgIHhtbFN0cmluZyA9IChuZXcgWE1MU2VyaWFsaXplcigpKS5zZXJpYWxpemVUb1N0cmluZyh4bWxEYXRhKTtcclxuICAgIH1cclxuICAgIHJldHVybiB4bWxTdHJpbmc7XHJcbiAgfVxyXG5cclxuXHJcbiAgZnVuY3Rpb24gZ2V0RWxlRGF0YShlbGUpIHtcclxuICAgIHZhciB0eXBlID0gZWxlLmlzTm9kZSgpID8gXCJub2RlXCIgOiBcImVkZ2VcIjtcclxuICAgIHZhciBhdHRycyA9IFtcImNzc1wiLCBcImRhdGFcIiwgXCJwb3NpdGlvblwiXTtcclxuICAgIHZhciByZXN1bHQgPSB7fTtcclxuXHJcbiAgICBmb3IgKHZhciBpID0gMDsgaSA8IGF0dHJzLmxlbmd0aDsgaSsrKSB7XHJcbiAgICAgIHZhciBhdHRyID0gYXR0cnNbaV07XHJcbiAgICAgIHZhciBvcHQgPSBvcHRpb25zW3R5cGVdW2F0dHJdO1xyXG4gICAgICBpZiAoIW9wdClcclxuICAgICAgICByZXN1bHRbYXR0cl0gPSB7fTtcclxuICAgICAgZWxzZSBpZiAoJC5pc0FycmF5KG9wdCkpIHtcclxuICAgICAgICByZXN1bHRbYXR0cl0gPSB7fTtcclxuICAgICAgICBmb3IgKHZhciBqID0gMDsgaiA8IG9wdC5sZW5ndGg7IGorKykge1xyXG4gICAgICAgICAgdmFyIGVsID0gb3B0W2ldO1xyXG4gICAgICAgICAgaWYgKGVsZVthdHRyXShlbCkpXHJcbiAgICAgICAgICAgIHJlc3VsdFthdHRyXVtlbF0gPSBlbGVbYXR0cl0oZWwpO1xyXG4gICAgICAgIH1cclxuICAgICAgfSBlbHNlIHtcclxuICAgICAgICB2YXIgZWxlQXR0ciA9IGVsZVthdHRyXSgpO1xyXG4gICAgICAgIHJlc3VsdFthdHRyXSA9IHt9O1xyXG4gICAgICAgIGZvciAodmFyIGtleSBpbiBlbGVBdHRyKVxyXG4gICAgICAgICAgaWYgKCQuaW5BcnJheShrZXksIG9wdGlvbnNbdHlwZV0uZGlzY2x1ZGVkcykgPCAwICYmIGtleSAhPSBcInBhcmVudFwiKVxyXG4gICAgICAgICAgICByZXN1bHRbYXR0cl1ba2V5XSA9IHt2YWx1ZTogZWxlQXR0cltrZXldLCBhdHRyVHlwZTogYXR0cn07XHJcblxyXG4gICAgICB9XHJcbiAgICB9XHJcblxyXG4gICAgcmV0dXJuICQuZXh0ZW5kKHJlc3VsdC5jc3MsIHJlc3VsdC5kYXRhLCByZXN1bHQucG9zaXRpb24pO1xyXG4gIH1cclxuXHJcblxyXG4gIGZ1bmN0aW9uIHBhcnNlTm9kZShlbGUsIHhtbCkge1xyXG4gICAgdmFyIG5vZGUgPSAkKCc8bm9kZSAvPicsIHhtbCkuYXR0cih7aWQ6IGVsZS5pZCgpfSkuYXBwZW5kVG8oeG1sKTtcclxuXHJcbiAgICB2YXIgZWxlRGF0YSA9IGdldEVsZURhdGEoZWxlKTtcclxuICAgIGZvciAodmFyIGtleSBpbiBlbGVEYXRhKVxyXG4gICAgICAkKCc8ZGF0YSAvPicsIG5vZGUpLmF0dHIoe3R5cGU6IGVsZURhdGFba2V5XS5hdHRyVHlwZSwga2V5OiBrZXl9KS50ZXh0KGVsZURhdGFba2V5XS52YWx1ZSkuYXBwZW5kVG8obm9kZSk7XHJcblxyXG5cclxuICAgIGlmIChlbGUuaXNQYXJlbnQoKSkge1xyXG4gICAgICB2YXIgc3ViZ3JhcGggPSAkKCc8Z3JhcGggLz4nLCBub2RlKS5hdHRyKHtpZDogZWxlLmlkKCkgKyAnOid9KS5hcHBlbmRUbyhub2RlKTtcclxuICAgICAgZWxlLmNoaWxkcmVuKCkuZWFjaChmdW5jdGlvbiAoY2hpbGQpIHtcclxuICAgICAgICBwYXJzZU5vZGUoY2hpbGQsIHN1YmdyYXBoKTtcclxuICAgICAgfSk7XHJcbiAgICB9XHJcblxyXG4gICAgcmV0dXJuIG5vZGU7XHJcbiAgfVxyXG5cclxuXHJcbiAgb3B0aW9ucy5ub2RlLmRpc2NsdWRlZHMucHVzaChcImlkXCIpO1xyXG4gIG9wdGlvbnMuZWRnZS5kaXNjbHVkZWRzLnB1c2goXCJpZFwiLCBcInNvdXJjZVwiLCBcInRhcmdldFwiKTtcclxuXHJcbiAgdmFyIHhtbERvYyA9ICQucGFyc2VYTUwoXHJcbiAgICAgICAgICAnPD94bWwgdmVyc2lvbj1cIjEuMFwiIGVuY29kaW5nPVwiVVRGLThcIj8+XFxuJyArXHJcbiAgICAgICAgICAnPGdyYXBobWwgeG1sbnM9XCJodHRwOi8vZ3JhcGhtbC5ncmFwaGRyYXdpbmcub3JnL3htbG5zXCJcXG4nICtcclxuICAgICAgICAgICd4bWxuczp4c2k9XCJodHRwOi8vd3d3LnczLm9yZy8yMDAxL1hNTFNjaGVtYS1pbnN0YW5jZVwiXFxuJyArXHJcbiAgICAgICAgICAneHNpOnNjaGVtYUxvY2F0aW9uPVwiaHR0cDovL2dyYXBobWwuZ3JhcGhkcmF3aW5nLm9yZy94bWxuc1xcbicgK1xyXG4gICAgICAgICAgJ2h0dHA6Ly9ncmFwaG1sLmdyYXBoZHJhd2luZy5vcmcveG1sbnMvMS4wL2dyYXBobWwueHNkXCI+XFxuJyArXHJcbiAgICAgICAgICAnICA8Z3JhcGg+XFxuJyArXHJcbiAgICAgICAgICAnIDwvZ3JhcGg+XFxuJyArXHJcbiAgICAgICAgICAnIDwvZ3JhcGhtbD5cXG4nXHJcbiAgICAgICAgICApO1xyXG4gIHZhciAkeG1sID0gJCh4bWxEb2MpO1xyXG5cclxuICB2YXIgJGdyYXBoID0gJHhtbC5maW5kKFwiZ3JhcGhcIik7XHJcblxyXG4gIGN5Lm5vZGVzKCkub3JwaGFucygpLmZvckVhY2goZnVuY3Rpb24gKGVsZSkge1xyXG4gICAgcGFyc2VOb2RlKGVsZSwgJGdyYXBoKTtcclxuICB9KTtcclxuXHJcbiAgY3kuZWRnZXMoKS5mb3JFYWNoKGZ1bmN0aW9uIChlbGUpIHtcclxuXHJcbiAgICB2YXIgZWRnZSA9ICQoJzxlZGdlIC8+JywgJGdyYXBoKS5hdHRyKHtpZDogZWxlLmlkKCksIHNvdXJjZTogZWxlLnNvdXJjZSgpLmlkKCksIHRhcmdldDogZWxlLnRhcmdldCgpLmlkKCl9KS5hcHBlbmRUbygkZ3JhcGgpO1xyXG5cclxuICAgIHZhciBlbGVEYXRhID0gZ2V0RWxlRGF0YShlbGUpO1xyXG4gICAgZm9yICh2YXIga2V5IGluIGVsZURhdGEpXHJcbiAgICAgICQoJzxkYXRhIC8+JywgZWRnZSkuYXR0cih7a2V5OiBrZXl9KS50ZXh0KGVsZURhdGFba2V5XS52YWx1ZSkuYXBwZW5kVG8oZWRnZSk7XHJcblxyXG4gIH0pO1xyXG5cclxuXHJcbiAgcmV0dXJuIHhtbFRvU3RyaW5nKHhtbERvYyk7XHJcbn07XHJcbiIsIm1vZHVsZS5leHBvcnRzID0gZnVuY3Rpb24gKGN5LCAkLCBvcHRpb25zLCBjeUdyYXBoTUwpIHtcclxuICBmdW5jdGlvbiByZW5kZXJOb2RlKCRncmFwaCwgJHBhcmVudCkge1xyXG4gICAgJGdyYXBoLmNoaWxkcmVuKFwibm9kZVwiKS5lYWNoKGZ1bmN0aW9uICgpIHtcclxuICAgICAgdmFyICRub2RlID0gJCh0aGlzKTtcclxuXHJcbiAgICAgIHZhciBzZXR0aW5ncyA9IHtcclxuICAgICAgICBkYXRhOiB7aWQ6ICRub2RlLmF0dHIoXCJpZFwiKX0sXHJcbiAgICAgICAgY3NzOiB7fSxcclxuICAgICAgICBwb3NpdGlvbjoge31cclxuICAgICAgfTtcclxuXHJcbiAgICAgIGlmKCRwYXJlbnQgIT0gbnVsbClcclxuICAgICAgICBzZXR0aW5nc1tcImRhdGFcIl1bXCJwYXJlbnRcIl0gPSAkcGFyZW50LmF0dHIoXCJpZFwiKTtcclxuXHJcbiAgICAgICRub2RlLmNoaWxkcmVuKCdkYXRhJykuZWFjaChmdW5jdGlvbiAoKSB7XHJcbiAgICAgICAgdmFyICRkYXRhID0gJCh0aGlzKTtcclxuICAgICAgICBzZXR0aW5nc1tcImRhdGFcIl1bJGRhdGEuYXR0cihcImtleVwiKV0gPSAkZGF0YS50ZXh0KCk7XHJcbiAgICAgIH0pO1xyXG5cclxuICAgICAgY3kuYWRkKHtcclxuICAgICAgICBncm91cDogXCJub2Rlc1wiLFxyXG4gICAgICAgIGRhdGE6IHNldHRpbmdzLmRhdGEsXHJcbiAgICAgICAgY3NzOiBzZXR0aW5ncy5jc3MsXHJcbiAgICAgICAgcG9zaXRpb246IHNldHRpbmdzLnBvc2l0aW9uXHJcbiAgICAgIH0pO1xyXG5cclxuICAgICAgJG5vZGUuY2hpbGRyZW4oXCJncmFwaFwiKS5lYWNoKGZ1bmN0aW9uICgpIHtcclxuICAgICAgICB2YXIgJGdyYXBoID0gJCh0aGlzKTtcclxuXHJcbiAgICAgICAgcmVuZGVyTm9kZSgkZ3JhcGgsICRub2RlKTtcclxuICAgICAgfSk7XHJcbiAgICB9KTtcclxuICB9XHJcblxyXG4gIGN5LmJhdGNoKGZ1bmN0aW9uICgpIHtcclxuICAgIHhtbCA9ICQucGFyc2VYTUwoY3lHcmFwaE1MKTtcclxuICAgICR4bWwgPSAkKHhtbCk7XHJcblxyXG4gICAgJGdyYXBocyA9ICR4bWwuZmluZChcImdyYXBoXCIpLmZpcnN0KCk7XHJcblxyXG4gICAgJGdyYXBocy5lYWNoKGZ1bmN0aW9uICgpIHtcclxuICAgICAgdmFyICRncmFwaCA9ICQodGhpcyk7XHJcblxyXG4gICAgICByZW5kZXJOb2RlKCRncmFwaCwgbnVsbCk7XHJcblxyXG4gICAgICAkZ3JhcGguZmluZChcImVkZ2VcIikuZWFjaChmdW5jdGlvbiAoKSB7XHJcbiAgICAgICAgdmFyICRlZGdlID0gJCh0aGlzKTtcclxuXHJcbiAgICAgICAgdmFyIHNldHRpbmdzID0ge1xyXG4gICAgICAgICAgZGF0YToge2lkOiAkZWRnZS5hdHRyKFwiaWRcIiksIHNvdXJjZTogJGVkZ2UuYXR0cihcInNvdXJjZVwiKSwgdGFyZ2V0OiAkZWRnZS5hdHRyKFwidGFyZ2V0XCIpfSxcclxuICAgICAgICAgIGNzczoge30sXHJcbiAgICAgICAgICBwb3NpdGlvbjoge31cclxuICAgICAgICB9O1xyXG5cclxuICAgICAgICAkZWRnZS5maW5kKCdkYXRhJykuZWFjaChmdW5jdGlvbiAoKSB7XHJcbiAgICAgICAgICB2YXIgJGRhdGEgPSAkKHRoaXMpO1xyXG4gICAgICAgICAgc2V0dGluZ3NbXCJkYXRhXCJdWyRkYXRhLmF0dHIoXCJrZXlcIildID0gJGRhdGEudGV4dCgpO1xyXG4gICAgICAgIH0pO1xyXG5cclxuICAgICAgICBjeS5hZGQoe1xyXG4gICAgICAgICAgZ3JvdXA6IFwiZWRnZXNcIixcclxuICAgICAgICAgIGRhdGE6IHNldHRpbmdzLmRhdGEsXHJcbiAgICAgICAgICBjc3M6IHNldHRpbmdzLmNzc1xyXG4gICAgICAgIH0pO1xyXG4gICAgICB9KTtcclxuXHJcbiAgICB9KTtcclxuICAgIHZhciBsYXlvdXRPcHRUID0gdHlwZW9mIG9wdGlvbnMubGF5b3V0Qnk7XHJcbiAgICBpZiAobGF5b3V0T3B0VCA9PSBcInN0cmluZ1wiKVxyXG4gICAgICAgY3kubGF5b3V0KHtuYW1lOiBvcHRpb25zLmxheW91dEJ5fSkucnVuKCk7XHJcbiAgICBlbHNlIGlmIChsYXlvdXRPcHRUID09IFwiZnVuY3Rpb25cIilcclxuICAgICAgb3B0aW9ucy5sYXlvdXRCeSgpO1xyXG4gIH0pO1xyXG59O1xyXG4iLCI7XHJcbihmdW5jdGlvbiAoKSB7XHJcbiAgJ3VzZSBzdHJpY3QnO1xyXG5cclxuICAvLyByZWdpc3RlcnMgdGhlIGV4dGVuc2lvbiBvbiBhIGN5dG9zY2FwZSBsaWIgcmVmXHJcbiAgdmFyIHJlZ2lzdGVyID0gZnVuY3Rpb24gKGN5dG9zY2FwZSwgJCkge1xyXG5cclxuICAgIGlmICghY3l0b3NjYXBlIHx8ICEkKSB7XHJcbiAgICAgIHJldHVybjtcclxuICAgIH0gLy8gY2FuJ3QgcmVnaXN0ZXIgaWYgY3l0b3NjYXBlIHVuc3BlY2lmaWVkXHJcblxyXG4gICAgdmFyIGV4cG9ydGVyID0gcmVxdWlyZShcIi4vZXhwb3J0ZXJcIik7XHJcbiAgICB2YXIgaW1wb3J0ZXIgPSByZXF1aXJlKFwiLi9pbXBvcnRlclwiKTtcclxuXHJcblxyXG4gICAgdmFyIG9wdGlvbnMgPSB7XHJcbiAgICAgIG5vZGU6IHtcclxuICAgICAgICBjc3M6IGZhbHNlLFxyXG4gICAgICAgIGRhdGE6IHRydWUsXHJcbiAgICAgICAgcG9zaXRpb246IHRydWUsXHJcbiAgICAgICAgZGlzY2x1ZGVkczogW11cclxuICAgICAgfSxcclxuICAgICAgZWRnZToge1xyXG4gICAgICAgIGNzczogZmFsc2UsXHJcbiAgICAgICAgZGF0YTogdHJ1ZSxcclxuICAgICAgICBkaXNjbHVkZWRzOiBbXVxyXG4gICAgICB9LFxyXG4gICAgICBsYXlvdXRCeTogXCJjb3NlXCIgLy8gc3RyaW5nIG9mIGxheW91dCBuYW1lIG9yIGxheW91dCBmdW5jdGlvblxyXG4gICAgfTtcclxuXHJcblxyXG5cclxuICAgIGN5dG9zY2FwZSgnY29yZScsICdncmFwaG1sJywgZnVuY3Rpb24gKGN5R3JhcGhNTCkge1xyXG4gICAgICB2YXIgY3kgPSB0aGlzO1xyXG4gICAgICB2YXIgcmVzO1xyXG5cclxuICAgICAgc3dpdGNoICh0eXBlb2YgY3lHcmFwaE1MKSB7XHJcbiAgICAgICAgY2FzZSBcInN0cmluZ1wiOiAvLyBpbXBvcnRcclxuICAgICAgICAgIHJlcyA9IGltcG9ydGVyKGN5LCAkLCBvcHRpb25zLCBjeUdyYXBoTUwpO1xyXG4gICAgICAgICAgYnJlYWs7XHJcbiAgICAgICAgY2FzZSBcIm9iamVjdFwiOiAvLyBzZXQgb3B0aW9uc1xyXG4gICAgICAgICAgJC5leHRlbmQodHJ1ZSwgb3B0aW9ucywgY3lHcmFwaE1MKTtcclxuICAgICAgICAgIHJlcyA9IGN5O1xyXG4gICAgICAgICAgYnJlYWs7XHJcbiAgICAgICAgY2FzZSBcInVuZGVmaW5lZFwiOiAvLyBleHBvcnRcclxuICAgICAgICAgIHJlcyA9IGV4cG9ydGVyKGN5LCAkLCBvcHRpb25zKTtcclxuICAgICAgICAgIGJyZWFrO1xyXG4gICAgICAgIGRlZmF1bHQ6XHJcbiAgICAgICAgICBjb25zb2xlLmxvZyhcIkZ1bmN0aW9uYWxpdHkoYXJndW1lbnQpIG9mIC5ncmFwaG1sKCkgaXMgbm90IHJlY29nbml6ZWQuXCIpO1xyXG4gICAgICB9XHJcblxyXG4gICAgICByZXR1cm4gcmVzO1xyXG5cclxuICAgIH0pO1xyXG5cclxuICB9O1xyXG5cclxuICBpZiAodHlwZW9mIG1vZHVsZSAhPT0gJ3VuZGVmaW5lZCcgJiYgbW9kdWxlLmV4cG9ydHMpIHsgLy8gZXhwb3NlIGFzIGEgY29tbW9uanMgbW9kdWxlXHJcbiAgICBtb2R1bGUuZXhwb3J0cyA9IHJlZ2lzdGVyO1xyXG4gIH1cclxuXHJcbiAgaWYgKHR5cGVvZiBkZWZpbmUgIT09ICd1bmRlZmluZWQnICYmIGRlZmluZS5hbWQpIHsgLy8gZXhwb3NlIGFzIGFuIGFtZC9yZXF1aXJlanMgbW9kdWxlXHJcbiAgICBkZWZpbmUoJ2N5dG9zY2FwZS1ncmFwaG1sJywgZnVuY3Rpb24gKCkge1xyXG4gICAgICByZXR1cm4gcmVnaXN0ZXI7XHJcbiAgICB9KTtcclxuICB9XHJcblxyXG4gIGlmICh0eXBlb2YgY3l0b3NjYXBlICE9PSAndW5kZWZpbmVkJyAmJiB0eXBlb2YgJCAhPT0gJ3VuZGVmaW5lZCcpIHsgLy8gZXhwb3NlIHRvIGdsb2JhbCBjeXRvc2NhcGUgKGkuZS4gd2luZG93LmN5dG9zY2FwZSlcclxuICAgIHJlZ2lzdGVyKGN5dG9zY2FwZSwgJCk7XHJcbiAgfVxyXG5cclxufSkoKTtcclxuIl19
