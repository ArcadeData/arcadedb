(function webpackUniversalModuleDefinition(root, factory) {
  if (typeof exports === "object" && typeof module === "object") module.exports = factory();
  else if (typeof define === "function" && define.amd) define([], factory);
  else if (typeof exports === "object") exports["cytoscapeCxtmenu"] = factory();
  else root["cytoscapeCxtmenu"] = factory();
})(this, function () {
  return /******/ (function (modules) {
    // webpackBootstrap
    /******/ // The module cache
    /******/ var installedModules = {};
    /******/
    /******/ // The require function
    /******/ function __webpack_require__(moduleId) {
      /******/
      /******/ // Check if module is in cache
      /******/ if (installedModules[moduleId]) {
        /******/ return installedModules[moduleId].exports;
        /******/
      }
      /******/ // Create a new module (and put it into the cache)
      /******/ var module = (installedModules[moduleId] = {
        /******/ i: moduleId,
        /******/ l: false,
        /******/ exports: {},
        /******/
      });
      /******/
      /******/ // Execute the module function
      /******/ modules[moduleId].call(module.exports, module, module.exports, __webpack_require__);
      /******/
      /******/ // Flag the module as loaded
      /******/ module.l = true;
      /******/
      /******/ // Return the exports of the module
      /******/ return module.exports;
      /******/
    }
    /******/
    /******/
    /******/ // expose the modules object (__webpack_modules__)
    /******/ __webpack_require__.m = modules;
    /******/
    /******/ // expose the module cache
    /******/ __webpack_require__.c = installedModules;
    /******/
    /******/ // identity function for calling harmony imports with the correct context
    /******/ __webpack_require__.i = function (value) {
      return value;
    };
    /******/
    /******/ // define getter function for harmony exports
    /******/ __webpack_require__.d = function (exports, name, getter) {
      /******/ if (!__webpack_require__.o(exports, name)) {
        /******/ Object.defineProperty(exports, name, {
          /******/ configurable: false,
          /******/ enumerable: true,
          /******/ get: getter,
          /******/
        });
        /******/
      }
      /******/
    };
    /******/
    /******/ // getDefaultExport function for compatibility with non-harmony modules
    /******/ __webpack_require__.n = function (module) {
      /******/ var getter =
        module && module.__esModule
          ? /******/ function getDefault() {
              return module["default"];
            }
          : /******/ function getModuleExports() {
              return module;
            };
      /******/ __webpack_require__.d(getter, "a", getter);
      /******/ return getter;
      /******/
    };
    /******/
    /******/ // Object.prototype.hasOwnProperty.call
    /******/ __webpack_require__.o = function (object, property) {
      return Object.prototype.hasOwnProperty.call(object, property);
    };
    /******/
    /******/ // __webpack_public_path__
    /******/ __webpack_require__.p = "";
    /******/
    /******/ // Load entry module and return exports
    /******/ return __webpack_require__((__webpack_require__.s = 4));
    /******/
  })(
    /************************************************************************/
    /******/ [
      /* 0 */
      /***/ function (module, exports, __webpack_require__) {
        "use strict";

        var defaults = __webpack_require__(2);
        var assign = __webpack_require__(1);

        var _require = __webpack_require__(3),
          removeEles = _require.removeEles,
          setStyles = _require.setStyles,
          createElement = _require.createElement,
          getPixelRatio = _require.getPixelRatio,
          getOffset = _require.getOffset;

        var cxtmenu = function cxtmenu(params) {
          var options = assign({}, defaults, params);
          var cy = this;
          var container = cy.container();
          var target = void 0;

          var data = {
            options: options,
            handlers: [],
            container: createElement({ class: "cxtmenu" }),
          };

          var wrapper = data.container;
          var parent = createElement();
          var canvas = createElement({ tag: "canvas" });
          var commands = [];
          var c2d = canvas.getContext("2d");

          var r = 100; // defailt radius;
          var containerSize = (r + options.activePadding) * 2;
          var activeCommandI = void 0;
          var offset = void 0;

          container.insertBefore(wrapper, container.firstChild);
          wrapper.appendChild(parent);
          parent.appendChild(canvas);

          setStyles(wrapper, {
            position: "absolute",
            zIndex: options.zIndex,
            userSelect: "none",
            pointerEvents: "none", // prevent events on menu in modern browsers
          });

          // prevent events on menu in legacy browsers
          ["mousedown", "mousemove", "mouseup", "contextmenu"].forEach(function (evt) {
            wrapper.addEventListener(evt, function (e) {
              e.preventDefault();

              return false;
            });
          });

          setStyles(parent, {
            display: "none",
            width: containerSize + "px",
            height: containerSize + "px",
            position: "absolute",
            zIndex: 1,
            marginLeft: -options.activePadding + "px",
            marginTop: -options.activePadding + "px",
            userSelect: "none",
          });

          canvas.width = containerSize;
          canvas.height = containerSize;

          function createMenuItems(r, rs) {
            removeEles(".cxtmenu-item", parent);
            var dtheta = (2 * Math.PI) / commands.length;
            var theta1 = Math.PI / 2;
            var theta2 = theta1 + dtheta;

            for (var i = 0; i < commands.length; i++) {
              var command = commands[i];

              var midtheta = (theta1 + theta2) / 2;
              var rx1 = ((r + rs) / 2) * Math.cos(midtheta);
              var ry1 = ((r + rs) / 2) * Math.sin(midtheta);

              // Arbitrary multiplier to increase the sizing of the space
              // available for the item.
              var width = 1 * Math.abs((r - rs) * Math.cos(midtheta));
              var height = 1 * Math.abs((r - rs) * Math.sin(midtheta));
              width = Math.max(width, height);

              var item = createElement({ class: "cxtmenu-item" });
              setStyles(item, {
                color: options.itemColor,
                cursor: "default",
                display: "table",
                "text-align": "center",
                //background: 'red',
                position: "absolute",
                "text-shadow":
                  "-1px -1px 2px " +
                  options.itemTextShadowColor +
                  ", 1px -1px 2px " +
                  options.itemTextShadowColor +
                  ", -1px 1px 2px " +
                  options.itemTextShadowColor +
                  ", 1px 1px 1px " +
                  options.itemTextShadowColor,
                left: "50%",
                top: "50%",
                "min-height": width + "px",
                width: width + "px",
                height: width + "px",
                marginLeft: rx1 - width / 2 + "px",
                marginTop: -ry1 - width / 2 + "px",
              });

              var content = createElement({ class: "cxtmenu-content" });

              if (command.content instanceof HTMLElement) {
                content.appendChild(command.content);
              } else {
                content.innerHTML = command.content;
              }

              setStyles(content, {
                width: width + "px",
                height: width + "px",
                "vertical-align": "middle",
                display: "table-cell",
              });

              setStyles(content, command.contentStyle || {});

              if (command.disabled === true || command.enabled === false) {
                content.setAttribute("class", "cxtmenu-content cxtmenu-disabled");
              }

              parent.appendChild(item);
              item.appendChild(content);

              theta1 += dtheta;
              theta2 += dtheta;
            }
          }

          function queueDrawBg(radius, rspotlight) {
            redrawQueue.drawBg = [radius, rspotlight];
          }

          function drawBg(radius, rspotlight) {
            c2d.globalCompositeOperation = "source-over";

            c2d.clearRect(0, 0, containerSize, containerSize);

            // draw background items
            c2d.fillStyle = options.fillColor;
            var dtheta = (2 * Math.PI) / commands.length;
            var theta1 = Math.PI / 2;
            var theta2 = theta1 + dtheta;

            for (var index = 0; index < commands.length; index++) {
              var command = commands[index];

              if (command.fillColor) {
                c2d.fillStyle = command.fillColor;
              }
              c2d.beginPath();
              c2d.moveTo(radius + options.activePadding, radius + options.activePadding);
              c2d.arc(radius + options.activePadding, radius + options.activePadding, radius, 2 * Math.PI - theta1, 2 * Math.PI - theta2, true);
              c2d.closePath();
              c2d.fill();

              theta1 += dtheta;
              theta2 += dtheta;

              c2d.fillStyle = options.fillColor;
            }

            // draw separators between items
            c2d.globalCompositeOperation = "destination-out";
            c2d.strokeStyle = "white";
            c2d.lineWidth = options.separatorWidth;
            theta1 = Math.PI / 2;
            theta2 = theta1 + dtheta;

            for (var i = 0; i < commands.length; i++) {
              var rx1 = radius * Math.cos(theta1);
              var ry1 = radius * Math.sin(theta1);
              c2d.beginPath();
              c2d.moveTo(radius + options.activePadding, radius + options.activePadding);
              c2d.lineTo(radius + options.activePadding + rx1, radius + options.activePadding - ry1);
              c2d.closePath();
              c2d.stroke();

              theta1 += dtheta;
              theta2 += dtheta;
            }

            c2d.fillStyle = "white";
            c2d.globalCompositeOperation = "destination-out";
            c2d.beginPath();
            c2d.arc(radius + options.activePadding, radius + options.activePadding, rspotlight + options.spotlightPadding, 0, Math.PI * 2, true);
            c2d.closePath();
            c2d.fill();

            c2d.globalCompositeOperation = "source-over";
          }

          function queueDrawCommands(rx, ry, radius, theta, rspotlight) {
            redrawQueue.drawCommands = [rx, ry, radius, theta, rspotlight];
          }

          function drawCommands(rx, ry, radius, theta, rs) {
            var dtheta = (2 * Math.PI) / commands.length;
            var theta1 = Math.PI / 2;
            var theta2 = theta1 + dtheta;

            theta1 += dtheta * activeCommandI;
            theta2 += dtheta * activeCommandI;

            c2d.fillStyle = options.activeFillColor;
            c2d.strokeStyle = "black";
            c2d.lineWidth = 1;
            c2d.beginPath();
            c2d.moveTo(radius + options.activePadding, radius + options.activePadding);
            c2d.arc(
              radius + options.activePadding,
              radius + options.activePadding,
              radius + options.activePadding,
              2 * Math.PI - theta1,
              2 * Math.PI - theta2,
              true,
            );
            c2d.closePath();
            c2d.fill();

            c2d.fillStyle = "white";
            c2d.globalCompositeOperation = "destination-out";

            var tx = radius + options.activePadding + (rx / radius) * (rs + options.spotlightPadding - options.indicatorSize / 4);
            var ty = radius + options.activePadding + (ry / radius) * (rs + options.spotlightPadding - options.indicatorSize / 4);
            var rot = Math.PI / 4 - theta;

            c2d.translate(tx, ty);
            c2d.rotate(rot);

            // clear the indicator
            // The indicator size (arrow) depends on the node size as well. If the indicator size is bigger and the rendered node size + padding,
            // use the rendered node size + padding as the indicator size.
            var indicatorSize = options.indicatorSize > rs + options.spotlightPadding ? rs + options.spotlightPadding : options.indicatorSize;
            c2d.beginPath();
            c2d.fillRect(-indicatorSize / 2, -indicatorSize / 2, indicatorSize, indicatorSize);
            c2d.closePath();
            c2d.fill();

            c2d.rotate(-rot);
            c2d.translate(-tx, -ty);

            // c2d.setTransform( 1, 0, 0, 1, 0, 0 );

            // clear the spotlight
            c2d.beginPath();
            c2d.arc(radius + options.activePadding, radius + options.activePadding, rs + options.spotlightPadding, 0, Math.PI * 2, true);
            c2d.closePath();
            c2d.fill();

            c2d.globalCompositeOperation = "source-over";
          }

          function updatePixelRatio() {
            var pxr = getPixelRatio();
            var w = containerSize;
            var h = containerSize;

            canvas.width = w * pxr;
            canvas.height = h * pxr;

            canvas.style.width = w + "px";
            canvas.style.height = h + "px";

            c2d.setTransform(1, 0, 0, 1, 0, 0);
            c2d.scale(pxr, pxr);
          }

          var redrawing = true;
          var redrawQueue = {};

          var raf =
            window.requestAnimationFrame ||
            window.webkitRequestAnimationFrame ||
            window.mozRequestAnimationFrame ||
            window.msRequestAnimationFrame ||
            function (fn) {
              return setTimeout(fn, 16);
            };

          var redraw = function redraw() {
            if (redrawQueue.drawBg) {
              drawBg.apply(null, redrawQueue.drawBg);
            }

            if (redrawQueue.drawCommands) {
              drawCommands.apply(null, redrawQueue.drawCommands);
            }

            redrawQueue = {};

            if (redrawing) {
              raf(redraw);
            }
          };

          // kick off
          updatePixelRatio();
          redraw();

          var ctrx = void 0,
            ctry = void 0,
            rs = void 0;

          var bindings = {
            on: function on(events, selector, fn) {
              var _fn = fn;
              if (selector === "core") {
                _fn = function _fn(e) {
                  if (e.cyTarget === cy || e.target === cy) {
                    // only if event target is directly core
                    return fn.apply(this, [e]);
                  }
                };
              }

              data.handlers.push({
                events: events,
                selector: selector,
                fn: _fn,
              });

              if (selector === "core") {
                cy.on(events, _fn);
              } else {
                cy.on(events, selector, _fn);
              }

              return this;
            },
          };

          function addEventListeners() {
            var grabbable = void 0;
            var inGesture = false;
            var dragHandler = void 0;
            var zoomEnabled = void 0;
            var panEnabled = void 0;
            var boxEnabled = void 0;
            var gestureStartEvent = void 0;

            var restoreZoom = function restoreZoom() {
              if (zoomEnabled) {
                cy.userZoomingEnabled(true);
              }
            };

            var restoreGrab = function restoreGrab() {
              if (grabbable) {
                target.grabify();
              }
            };

            var restorePan = function restorePan() {
              if (panEnabled) {
                cy.userPanningEnabled(true);
              }
            };

            var restoreBoxSeln = function restoreBoxSeln() {
              if (boxEnabled) {
                cy.boxSelectionEnabled(true);
              }
            };

            var restoreGestures = function restoreGestures() {
              restoreGrab();
              restoreZoom();
              restorePan();
              restoreBoxSeln();
            };

            window.addEventListener("resize", updatePixelRatio);

            bindings
              .on("resize", function () {
                updatePixelRatio();
              })
              .on(options.openMenuEvents, options.selector, function (e) {
                target = this; // Remember which node the context menu is for
                var ele = this;
                var isCy = this === cy;

                if (inGesture) {
                  parent.style.display = "none";

                  inGesture = false;

                  restoreGestures();
                }

                if (typeof options.commands === "function") {
                  var res = options.commands(target);
                  if (res.then) {
                    res.then(function (_commands) {
                      commands = _commands;
                      openMenu();
                    });
                  } else {
                    commands = res;
                    openMenu();
                  }
                } else {
                  commands = options.commands;
                  openMenu();
                }

                function openMenu() {
                  if (!commands || commands.length === 0) {
                    return;
                  }

                  zoomEnabled = cy.userZoomingEnabled();
                  cy.userZoomingEnabled(false);

                  panEnabled = cy.userPanningEnabled();
                  cy.userPanningEnabled(false);

                  boxEnabled = cy.boxSelectionEnabled();
                  cy.boxSelectionEnabled(false);

                  grabbable = target.grabbable && target.grabbable();
                  if (grabbable) {
                    target.ungrabify();
                  }

                  var rp = void 0,
                    rw = void 0,
                    rh = void 0,
                    rs = void 0;
                  if (!isCy && ele && ele.isNode instanceof Function && ele.isNode() && !ele.isParent() && !options.atMouse) {
                    // If it's a node, the default spotlight radius for a node is the node width
                    rp = ele.renderedPosition();
                    rw = ele.renderedOuterWidth();
                    rh = ele.renderedOuterHeight();
                    rs = rw / 2;
                    // If adaptativeNodespotlightRadius is not enabled and min|maxSpotlighrRadius is defined, use those instead
                    rs = !options.adaptativeNodeSpotlightRadius && options.minSpotlightRadius ? Math.max(rs, options.minSpotlightRadius) : rs;
                    rs = !options.adaptativeNodeSpotlightRadius && options.maxSpotlightRadius ? Math.min(rs, options.maxSpotlightRadius) : rs;
                  } else {
                    // If it's the background or an edge, the spotlight radius is the min|maxSpotlightRadius
                    rp = e.renderedPosition || e.cyRenderedPosition;
                    rw = 1;
                    rh = 1;
                    rs = rw / 2;
                    rs = options.minSpotlightRadius ? Math.max(rs, options.minSpotlightRadius) : rs;
                    rs = options.maxSpotlightRadius ? Math.min(rs, options.maxSpotlightRadius) : rs;
                  }

                  offset = getOffset(container);

                  ctrx = rp.x;
                  ctry = rp.y;
                  r = rw / 2 + (options.menuRadius instanceof Function ? options.menuRadius(target) : Number(options.menuRadius));
                  containerSize = (r + options.activePadding) * 2;
                  updatePixelRatio();

                  setStyles(parent, {
                    width: containerSize + "px",
                    height: containerSize + "px",
                    display: "block",
                    left: rp.x - r + "px",
                    top: rp.y - r + "px",
                  });
                  createMenuItems(r, rs);
                  queueDrawBg(r, rs);

                  activeCommandI = undefined;

                  inGesture = true;
                  gestureStartEvent = e;
                }
              })
              .on(
                "cxtdrag tapdrag",
                options.selector,
                (dragHandler = function dragHandler(e) {
                  if (!inGesture) {
                    return;
                  }
                  e.preventDefault(); // Otherwise, on mobile, the pull-down refresh gesture gets activated

                  var origE = e.originalEvent;
                  var isTouch = origE.touches && origE.touches.length > 0;

                  var pageX = (isTouch ? origE.touches[0].pageX : origE.pageX) - window.pageXOffset;
                  var pageY = (isTouch ? origE.touches[0].pageY : origE.pageY) - window.pageYOffset;

                  activeCommandI = undefined;

                  var dx = pageX - offset.left - ctrx;
                  var dy = pageY - offset.top - ctry;

                  if (dx === 0) {
                    dx = 0.01;
                  }

                  var d = Math.sqrt(dx * dx + dy * dy);
                  var cosTheta = (dy * dy - d * d - dx * dx) / (-2 * d * dx);
                  var theta = Math.acos(cosTheta);

                  var rw = void 0;
                  if (target && target.isNode instanceof Function && target.isNode() && !target.isParent() && !options.atMouse) {
                    // If it's a node, the default spotlight radius for a node is the node width
                    rw = target.renderedOuterWidth();
                    rs = rw / 2;
                    // If adaptativeNodespotlightRadius is not enabled and min|maxSpotlighrRadius is defined, use those instead
                    rs = !options.adaptativeNodeSpotlightRadius && options.minSpotlightRadius ? Math.max(rs, options.minSpotlightRadius) : rs;
                    rs = !options.adaptativeNodeSpotlightRadius && options.maxSpotlightRadius ? Math.min(rs, options.maxSpotlightRadius) : rs;
                  } else {
                    // If it's the background or an edge, the spotlight radius is the min|maxSpotlightRadius
                    rw = 1;
                    rs = rw / 2;
                    rs = options.minSpotlightRadius ? Math.max(rs, options.minSpotlightRadius) : rs;
                    rs = options.maxSpotlightRadius ? Math.min(rs, options.maxSpotlightRadius) : rs;
                  }

                  r = rw / 2 + (options.menuRadius instanceof Function ? options.menuRadius(target) : Number(options.menuRadius));
                  if (
                    d < rs + options.spotlightPadding ||
                    (typeof options.outsideMenuCancel === "number" && d > r + options.activePadding + options.outsideMenuCancel)
                  ) {
                    //

                    queueDrawBg(r, rs);
                    return;
                  }
                  queueDrawBg(r, rs);

                  var rx = (dx * r) / d;
                  var ry = (dy * r) / d;

                  if (dy > 0) {
                    theta = Math.PI + Math.abs(theta - Math.PI);
                  }

                  var dtheta = (2 * Math.PI) / commands.length;
                  var theta1 = Math.PI / 2;
                  var theta2 = theta1 + dtheta;

                  for (var i = 0; i < commands.length; i++) {
                    var command = commands[i];

                    var inThisCommand = (theta1 <= theta && theta <= theta2) || (theta1 <= theta + 2 * Math.PI && theta + 2 * Math.PI <= theta2);

                    if (command.disabled === true || command.enabled === false) {
                      inThisCommand = false;
                    }

                    if (inThisCommand) {
                      activeCommandI = i;
                      break;
                    }

                    theta1 += dtheta;
                    theta2 += dtheta;
                  }
                  queueDrawCommands(rx, ry, r, theta, rs);
                }),
              )
              .on("tapdrag", dragHandler)
              .on("cxttapend tapend", function () {
                parent.style.display = "none";
                if (activeCommandI !== undefined) {
                  var select = commands[activeCommandI].select;

                  if (select) {
                    select.apply(target, [target, gestureStartEvent]);
                    activeCommandI = undefined;
                  }
                }

                inGesture = false;

                restoreGestures();
              });
          }

          function removeEventListeners() {
            var handlers = data.handlers;

            for (var i = 0; i < handlers.length; i++) {
              var h = handlers[i];

              if (h.selector === "core") {
                cy.off(h.events, h.fn);
              } else {
                cy.off(h.events, h.selector, h.fn);
              }
            }

            window.removeEventListener("resize", updatePixelRatio);
          }

          function destroyInstance() {
            redrawing = false;

            removeEventListeners();

            wrapper.remove();
          }

          addEventListeners();

          return {
            destroy: function destroy() {
              destroyInstance();
            },
          };
        };

        module.exports = cxtmenu;

        /***/
      },
      /* 1 */
      /***/ function (module, exports, __webpack_require__) {
        "use strict";

        // Simple, internal Object.assign() polyfill for options objects etc.

        module.exports =
          Object.assign != null
            ? Object.assign.bind(Object)
            : function (tgt) {
                for (var _len = arguments.length, srcs = Array(_len > 1 ? _len - 1 : 0), _key = 1; _key < _len; _key++) {
                  srcs[_key - 1] = arguments[_key];
                }

                srcs
                  .filter(function (src) {
                    return src != null;
                  })
                  .forEach(function (src) {
                    Object.keys(src).forEach(function (k) {
                      return (tgt[k] = src[k]);
                    });
                  });

                return tgt;
              };

        /***/
      },
      /* 2 */
      /***/ function (module, exports, __webpack_require__) {
        "use strict";

        var defaults = {
          menuRadius: function menuRadius(ele) {
            return 100;
          }, // the radius of the circular menu in pixels
          selector: "node", // elements matching this Cytoscape.js selector will trigger cxtmenus
          commands: [
            // an array of commands to list in the menu or a function that returns the array
            /*
    { // example command
      fillColor: 'rgba(200, 200, 200, 0.75)', // optional: custom background color for item
      content: 'a command name' // html/text content to be displayed in the menu
      contentStyle: {}, // css key:value pairs to set the command's css in js if you want
      select: function(ele){ // a function to execute when the command is selected
        console.log( ele.id() ) // `ele` holds the reference to the active element
      },
      enabled: true // whether the command is selectable
    }
    */
          ], // function( ele ){ return [ /*...*/ ] }, // example function for commands
          fillColor: "rgba(0, 0, 0, 0.75)", // the background colour of the menu
          activeFillColor: "rgba(1, 105, 217, 0.75)", // the colour used to indicate the selected command
          activePadding: 20, // additional size in pixels for the active command
          indicatorSize: 24, // the size in pixels of the pointer to the active command, will default to the node size if the node size is smaller than the indicator size,
          separatorWidth: 3, // the empty spacing in pixels between successive commands
          spotlightPadding: 4, // extra spacing in pixels between the element and the spotlight
          adaptativeNodeSpotlightRadius: false, // specify whether the spotlight radius should adapt to the node size
          minSpotlightRadius: 24, // the minimum radius in pixels of the spotlight (ignored for the node if adaptativeNodeSpotlightRadius is enabled but still used for the edge & background)
          maxSpotlightRadius: 38, // the maximum radius in pixels of the spotlight (ignored for the node if adaptativeNodeSpotlightRadius is enabled but still used for the edge & background)
          openMenuEvents: "cxttapstart taphold", // space-separated cytoscape events that will open the menu; only `cxttapstart` and/or `taphold` work here
          itemColor: "white", // the colour of text in the command's content
          itemTextShadowColor: "transparent", // the text shadow colour of the command's content
          zIndex: 9999, // the z-index of the ui div
          atMouse: false, // draw menu at mouse position
          outsideMenuCancel: false, // if set to a number, this will cancel the command if the pointer is released outside of the spotlight, padded by the number given
        };

        module.exports = defaults;

        /***/
      },
      /* 3 */
      /***/ function (module, exports, __webpack_require__) {
        "use strict";

        var removeEles = function removeEles(query) {
          var ancestor = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : document;

          var els = ancestor.querySelectorAll(query);

          for (var i = 0; i < els.length; i++) {
            var el = els[i];

            el.parentNode.removeChild(el);
          }
        };

        var setStyles = function setStyles(el, style) {
          var props = Object.keys(style);

          for (var i = 0, l = props.length; i < l; i++) {
            el.style[props[i]] = style[props[i]];
          }
        };

        var createElement = function createElement(options) {
          options = options || {};

          var el = document.createElement(options.tag || "div");

          el.className = options.class || "";

          if (options.style) {
            setStyles(el, options.style);
          }

          return el;
        };

        var getPixelRatio = function getPixelRatio() {
          return window.devicePixelRatio || 1;
        };

        var getOffset = function getOffset(el) {
          var offset = el.getBoundingClientRect();

          return {
            left:
              offset.left +
              document.body.scrollLeft +
              parseFloat(getComputedStyle(document.body)["padding-left"]) +
              parseFloat(getComputedStyle(document.body)["border-left-width"]),
            top:
              offset.top +
              document.body.scrollTop +
              parseFloat(getComputedStyle(document.body)["padding-top"]) +
              parseFloat(getComputedStyle(document.body)["border-top-width"]),
          };
        };

        module.exports = { removeEles: removeEles, setStyles: setStyles, createElement: createElement, getPixelRatio: getPixelRatio, getOffset: getOffset };

        /***/
      },
      /* 4 */
      /***/ function (module, exports, __webpack_require__) {
        "use strict";

        var cxtmenu = __webpack_require__(0);

        // registers the extension on a cytoscape lib ref
        var register = function register(cytoscape) {
          if (!cytoscape) {
            return;
          } // can't register if cytoscape unspecified

          cytoscape("core", "cxtmenu", cxtmenu); // register with cytoscape.js
        };

        if (typeof cytoscape !== "undefined") {
          // expose to global cytoscape (i.e. window.cytoscape)
          register(cytoscape);
        }

        module.exports = register;

        /***/
      },
      /******/
    ],
  );
});
