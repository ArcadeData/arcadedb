/*
 * Copyright © 2021-present Arcade Data Ltd (info@arcadedata.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * SPDX-FileCopyrightText: 2021-present Arcade Data Ltd (info@arcadedata.com)
 * SPDX-License-Identifier: Apache-2.0
 */

// Animated network graph canvas for login page
(function() {
  var nodes = [];
  var edges = [];
  var canvas, ctx;
  var animFrame;
  var isRunning = false;
  var NODE_COUNT = 50;
  var CONNECT_DIST = 160;
  var mouseX = -1000, mouseY = -1000;

  function initNetwork() {
    canvas = document.getElementById('loginNetworkCanvas');
    if (!canvas) return;
    ctx = canvas.getContext('2d');
    resize();
    createNodes();
    isRunning = true;
    animate();

    canvas.parentElement.addEventListener('mousemove', function(e) {
      var rect = canvas.getBoundingClientRect();
      mouseX = e.clientX - rect.left;
      mouseY = e.clientY - rect.top;
    });

    canvas.parentElement.addEventListener('mouseleave', function() {
      mouseX = -1000;
      mouseY = -1000;
    });

    window.addEventListener('resize', function() {
      if (isRunning) resize();
    });
  }

  function resize() {
    if (!canvas || !canvas.parentElement) return;
    var dpr = window.devicePixelRatio || 1;
    var rect = canvas.parentElement.getBoundingClientRect();
    canvas.width = rect.width * dpr;
    canvas.height = rect.height * dpr;
    canvas.style.width = rect.width + 'px';
    canvas.style.height = rect.height + 'px';
    ctx.scale(dpr, dpr);
  }

  function createNodes() {
    nodes = [];
    var w = canvas.parentElement.offsetWidth;
    var h = canvas.parentElement.offsetHeight;
    for (var i = 0; i < NODE_COUNT; i++) {
      nodes.push({
        x: Math.random() * w,
        y: Math.random() * h,
        vx: (Math.random() - 0.5) * 0.4,
        vy: (Math.random() - 0.5) * 0.4,
        r: Math.random() * 2 + 1.5,
        pulse: Math.random() * Math.PI * 2
      });
    }
  }

  function animate() {
    if (!isRunning) return;
    var w = canvas.parentElement.offsetWidth;
    var h = canvas.parentElement.offsetHeight;
    ctx.clearRect(0, 0, w, h);

    // Update positions
    for (var i = 0; i < nodes.length; i++) {
      var n = nodes[i];
      n.x += n.vx;
      n.y += n.vy;
      n.pulse += 0.015;

      // Mouse repulsion
      var dx = n.x - mouseX;
      var dy = n.y - mouseY;
      var dist = Math.sqrt(dx * dx + dy * dy);
      if (dist < 120 && dist > 0) {
        var force = (120 - dist) / 120 * 0.8;
        n.vx += (dx / dist) * force;
        n.vy += (dy / dist) * force;
      }

      // Damping
      n.vx *= 0.995;
      n.vy *= 0.995;

      // Boundaries
      if (n.x < 0) { n.x = 0; n.vx *= -1; }
      if (n.x > w) { n.x = w; n.vx *= -1; }
      if (n.y < 0) { n.y = 0; n.vy *= -1; }
      if (n.y > h) { n.y = h; n.vy *= -1; }
    }

    // Draw edges
    for (var i = 0; i < nodes.length; i++) {
      for (var j = i + 1; j < nodes.length; j++) {
        var dx = nodes[i].x - nodes[j].x;
        var dy = nodes[i].y - nodes[j].y;
        var dist = Math.sqrt(dx * dx + dy * dy);
        if (dist < CONNECT_DIST) {
          var alpha = (1 - dist / CONNECT_DIST) * 0.25;
          ctx.beginPath();
          ctx.moveTo(nodes[i].x, nodes[i].y);
          ctx.lineTo(nodes[j].x, nodes[j].y);
          ctx.strokeStyle = 'rgba(0, 174, 238, ' + alpha + ')';
          ctx.lineWidth = 1;
          ctx.stroke();
        }
      }
    }

    // Draw nodes
    for (var i = 0; i < nodes.length; i++) {
      var n = nodes[i];
      var glow = 0.4 + Math.sin(n.pulse) * 0.3;
      var r = n.r + Math.sin(n.pulse) * 0.5;

      // Glow
      ctx.beginPath();
      ctx.arc(n.x, n.y, r + 3, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(0, 174, 238, ' + (glow * 0.15) + ')';
      ctx.fill();

      // Node
      ctx.beginPath();
      ctx.arc(n.x, n.y, r, 0, Math.PI * 2);
      ctx.fillStyle = 'rgba(0, 174, 238, ' + glow + ')';
      ctx.fill();
    }

    animFrame = requestAnimationFrame(animate);
  }

  window.loginNetworkInit = initNetwork;

  window.loginNetworkStop = function() {
    isRunning = false;
    if (animFrame) cancelAnimationFrame(animFrame);
  };

  window.loginNetworkStart = function() {
    if (!isRunning) {
      isRunning = true;
      resize();
      if (nodes.length === 0) createNodes();
      animate();
    }
  };
})();
