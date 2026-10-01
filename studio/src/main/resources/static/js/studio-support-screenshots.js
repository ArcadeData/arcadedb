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

// Screenshots in the Support page. A screenshot of a query result is the most common attachment of a support issue, so the
// user can PASTE one (Ctrl/Cmd+V), drop one, or pick one, in the form that opens an issue and in a reply. Each picture is
// staged on this server as soon as it is added (api/v1/server/support/screenshots) and shown back as a thumbnail the user can
// remove; it is sent to ArcadeData only when the user clicks Send, by id, so the rule of the whole page holds: the user reviews
// exactly what is sent.
//
// What a picture is comes from its first bytes (the same rules as the server and the customer portal): PNG, JPEG, GIF or WebP,
// at most 5 MB, at most 5 per send. SVG is not accepted: it is a document that can carry script.

var SUPPORT_SHOT_MAX_BYTES = 5 * 1024 * 1024;
var SUPPORT_SHOT_MAX_COUNT = 5;

/** `image/png`, `image/jpeg`, `image/gif` or `image/webp` for what these bytes are, or null when they are not an accepted picture. */
function supportShotType(bytes) {
  if (!bytes || bytes.length < 12) return null;
  function ascii(from, text) {
    for (var i = 0; i < text.length; i++) if (bytes[from + i] !== text.charCodeAt(i)) return false;
    return true;
  }
  if (bytes[0] === 0x89 && ascii(1, "PNG") && bytes[4] === 0x0d && bytes[5] === 0x0a && bytes[6] === 0x1a && bytes[7] === 0x0a) return "image/png";
  if (bytes[0] === 0xff && bytes[1] === 0xd8 && bytes[2] === 0xff) return "image/jpeg";
  if (ascii(0, "GIF8") && (bytes[4] === 0x37 || bytes[4] === 0x39) && bytes[5] === 0x61) return "image/gif";
  if (ascii(0, "RIFF") && ascii(8, "WEBP")) return "image/webp";
  return null;
}

/** Base64 of some bytes, in slices so a large picture does not exceed the call stack of String.fromCharCode. */
function supportShotBase64(bytes) {
  var out = "";
  for (var i = 0; i < bytes.length; i += 0x8000) out += String.fromCharCode.apply(null, bytes.subarray(i, i + 0x8000));
  return btoa(out);
}

/** The picture files in a clipboard or drag-and-drop payload (a list of items or of files); everything else is ignored. */
function supportShotsFrom(list) {
  var out = [];
  if (!list) return out;
  for (var i = 0; i < list.length; i++) {
    var item = list[i];
    var file = item && typeof item.getAsFile === "function" ? (item.kind === "file" ? item.getAsFile() : null) : item;
    if (file && typeof file.type === "string" && file.type.indexOf("image/") === 0) out.push(file);
  }
  return out;
}

// ---------------------------------------------------------------------------------------------- state and rendering

/** prefix -> [{id, url, size}] of the pictures staged for that form. */
var supportShots = {};

function supportShotsIds(prefix) {
  return (supportShots[prefix] || []).map(function (s) {
    return s.id;
  });
}

function supportShotsHtml(prefix) {
  return (
    '<div class="support-shots mb-2" data-shots-area="' + prefix + '">' +
    '<div class="d-flex flex-wrap align-items-center gap-2">' +
    '<label class="btn btn-sm btn-outline-secondary mb-0"><i class="fa fa-image"></i> Add a screenshot' +
    '<input type="file" class="sp-shot-pick" data-prefix="' + prefix + '" accept="image/png,image/jpeg,image/gif,image/webp" multiple hidden></label>' +
    '<span class="support-hint">or paste one here (Ctrl/Cmd+V), or drop it. It is sent only when you click Send.</span></div>' +
    '<div class="support-shots-list mt-2" id="' + prefix + 'ShotsList"></div><div id="' + prefix + 'ShotsAlert"></div></div>'
  );
}

function supportShotsRefresh(prefix) {
  var html = "";
  (supportShots[prefix] || []).forEach(function (s) {
    html +=
      '<span class="support-shot"><img src="' + s.url + '" alt="Screenshot to send">' +
      '<button type="button" class="sp-shot-remove" data-prefix="' + prefix + '" data-id="' + supportEsc(s.id) + '" title="Remove" aria-label="Remove this screenshot">&times;</button></span>';
  });
  $("#" + prefix + "ShotsList").html(html);
}

function supportShotsAlert(prefix, message) {
  $("#" + prefix + "ShotsAlert").html(
    message ? '<div class="alert alert-warning py-1 px-2 mt-1" style="font-size: 0.82rem;" role="alert">' + supportEsc(message) + "</div>" : "",
  );
}

/** Forgets the pictures of a form (it is closed, redrawn or sent): the object URLs are released and the server drops its copies. */
function supportShotsClear(prefix, sent) {
  (supportShots[prefix] || []).forEach(function (s) {
    URL.revokeObjectURL(s.url);
    if (!sent) supportApi("DELETE", "/screenshots/" + encodeURIComponent(s.id));
  });
  supportShots[prefix] = [];
  supportShotsRefresh(prefix);
  supportShotsAlert(prefix, "");
}

// ---------------------------------------------------------------------------------------------- adding

/** Checks a picture, stages it on this server and shows it back. Reports the first problem in the form's alert. */
function supportShotsAdd(prefix, blob) {
  var list = supportShots[prefix] || (supportShots[prefix] = []);
  if (list.length >= SUPPORT_SHOT_MAX_COUNT) return supportShotsAlert(prefix, "At most " + SUPPORT_SHOT_MAX_COUNT + " screenshots can be sent at a time.");
  if (blob.size > SUPPORT_SHOT_MAX_BYTES) return supportShotsAlert(prefix, "A screenshot is at most " + SUPPORT_SHOT_MAX_BYTES / (1024 * 1024) + " MB.");
  blob.arrayBuffer().then(function (buffer) {
    var bytes = new Uint8Array(buffer);
    var type = supportShotType(bytes);
    if (!type) return supportShotsAlert(prefix, "That is not a PNG, JPEG, GIF or WebP image.");
    supportShotsAlert(prefix, "");
    var url = URL.createObjectURL(new Blob([bytes], { type: type }));
    supportApi("POST", "/screenshots", { data: supportShotBase64(bytes) })
      .done(function (text) {
        var staged = supportParse(text) || {};
        if (!staged.id || (supportShots[prefix] || []).length >= SUPPORT_SHOT_MAX_COUNT) {
          URL.revokeObjectURL(url);
          return;
        }
        supportShots[prefix].push({ id: staged.id, url: url, size: bytes.length });
        supportShotsRefresh(prefix);
      })
      .fail(function (jqXHR) {
        URL.revokeObjectURL(url);
        supportShotsAlert(prefix, supportError(jqXHR).message);
      });
  });
}

// ---------------------------------------------------------------------------------------------- events

// A form that takes screenshots carries data-shots="<prefix>": pasting, dropping or picking inside it adds to that form
$(document).on("paste", "[data-shots]", function (e) {
  var prefix = $(this).attr("data-shots");
  var clipboard = e.originalEvent && e.originalEvent.clipboardData;
  var files = supportShotsFrom(clipboard && clipboard.items);
  if (!files.length) return; // text: the textarea handles it
  e.preventDefault();
  files.forEach(function (f) {
    supportShotsAdd(prefix, f);
  });
});

$(document).on("dragover", "[data-shots]", function (e) {
  var types = (e.originalEvent && e.originalEvent.dataTransfer && e.originalEvent.dataTransfer.types) || [];
  if (Array.prototype.indexOf.call(types, "Files") >= 0) e.preventDefault();
});

$(document).on("drop", "[data-shots]", function (e) {
  var prefix = $(this).attr("data-shots");
  var transfer = e.originalEvent && e.originalEvent.dataTransfer;
  var files = supportShotsFrom(transfer && transfer.files);
  if (!files.length) return;
  e.preventDefault();
  files.forEach(function (f) {
    supportShotsAdd(prefix, f);
  });
});

$(document).on("change", ".sp-shot-pick", function () {
  var prefix = $(this).attr("data-prefix");
  supportShotsFrom(this.files).forEach(function (f) {
    supportShotsAdd(prefix, f);
  });
  $(this).val("");
});

$(document).on("click", ".sp-shot-remove", function () {
  var prefix = $(this).attr("data-prefix");
  var id = $(this).attr("data-id");
  var list = supportShots[prefix] || [];
  var kept = [];
  list.forEach(function (s) {
    if (s.id === id) {
      URL.revokeObjectURL(s.url);
      supportApi("DELETE", "/screenshots/" + encodeURIComponent(s.id));
    } else kept.push(s);
  });
  supportShots[prefix] = kept;
  supportShotsRefresh(prefix);
  supportShotsAlert(prefix, "");
});
