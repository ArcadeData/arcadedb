var globalWidgetExpanded = {};

function _globalModalIcon(icon) {
  if (icon === "error" || icon === "danger")
    return '<i class="fa fa-circle-exclamation" style="color:#dc3545;font-size:1.2rem"></i>';
  if (icon === "warning")
    return '<i class="fa fa-triangle-exclamation" style="color:#ffc107;font-size:1.2rem"></i>';
  if (icon === "info")
    return '<i class="fa fa-circle-info" style="color:#0dcaf0;font-size:1.2rem"></i>';
  if (icon === "success")
    return '<i class="fa fa-circle-check" style="color:#28a745;font-size:1.2rem"></i>';
  return '';
}

function globalAlert(title, text, icon, callback) {
  if (!icon) icon = "error";

  var el = document.getElementById('globalModal');
  var label = document.getElementById('globalModalLabel');
  var iconEl = document.getElementById('globalModalIcon');
  var body = document.getElementById('globalModalBody');
  var footer = document.getElementById('globalModalFooter');

  label.textContent = title || '';
  iconEl.innerHTML = _globalModalIcon(icon);
  body.innerHTML = text || '';
  footer.innerHTML = '<button type="button" class="btn btn-primary" data-bs-dismiss="modal">OK</button>';

  var modal = bootstrap.Modal.getOrCreateInstance(el);

  function onHidden() {
    el.removeEventListener('hidden.bs.modal', onHidden);
    if (callback) callback();
  }
  el.addEventListener('hidden.bs.modal', onHidden);

  modal.show();
}

function globalConfirm(title, text, icon, yes, no) {
  var el = document.getElementById('globalModal');
  var label = document.getElementById('globalModalLabel');
  var iconEl = document.getElementById('globalModalIcon');
  var body = document.getElementById('globalModalBody');
  var footer = document.getElementById('globalModalFooter');

  label.textContent = title || '';
  iconEl.innerHTML = _globalModalIcon(icon);
  body.innerHTML = text || '';

  var confirmed = false;
  footer.innerHTML =
    '<button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>' +
    '<button type="button" class="btn btn-primary" id="globalModalConfirmBtn">Yes</button>';

  var modal = bootstrap.Modal.getOrCreateInstance(el);

  var confirmBtn = document.getElementById('globalModalConfirmBtn');
  function onConfirm() {
    confirmed = true;
    modal.hide();
  }
  confirmBtn.addEventListener('click', onConfirm);

  function onHidden() {
    el.removeEventListener('hidden.bs.modal', onHidden);
    confirmBtn.removeEventListener('click', onConfirm);
    if (confirmed) {
      if (yes) yes();
    } else {
      if (no) no();
    }
  }
  el.addEventListener('hidden.bs.modal', onHidden);

  modal.show();
}

function globalPrompt(title, bodyHtml, confirmText, callback) {
  var el = document.getElementById('globalModal');
  var label = document.getElementById('globalModalLabel');
  var iconEl = document.getElementById('globalModalIcon');
  var body = document.getElementById('globalModalBody');
  var footer = document.getElementById('globalModalFooter');

  label.textContent = title || '';
  iconEl.innerHTML = '';
  body.innerHTML = bodyHtml || '';

  var confirmed = false;
  footer.innerHTML =
    '<button type="button" class="btn btn-secondary" data-bs-dismiss="modal">Cancel</button>' +
    '<button type="button" class="btn btn-primary" id="globalModalConfirmBtn">' + (confirmText || 'OK') + '</button>';

  var modal = bootstrap.Modal.getOrCreateInstance(el);

  var confirmBtn = document.getElementById('globalModalConfirmBtn');
  function onConfirm() {
    confirmed = true;
    modal.hide();
  }
  confirmBtn.addEventListener('click', onConfirm);

  function onHidden() {
    el.removeEventListener('hidden.bs.modal', onHidden);
    confirmBtn.removeEventListener('click', onConfirm);
    if (confirmed && callback) callback();
  }
  el.addEventListener('hidden.bs.modal', onHidden);

  modal.show();

  // Focus first input in body if present
  var firstInput = body.querySelector('input, textarea');
  if (firstInput)
    setTimeout(function() { firstInput.focus(); }, 200);
}

function globalNotifyError(response) {
  try {
    let json = JSON.parse(response);

    let title = json.error ? json.error : "Error";
    let message = json.detail ? json.detail : "Error on execution of the command";

    globalNotify(title, message, "danger");
  } catch (e) {
    globalNotify("Error", response, "danger");
  }
}


function globalActivateTab(tab) {
  $('.nav a[href="#' + tab + '"]')?.tab("show");
}

function globalSetCookie(key, value, expiry) {
  var expires = new Date();
  expires.setTime(expires.getTime() + expiry * 24 * 60 * 60 * 1000);
  document.cookie = key + "=" + value + ";expires=" + expires.toUTCString() + ";path=/";
}

function globalGetCookie(key) {
  var keyValue = document.cookie.match("(^|;) ?" + key + "=([^;]*)(;|$)");
  return keyValue ? keyValue[2] : null;
}

function globalEraseCookie(key) {
  var keyValue = globalGetCookie(key);
  globalSetCookie(key, keyValue, "-1");
  return keyValue;
}

function escapeHtml(unsafe) {
  if (unsafe == null) return null;

  if (typeof unsafe === "object") unsafe = JSON.stringify(unsafe);
  else unsafe = unsafe.toString();

  return unsafe.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;").replace(/'/g, "&#039;");
}

function arrayRemove(array, predicate) {
  for (var i = 0; i < array.length; i++) {
    if (predicate(array[i])) {
      return array.splice(i, 1);
    }
  }
}

function arrayRemoveAll(array, predicate) {
  var removed = [];

  for (var i = 0; i < array.length; ) {
    if (predicate(array[i])) {
      removed.push(array.splice(i, 1));
      continue;
    }
    i++;
  }
  return removed;
}

function globalStorageLoad(key, defaultValue) {
  let value = localStorage.getItem(key);
  return value == null ? defaultValue : value;
}

function globalStorageSave(key, value) {
  localStorage.setItem(key, value);
}

function globalToggleCheckbox(element) {
  $(element).prop("checked", !$(element).prop("checked"));
}

function globalToggleCheckboxAndSave(element, storageKey) {
  let checked = !$(element).prop("checked");
  $(element).prop("checked", checked);
  globalStorageSave(storageKey, checked);
}

function globalCheckboxAndSave(element, storageKey) {
  let checked = $(element).prop("checked");
  $(element).prop("checked", checked);
  globalStorageSave(storageKey, checked);
}

function globalTogglePanel(panelId1, panelId2, panelId3, panelId4, panelId5) {
  $("#" + panelId1).collapse("toggle");
  if (panelId2) $("#" + panelId2).collapse("toggle");
  if (panelId3) $("#" + panelId3).collapse("toggle");
  if (panelId4) $("#" + panelId4).collapse("toggle");
  if (panelId5) $("#" + panelId5).collapse("toggle");
  return false;
}

function globalToggleWidget(panelId, expandButtonId) {
  if ($("#" + panelId).hasClass("show")) {
    $("#" + expandButtonId).removeClass("fa-minus");
    $("#" + expandButtonId).addClass("fa-plus");
    globalWidgetExpanded[panelId] = false;
  } else {
    $("#" + expandButtonId).removeClass("fa-plus");
    $("#" + expandButtonId).addClass("fa-minus");
    globalWidgetExpanded[panelId] = true;
  }
  globalTogglePanel(panelId);
  return false;
}

function saveAs(blob, filename) {
  if (window.navigator.msSaveOrOpenBlob) {
    window.navigator.msSaveBlob(blob, filename);
  } else {
    const elem = window.document.createElement("a");
    elem.href = window.URL.createObjectURL(blob);
    elem.download = filename;
    document.body.appendChild(elem);
    elem.click();
    document.body.removeChild(elem);
  }
}

function base64ToBlob(base64Image) {
  const parts = base64Image.split(";base64,");
  const imageType = parts[0].split(":")[1];
  const decodedData = window.atob(parts[1]);
  const uInt8Array = new Uint8Array(decodedData.length);
  for (let i = 0; i < decodedData.length; ++i) {
    uInt8Array[i] = decodedData.charCodeAt(i);
  }
  return new Blob([uInt8Array], { type: imageType });
}

function getURLParameter(param) {
  var sPageURL = window.location.search.substring(1);
  var sURLVariables = sPageURL.split("&");
  for (var i = 0; i < sURLVariables.length; i++) {
    var paramName = sURLVariables[i].split("=");
    if (paramName[0] == param) return paramName[1];
  }
}

function globalFormatDouble(x, decimals) {
  if (typeof x == "undefined") return "";

  if (typeof decimals == "undefined") decimals = 3;

  let transformed = globalFormatDoubleNoCommas(x, decimals);

  let pos = transformed.indexOf(".");
  let integer = pos > -1 ? transformed.substr(0, pos) : transformed;
  let decimal = pos > -1 ? transformed.substr(pos) : "";

  return integer.replace(/\B(?=(\d{3})+(?!\d))/g, ",") + decimal;
}

function globalFormatDoubleNoCommas(x, decimals) {
  if (typeof x == "undefined") return "";

  if (decimals == undefined) decimals = 2;

  x = x.toString();
  var sep = x.indexOf(".");

  if (decimals > 0) {
    if (sep == -1) x += ".";

    for (let i = 0; i < decimals; ++i) {
      x += "0";
    }
  }

  sep = x.indexOf(".");
  if (sep > -1) {
    if (decimals == 0) x = x.substring(0, sep);
    else x = x.substring(0, sep + decimals + 1);
  }

  return x;
}

function globalFormatSpace(value) {
  if (typeof value == "undefined" || value == "") return "0";
  if (value > 1000000000000) return globalFormatDouble(value / 1000000000000) + "TB";
  if (value > 1000000000) return globalFormatDouble(value / 1000000000) + "GB";
  if (value > 1000000) return globalFormatDouble(value / 1000000) + "MB";
  if (value > 1000) return globalFormatDouble(value / 1000) + "KB";
  return value;
}
