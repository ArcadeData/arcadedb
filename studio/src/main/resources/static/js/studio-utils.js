var globalWidgetExpanded = {};

function globalAlert(title, text, icon, callback) {
  if (!icon) icon = "error";

  let swal = Swal.fire({
    title: title,
    html: text,
    icon: icon,
  }).then((result) => {
    if (callback) callback();
  });
}

function globalConfirm(title, text, icon, yes, no) {
  let swal = Swal.fire({
    title: title,
    html: text,
    icon: icon,
    showCancelButton: true,
    confirmButtonColor: "#3ac47d",
    cancelButtonColor: "red",
  }).then((result) => {
    if (result.value) {
      if (yes) yes();
    } else {
      if (no) no();
    }
  });
}

function globalNotifyError(response) {
  try {
    let json = JSON.parse(response);

    let title = json.error ? json.error : "Error";
    let message = json.detail ? json.detail : "Error on execution of the command";

    globalNotify(title, escapeHtml(message), "danger");
  } catch (e) {
    globalNotify("Error", escapeHtml(response), "danger");
  }
}

function globalNotify(title, message, type) {
  $.notify(
    {
      title: "<strong>" + title + "</strong>",
      message: message,
    },
    {
      type: type,
      z_index: 100000,
      placement: {
        from: "top",
        align: "center",
      },
    },
  );
}


function globalActivateTab(tab) {
  $('.nav a[href="#' + tab + '"]').tab("show");
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
  // Split into two parts
  const parts = base64Image.split(";base64,");

  // Hold the content type
  const imageType = parts[0].split(":")[1];

  // Decode Base64 string
  const decodedData = window.atob(parts[1]);

  // Create UNIT8ARRAY of size same as row data length
  const uInt8Array = new Uint8Array(decodedData.length);

  // Insert all character code into uInt8Array
  for (let i = 0; i < decodedData.length; ++i) {
    uInt8Array[i] = decodedData.charCodeAt(i);
  }

  // Return BLOB image after conversion
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
