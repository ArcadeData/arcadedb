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

// Bootstrap 4 → 5 Compatibility Shims
$(document).ready(function() {
  $('[data-toggle]').each(function() {
    const toggle = $(this).attr('data-toggle');
    $(this).attr('data-bs-toggle', toggle);
  });
  $('[data-dismiss]').each(function() {
    const dismiss = $(this).attr('data-dismiss');
    $(this).attr('data-bs-dismiss', dismiss);
  });
});

// Notification system using Bootstrap Toasts
var isToastReady = false;
var notificationQueue = [];

document.addEventListener('DOMContentLoaded', function() {
  isToastReady = true;
  processNotificationQueue();
});

function processNotificationQueue() {
  while (notificationQueue.length > 0 && isToastReady) {
    var n = notificationQueue.shift();
    showNotification(n.title, n.message, n.type, n.delay, n.tag);
  }
}

function showNotification(title, message, type, delay, tag) {
  var container = document.getElementById('toastContainer');
  if (!container) return;

  var isError = (type === 'danger' || type === 'error');
  var isWarning = (type === 'warning');
  var iconClass = isError ? 'fa-circle-exclamation' : (isWarning ? 'fa-triangle-exclamation' : 'fa-circle-check');
  var iconColor = isError ? '#dc3545' : (isWarning ? '#ffc107' : '#28a745');

  var toastEl = document.createElement('div');
  toastEl.className = 'toast studio-toast';
  toastEl.setAttribute('role', 'alert');
  if (tag) toastEl.setAttribute('data-notify-tag', tag);
  toastEl.innerHTML =
    '<div class="toast-header">' +
      '<i class="fa ' + iconClass + ' me-2" style="color:' + iconColor + '"></i>' +
      '<strong class="me-auto"></strong>' +
      '<button type="button" class="btn-close" data-bs-dismiss="toast"></button>' +
    '</div>' +
    '<div class="toast-body"></div>';
  toastEl.querySelector('.toast-header strong').textContent = title || '';
  toastEl.querySelector('.toast-body').textContent = message || '';

  container.appendChild(toastEl);
  // Errors and warnings stay visible until the user dismisses them; informational toasts auto-hide.
  var options = (isError || isWarning)
    ? (delay ? { delay: delay } : { autohide: false })
    : { delay: delay || 3500 };
  var toast = new bootstrap.Toast(toastEl, options);
  toast.show();
  toastEl.addEventListener('hidden.bs.toast', function() { toastEl.remove(); });
}

function globalNotify(title, message, type, delay, tag) {
  if (!isToastReady) {
    notificationQueue.push({ title: title, message: message, type: type, delay: delay, tag: tag });
    return;
  }
  showNotification(title, message, type, delay, tag);
}

// Dismisses any currently visible (or still-queued) toast created with the given tag.
// Used to clear a still-open "Session Expired" warning once the user logs back in,
// since Bootstrap warning toasts don't auto-hide on their own (see #8231).
function dismissNotification(tag) {
  if (!tag) return;

  for (var i = notificationQueue.length - 1; i >= 0; i--) {
    if (notificationQueue[i].tag === tag) notificationQueue.splice(i, 1);
  }

  var container = document.getElementById('toastContainer');
  if (!container) return;
  // Compare the attribute directly instead of interpolating `tag` into a CSS selector string.
  var toasts = container.querySelectorAll('.studio-toast');
  for (var j = 0; j < toasts.length; j++) {
    var el = toasts[j];
    if (el.getAttribute('data-notify-tag') !== tag) continue;
    var instance = bootstrap.Toast.getInstance(el);
    if (instance) instance.hide();
    else el.remove();
  }
}
