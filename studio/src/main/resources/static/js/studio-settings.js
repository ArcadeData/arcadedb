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

function applyTheme(pref) {
  globalStorageSave('studio-theme', pref);

  var resolved = pref;
  if (pref === 'system')
    resolved = window.matchMedia('(prefers-color-scheme:dark)').matches ? 'dark' : 'light';

  document.documentElement.setAttribute('data-theme', resolved);
  updateThemeButtons(pref);
}

function updateThemeButtons(pref) {
  var group = document.getElementById('themeOptionGroup');
  if (!group) return;
  var btns = group.querySelectorAll('.theme-option');
  for (var i = 0; i < btns.length; i++) {
    if (btns[i].getAttribute('data-theme-value') === pref)
      btns[i].classList.add('active');
    else
      btns[i].classList.remove('active');
  }
}

// Initialize theme buttons on load
document.addEventListener('DOMContentLoaded', function() {
  var saved = localStorage.getItem('studio-theme') || 'light';
  updateThemeButtons(saved);

  // Listen for system theme changes
  window.matchMedia('(prefers-color-scheme:dark)').addEventListener('change', function() {
    var current = localStorage.getItem('studio-theme') || 'light';
    if (current === 'system')
      applyTheme('system');
  });
});
