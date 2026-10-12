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

var studioCurrentTab = "query";

$(function () {
  $("#loginForm").on("keypress", function (e) {
    if (e.which == 13) {
      login();
    }
  });

  $('a[data-bs-toggle="tab"]').on("shown.bs.tab", function (e) {
    var activeTab = this.id;

    // Tabs that don't require a database - always show main content
    var noDatabaseTabs = ["tab-server-sel", "tab-profiler-sel", "tab-security-sel",
      "tab-cluster-sel", "tab-api-sel", "tab-resources-sel", "tab-support-sel", "tab-settings-sel"];
    if (noDatabaseTabs.indexOf(activeTab) >= 0) {
      $("#quickStartPanel").hide();
      $("#mainTabContent").show();
    }

    // Tabs that require a database - show quick start if none exists
    var databaseTabs = ["tab-query-sel", "tab-database-sel"];
    if (databaseTabs.indexOf(activeTab) >= 0 && !getCurrentDatabase()) {
      $("#mainTabContent").hide();
      showQuickStart();
    }

    if (activeTab == "tab-query-sel") {
      studioCurrentTab = "query";
      editorFocus();
    } else if (activeTab == "tab-server-sel") {
      studioCurrentTab = "server";
      updateServer();
    } else if (activeTab == "tab-profiler-sel") {
      studioCurrentTab = "profiler";
      initProfiler();
    } else if (activeTab == "tab-security-sel") {
      studioCurrentTab = "security";
      initSecurity();
    } else if (activeTab == "tab-cluster-sel") {
      studioCurrentTab = "cluster";
      updateCluster();
    } else if (activeTab == "tab-database-sel") {
      studioCurrentTab = "database";
    } else if (activeTab == "tab-api-sel") {
      studioCurrentTab = "api";
    } else if (activeTab == "tab-resources-sel") {
      studioCurrentTab = "resources";
    } else if (activeTab == "tab-support-sel") {
      studioCurrentTab = "support";
      initSupportTab();
    } else if (activeTab == "tab-settings-sel") {
      studioCurrentTab = "settings";
    }
  });

  // Initialize searchable database selectors
  initSearchableDbSelect("queryDbSelectContainer");
  initSearchableDbSelect("schemaDbSelectContainer");

  // Check for existing session in localStorage
  if (initSessionFromStorage()) {
    console.log("Found stored session, attempting auto-login");
    updateDatabases(function () {
      console.log("Auto-login successful, initializing query editor");
      initQuery();
      studioRouteReady();
    });
  } else if (globalCredentials == null) {
    showLoginPopup();
  }
});
