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
package com.arcadedb.server.security;

import com.arcadedb.serializer.json.JSONObject;

/**
 * Opens the package-visible corners of {@link ServerSecurity} that test fixtures need, from inside the package
 * that owns them. Tests in other modules (which reach this class through the server test-jar) can then declare
 * their own package instead of re-declaring {@code com.arcadedb.server.security} just to reach a protected
 * method - a package split across two artifacts that only holds together while both jars share a class loader.
 */
public class ServerSecurityTestAccess {

  private ServerSecurityTestAccess() {
  }

  /**
   * Returns the mutable group definitions a fixture plants test groups into, exactly as
   * {@code ServerSecurity.getDatabaseGroupsConfiguration} does: the live wildcard ({@code "*"}) groups object
   * when the database has no entry of its own, so a group put here applies to that database.
   */
  public static JSONObject databaseGroups(final ServerSecurity security, final String databaseName) {
    return security.getDatabaseGroupsConfiguration(databaseName);
  }
}
