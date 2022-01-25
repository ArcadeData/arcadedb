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
package com.arcadedb.security;

import java.util.Set;

/**
 * Security user at global level. The user can have access to multiple databases.
 */
public interface SecurityUser {
  /**
   * Returns the user's name.
   */
  String getName();

  /**
   * Returns the user's encoded password.
   */
  String getPassword();

  /**
   * Returns the set of databases the user has access to.
   */
  Set<String> getAuthorizedDatabases();

  /**
   * Add a database to the user's authorized list. Groups is a list of group names the user belong to for the specific database or can be empty ot null
   * (default group will be taken).
   */
  SecurityUser addDatabase(String databaseName, String[] groups);
}
