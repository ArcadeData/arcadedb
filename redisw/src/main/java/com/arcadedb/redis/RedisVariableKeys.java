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
package com.arcadedb.redis;

/**
 * The one place a Redis key becomes the name of a database global variable (issue #10694).
 * <p>
 * The database-backed Redis key space is the database's global-variable map, so that a plain key {@code k} is the SQL
 * variable {@code $k}. Every {@code DatabaseInternal} accessor of that map strips ONE leading {@code $}, because for SQL
 * {@code $name} and {@code name} spell the same variable. A Redis key is opaque client data, though, and handing it over
 * as is aliased {@code $k} onto {@code k} and {@code $} onto the empty key. Prefixing a key that starts with {@code $}
 * with one more {@code $} makes that strip remove exactly what was added, so the map is keyed by the client's key
 * verbatim. A key that does not start with {@code $} - nearly all of them - is returned unchanged and allocates nothing.
 * <p>
 * Every call that passes a Redis key to {@code getGlobalVariable}, {@code setGlobalVariable*} or
 * {@code computeGlobalVariable} goes through {@link #toVariableName}: the RESP wire path and the {@code redis} query
 * language alike, so the two surfaces name a key by the same string.
 */
public final class RedisVariableKeys {
  private RedisVariableKeys() {
  }

  public static String toVariableName(final String key) {
    return key.startsWith("$") ? "$" + key : key;
  }
}
