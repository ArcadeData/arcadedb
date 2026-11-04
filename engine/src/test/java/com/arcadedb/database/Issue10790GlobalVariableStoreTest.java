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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.SQLQueryEngine;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #10790: the global-variable store holds a key as given, for a key space that is not SQL's (the Redis wrapper), while the
 * SQL accessors keep their rules - one leading {@code $} stripped, the reserved names refused - and a name SQL reserves stays the
 * context's: an entry the store holds under it never answers a SQL {@code $parent}, {@code $depth}...
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10790GlobalVariableStoreTest extends TestHelper {

  @Test
  void theStoreHoldsEveryKeyAsGiven() {
    final ConcurrentMap<String, Object> store = ((DatabaseInternal) database).getGlobalVariableStore();
    try {
      for (final String name : SQLQueryEngine.RESERVED_VARIABLE_NAMES) {
        store.put(name, "plain-" + name);
        store.put("$" + name, "dollar-" + name);
        assertThat(store.get(name)).isEqualTo("plain-" + name);
        assertThat(store.get("$" + name)).isEqualTo("dollar-" + name);
      }
      store.put("k10790", "plain");
      store.put("$k10790", "dollar");
      assertThat(store.get("k10790")).as("$k and k are two keys of the store").isEqualTo("plain");
      assertThat(store.get("$k10790")).isEqualTo("dollar");

      // the SQL accessors keep their rules
      assertThat(((DatabaseInternal) database).getGlobalVariable("$k10790")).as("SQL strips one $").isEqualTo("plain");
      assertThatThrownBy(() -> ((DatabaseInternal) database).setGlobalVariable("parent", "x")).hasMessageContaining("reserved");
    } finally {
      store.clear();
    }
  }

  @Test
  void aReservedNameInTheStoreNeverAnswersForTheSqlContextVariable() {
    final ConcurrentMap<String, Object> store = ((DatabaseInternal) database).getGlobalVariableStore();
    try {
      for (final String name : SQLQueryEngine.RESERVED_VARIABLE_NAMES)
        store.put(name, "leaked-" + name);
      store.put("visible10790", "shared");

      for (final String name : SQLQueryEngine.RESERVED_VARIABLE_NAMES)
        assertThat(((DatabaseInternal) database).getGlobalVariable(name)).as("$" + name + " is the context's").isNull();

      try (final ResultSet rs = database.query("sql", "SELECT $parent AS parent, $depth AS depth, $visible10790 AS visible")) {
        final var row = rs.next();
        assertThat(row.<Object>getProperty("parent")).isNull();
        assertThat(row.<Object>getProperty("depth")).isNull();
        assertThat(row.<Object>getProperty("visible")).as("a name SQL can use still reads the store").isEqualTo("shared");
      }
    } finally {
      store.clear();
    }
  }
}
