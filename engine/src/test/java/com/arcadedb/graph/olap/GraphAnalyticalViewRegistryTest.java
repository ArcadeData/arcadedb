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
package com.arcadedb.graph.olap;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #4586: the {@link GraphAnalyticalViewRegistry} must not accumulate
 * entries for closed databases. Database lifetime is governed by explicit cleanup (the registry
 * entry is removed on {@code Database.close()} via {@code shutdownAll}), so a sequence of
 * open/register/close cycles must leave the static registry at its baseline size.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class GraphAnalyticalViewRegistryTest {
  private static final String DB_PATH = "./target/databases/gav-registry-test";

  @BeforeEach
  @AfterEach
  void cleanup() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    if (factory.exists())
      factory.open().drop();
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @SuppressWarnings("unchecked")
  private static int registrySize() throws Exception {
    final Field f = GraphAnalyticalViewRegistry.class.getDeclaredField("REGISTRY");
    f.setAccessible(true);
    final Map<Database, ?> map = (Map<Database, ?>) f.get(null);
    synchronized (map) {
      return map.size();
    }
  }

  @Test
  void closeRemovesRegistryEntryNoAccumulation() throws Exception {
    final int baseline = registrySize();

    // Open the database, register a named GAV, then close. Repeat a few times: a defeated
    // WeakHashMap (value strongly references its key) would still be cleaned up here because
    // close() removes the entry explicitly, so the registry must return to its baseline size.
    // On reopen the named GAV is auto-loaded from schema persistence, so only build it once.
    for (int i = 0; i < 3; i++) {
      final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
      try (final Database db = factory.exists() ? factory.open() : factory.create()) {
        db.getSchema().getOrCreateVertexType("Person");
        db.getSchema().getOrCreateEdgeType("FOLLOWS");

        if (GraphAnalyticalViewRegistry.get(db, "social-graph") == null)
          GraphAnalyticalView.builder(db)
              .withName("social-graph")
              .withVertexTypes("Person")
              .withEdgeTypes("FOLLOWS")
              .build();

        assertThat(GraphAnalyticalViewRegistry.get(db, "social-graph")).isNotNull();
        assertThat(registrySize()).isEqualTo(baseline + 1);
      }

      // After close(), shutdownAll() must have removed the database's entry: no accumulation.
      assertThat(registrySize()).isEqualTo(baseline);
    }
  }

  @Test
  void dropRemovesRegistryEntry() throws Exception {
    final int baseline = registrySize();

    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    final Database db = factory.create();
    db.getSchema().getOrCreateVertexType("Person");
    GraphAnalyticalView.builder(db).withName("v").withVertexTypes("Person").build();
    assertThat(registrySize()).isEqualTo(baseline + 1);

    db.drop();

    assertThat(registrySize()).isEqualTo(baseline);
  }
}
