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
package com.arcadedb.index;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test: when querying a composite LSM index (key1, key2) using only the first key,
 * the entries must come back ordered by the remaining key components (key2), reflecting the
 * natural sort order of the LSM-Tree index. Previously {@code index.get()} accumulated the
 * partial-key matches into a {@link java.util.HashSet}, losing the ordering.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CompositeIndexPartialKeyOrderTest extends TestHelper {

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("DB");
      type.createProperty("Key1", Type.BOOLEAN);
      type.createProperty("Key2", Type.INTEGER);
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "DB", "Key1", "Key2");
    });

    // Insert Key2 values in a deliberately shuffled order so the natural index ordering differs
    // from the insertion order.
    final int[] shuffled = { 7, 3, 9, 1, 5, 8, 0, 6, 2, 4 };
    database.transaction(() -> {
      for (final int k2 : shuffled)
        database.newDocument("DB").set("Key1", true).set("Key2", k2).save();
      // Add some Key1=false rows to ensure they are not returned.
      for (int k2 = 0; k2 < 5; k2++)
        database.newDocument("DB").set("Key1", false).set("Key2", k2 * 100).save();
    });
  }

  @Test
  void partialKeyReturnsEntriesSortedBySecondKey() {
    database.transaction(() -> {
      final List<Integer> key2Values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql", "SELECT Key1, Key2 FROM DB WHERE Key1 = true")) {
        while (rs.hasNext()) {
          final Result row = rs.next();
          assertThat((Boolean) row.getProperty("Key1")).isTrue();
          key2Values.add(row.getProperty("Key2"));
        }
      }

      assertThat(key2Values).containsExactly(0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
    });
  }

  /**
   * The exact scenario reported on Discord: the composite index lives on a parent type and the
   * query targets a subtype. This produces the polymorphic plan with the EXTRACT VALUE FROM INDEX
   * ENTRY + DISTINCT + FILTER ITEMS BY TYPE steps. None of these reorder the stream, so the
   * entries must still come back sorted by the second key.
   */
  @Test
  void partialKeyOnSubTypeWithDistinctStepStaysSorted() {
    database.transaction(() -> {
      final VertexType parent = database.getSchema().createVertexType("ParentType");
      parent.createProperty("PKey1", Type.BOOLEAN);
      parent.createProperty("PKey2", Type.INTEGER);
      final VertexType child = database.getSchema().createVertexType("ChildType");
      child.addSuperType(parent);
      database.getSchema().buildTypeIndex("ParentType", new String[] { "PKey1", "PKey2" })
          .withType(Schema.INDEX_TYPE.LSM_TREE).withUnique(false).create();
    });

    // 200 rows inserted in a pseudo-shuffled PKey2 order (all distinct), spanning many buckets.
    database.transaction(() -> {
      for (int i = 0; i < 200; i++) {
        final int k2 = (i * 137) % 200;
        database.newVertex("ChildType").set("PKey1", true).set("PKey2", k2).save();
      }
    });

    database.transaction(() -> {
      // Verify the plan really is the polymorphic one with the DISTINCT step.
      try (final ResultSet explain = database.query("sql",
          "EXPLAIN SELECT PKey1, PKey2 FROM ChildType WHERE PKey1 = true LIMIT 500")) {
        final String plan = explain.next().getProperty("executionPlanAsString");
        assertThat(plan).contains("FETCH FROM INDEX");
        assertThat(plan).contains("DISTINCT");
      }

      final List<Integer> key2Values = new ArrayList<>();
      try (final ResultSet rs = database.query("sql",
          "SELECT PKey1, PKey2 FROM ChildType WHERE PKey1 = true LIMIT 500")) {
        while (rs.hasNext())
          key2Values.add(rs.next().getProperty("PKey2"));
      }

      assertThat(key2Values).hasSize(200);
      final List<Integer> expected = new ArrayList<>(key2Values);
      expected.sort(null);
      assertThat(key2Values).as("entries must be naturally sorted by the second composite key").isEqualTo(expected);
    });
  }
}
