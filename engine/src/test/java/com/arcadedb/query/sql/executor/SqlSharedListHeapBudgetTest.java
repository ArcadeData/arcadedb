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
package com.arcadedb.query.sql.executor;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11812, on the SQL side: a list a LET computes once and every row projects is held once, but the ORDER BY over those rows
 * charged it to the query heap budget once per row, the sort of a few hundred rows asking for far more than a 64 MB budget although
 * it holds the list once. The sequential sort and the top-K heap, which gives rows back as better ones arrive, both charge it once.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class SqlSharedListHeapBudgetTest extends TestHelper {

  private static final int ROWS    = 300;
  private static final int LIST_SZ = 2_000;

  @BeforeEach
  void fill() {
    QueryHeapBudgetTestSupport.awaitNoReservationLeft();
    database.getSchema().createDocumentType("W");
    database.getSchema().createVertexType("V");
    database.transaction(() -> {
      for (int i = 0; i < ROWS; i++)
        database.newDocument("W").set("i", i).save();
      for (int i = 0; i < LIST_SZ; i++) {
        final float[] vector = new float[256];
        vector[i % 256] = i;
        database.newVertex("V").set("i", i, "v", vector).save();
      }
    });
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(64);
  }

  @AfterEach
  void resetBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void aSortChargesAListEveryRowSharesOnce() {
    final List<Integer> ordered = read("SELECT i, $ns AS ns FROM W LET $ns = (SELECT FROM V) ORDER BY i DESC");

    assertThat(ordered).hasSize(ROWS);
    assertThat(ordered.getFirst()).isEqualTo(ROWS - 1);
    assertThat(ordered.getLast()).isEqualTo(0);
  }

  @Test
  void aTopKHeapChargesAListEveryRowSharesOnce() {
    final List<Integer> ordered = read("SELECT i, $ns AS ns FROM W LET $ns = (SELECT FROM V) ORDER BY i DESC LIMIT 100");

    assertThat(ordered).hasSize(100);
    assertThat(ordered.getFirst()).isEqualTo(ROWS - 1);
    assertThat(ordered.getLast()).isEqualTo(ROWS - 100);
  }

  private List<Integer> read(final String query) {
    final List<Integer> ordered = new ArrayList<>();
    try (final ResultSet rows = database.query("sql", query)) {
      while (rows.hasNext()) {
        final Result row = rows.next();
        assertThat(row.<Collection<?>>getProperty("ns")).hasSize(LIST_SZ);
        ordered.add(row.<Number>getProperty("i").intValue());
      }
    }
    return ordered;
  }
}
