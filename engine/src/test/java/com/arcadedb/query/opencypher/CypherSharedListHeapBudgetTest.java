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
package com.arcadedb.query.opencypher;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #11812: a list collected once and carried by every row (WITH collect(n) AS ns UNWIND ...) is held once, but an ORDER BY over
 * those rows charged it to the query heap budget once per row: 300 rows of a list of 2,000 records asked for far more than
 * a 64 MB budget although the sort holds a few kilobytes of its own.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherSharedListHeapBudgetTest extends TestHelper {

  @AfterEach
  void resetBudget() {
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.reset();
  }

  @Test
  void anOrderByChargesAListEveryRowSharesOnce() {
    database.getSchema().createVertexType("V");
    database.transaction(() -> {
      for (int i = 0; i < 2_000; i++) {
        final float[] vector = new float[256];
        vector[i % 256] = i;
        database.newVertex("V").set("i", i, "v", vector).save();
      }
    });
    GlobalConfiguration.QUERY_MAX_HEAP_RAM.setValue(64);

    final List<Long> ordered = new ArrayList<>();
    try (final ResultSet rows = database.query("opencypher",
        "MATCH (n:V) WITH collect(n) AS ns UNWIND range(1, 300) AS i RETURN i, size(ns) AS n ORDER BY i DESC")) {
      while (rows.hasNext()) {
        final Result row = rows.next();
        assertThat(row.<Number>getProperty("n").intValue()).isEqualTo(2_000);
        ordered.add(row.<Number>getProperty("i").longValue());
      }
    }

    assertThat(ordered).hasSize(300);
    assertThat(ordered.getFirst()).isEqualTo(300L);
    assertThat(ordered.getLast()).isEqualTo(1L);
  }
}
