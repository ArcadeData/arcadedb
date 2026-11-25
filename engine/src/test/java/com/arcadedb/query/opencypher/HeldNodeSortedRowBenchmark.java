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

import com.arcadedb.TestHelper;
import com.arcadedb.log.LogManager;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The cost of lining the rows a pipeline breaker hands out up with the transaction's written copies (issue #12565). A sort
 * hands out each of {@value #VERTICES} rows holding one node, and the statement then reads {@value #PROPERTIES} of its
 * properties:
 * <ul>
 *   <li>by a statement that writes nothing: no row can hold a record older than the transaction's copy, so the sort hands its
 *   rows out as they are;</li>
 *   <li>by a statement that writes (a {@code CREATE} its filter never reaches), in a transaction that has written nothing: the
 *   lookup stops at the transaction's size check;</li>
 *   <li>by the same statement in a transaction that has written an unrelated record: one hash lookup per row.</li>
 * </ul>
 * A property read itself never looks the copy up. Reported per row.
 * Logged at SEVERE, the level the engine's test logging lets through.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
@Tag("benchmark")
class HeldNodeSortedRowBenchmark extends TestHelper {
  private static final int    VERTICES    = Integer.parseInt(System.getProperty("heldread.vertices", "200000"));
  private static final int    PROPERTIES  = 5;
  private static final int    WARMUP      = 5;
  private static final int    REPEATS     = 15;
  private static final String READ_ONLY   = "MATCH (n:B) WITH n ORDER BY n.a RETURN sum(n.a + n.b + n.c + n.d + n.e) AS s";
  private static final String WRITE_SHAPE = "MATCH (n:B) WITH n ORDER BY n.a WITH sum(n.a + n.b + n.c + n.d + n.e) AS s WHERE s < 0 CREATE (:Other) RETURN s";

  @Override
  protected void beginTest() {
    database.transaction(() -> database.getSchema().createVertexType("B"));
    database.transaction(() -> {
      for (int i = 0; i < VERTICES; i++)
        database.newVertex("B").set("a", 1, "b", 1, "c", 1, "d", 1, "e", 1).save();
    });
    database.transaction(() -> database.getSchema().createVertexType("Other"));
  }

  @Test
  void sortedRowCostWithAndWithoutWrittenRecords() {
    final double[] readOnly = new double[1];
    final double[] writeNothingWritten = new double[1];
    final double[] writeSomethingWritten = new double[1];

    database.transaction(() -> {
      readOnly[0] = measure(READ_ONLY, true);
      writeNothingWritten[0] = measure(WRITE_SHAPE, false);
    });
    database.transaction(() -> {
      database.newVertex("Other").set("x", 1).save();
      writeSomethingWritten[0] = measure(WRITE_SHAPE, false);
    });

    LogManager.instance().log(this, Level.SEVERE,
        "%d sorted rows: %.2f ns/row by a read-only statement, %.2f ns/row by a write statement in a transaction that wrote nothing, %.2f ns/row by a write statement in a transaction that wrote a record",
        VERTICES, readOnly[0], writeNothingWritten[0], writeSomethingWritten[0]);
  }

  /** @return the best time of a row, in nanoseconds, over the repeats */
  private double measure(final String query, final boolean answersTheSum) {
    for (int i = 0; i < WARMUP; i++)
      run(query, answersTheSum);
    long best = Long.MAX_VALUE;
    for (int i = 0; i < REPEATS; i++) {
      final long begin = System.nanoTime();
      run(query, answersTheSum);
      best = Math.min(best, System.nanoTime() - begin);
    }
    return best / (double) VERTICES;
  }

  private void run(final String query, final boolean answersTheSum) {
    try (final ResultSet rs = database.command("opencypher", query)) {
      if (answersTheSum)
        assertThat(rs.next().<Number>getProperty("s").longValue()).isEqualTo((long) VERTICES * PROPERTIES);
      else
        assertThat(rs.hasNext()).isFalse();
    }
  }
}
