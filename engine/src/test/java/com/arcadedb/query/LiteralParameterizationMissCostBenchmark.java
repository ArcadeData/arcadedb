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
package com.arcadedb.query;

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #8307: the worst case of literal parameterization is a workload whose every text has a shape never seen before, so the
 * lexing and the classification are paid and nothing is ever shared. Measures that miss path with the feature on against off,
 * best of several rounds, for SQL and OpenCypher.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("benchmark")
class LiteralParameterizationMissCostBenchmark extends TestHelper {
  private static final int QUERIES = 3_000;
  private static final int ROUNDS  = 5;

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE Person");
    database.command("sql", "CREATE PROPERTY Person.id INTEGER");
    database.command("sql", "CREATE INDEX ON Person (id) UNIQUE");
    database.transaction(() -> {
      for (int i = 0; i < 1_000; i++)
        database.newVertex("Person").set("id", i).set("name", "p" + i).save();
    });
  }

  @Test
  void aShapeSeenOnceCostsAboutWhatItCostWithoutParameterization() {
    // every text gets its own shape through its alias, so no statement is ever shared
    final double sqlRatio = ratio("sql", "SELECT name AS a%d FROM Person WHERE id = %d AND name <> 'x%d'");
    final double cypherRatio = ratio("opencypher", "MATCH (p:Person) WHERE p.id = %2$d AND p.name <> 'x%3$d' RETURN p.name AS a%1$d");

    assertThat(sqlRatio).as("SQL unique-shape cost, on / off").isLessThan(1.3);
    assertThat(cypherRatio).as("OpenCypher unique-shape cost, on / off").isLessThan(1.3);
  }

  private double ratio(final String language, final String format) {
    long bestOn = Long.MAX_VALUE;
    long bestOff = Long.MAX_VALUE;
    int salt = 0;
    for (int round = 0; round < ROUNDS; round++) {
      bestOff = Math.min(bestOff, run(language, format, false, salt));
      salt += QUERIES;
      bestOn = Math.min(bestOn, run(language, format, true, salt));
      salt += QUERIES;
    }
    return (double) bestOn / bestOff;
  }

  private long run(final String language, final String format, final boolean parameterize, final int salt) {
    database.getConfiguration().setValue(GlobalConfiguration.QUERY_LITERAL_PARAMETERIZATION, parameterize);
    final long start = System.nanoTime();
    for (int i = 0; i < QUERIES; i++) {
      final int n = salt + i;
      try (final ResultSet rs = database.query(language, String.format(format, n, n % 1_000, n))) {
        rs.next();
      }
    }
    return System.nanoTime() - start;
  }
}
