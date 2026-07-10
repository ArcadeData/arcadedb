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
package performance;

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.engine.PageManager;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.File;

/**
 * Measures the PageManager cacheHits delta produced by a simple {@code SELECT FROM X LIMIT 20}
 * query on a small dataset. Use this to track regressions or improvements in per-record page
 * access on the SELECT hot path. Tagged {@code benchmark} so it is excluded from the regular
 * CI run; invoke explicitly with {@code mvn test -Dtest=SelectCacheHitsBenchmark
 * -Dgroups=benchmark}.
 *
 * <p>Context: a Studio user reported ~2,500 cache hits per LIMIT-20 SELECT on a small DB. This
 * baseline captures the current cost so future engine changes have something to compare against.
 */
@Tag("benchmark")
class SelectCacheHitsBenchmark {

  private static final String DB_PATH = "target/databases/select-cache-hits-bench";
  private static final int    DOC_COUNT = 200;
  private static final int    LIMIT = 20;
  private static final int    WARMUP_ITERATIONS = 3;
  private static final int    MEASURED_ITERATIONS = 5;

  @Test
  void cacheHitsPerSelectLimit20() {
    runScenario("Document/200rows", "CREATE DOCUMENT TYPE Brewery", false, false, DOC_COUNT);
    runScenario("Vertex/200rows", "CREATE VERTEX TYPE Brewery", true, false, DOC_COUNT);
    runScenario("Vertex/200rows-with-edges", "CREATE VERTEX TYPE Brewery", true, true, DOC_COUNT);
    runScenario("Vertex/21rows-with-edges", "CREATE VERTEX TYPE Brewery", true, true, 21);
  }

  private void runScenario(final String label, final String createTypeDdl, final boolean isVertex,
      final boolean withEdges, final int rowCount) {
    FileUtils.deleteRecursively(new File(DB_PATH));

    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.create()) {
        db.command("sql", createTypeDdl);
        db.command("sql", "CREATE PROPERTY Brewery.name STRING");
        db.command("sql", "CREATE PROPERTY Brewery.city STRING");
        if (withEdges) {
          db.command("sql", "CREATE VERTEX TYPE Category");
          db.command("sql", "CREATE EDGE TYPE HasCategory");
        }

        db.transaction(() -> {
          MutableVertex categoryAle = null;
          MutableVertex categoryLager = null;
          if (withEdges) {
            categoryAle = db.newVertex("Category");
            categoryAle.set("name", "Ale");
            categoryAle.save();
            categoryLager = db.newVertex("Category");
            categoryLager.set("name", "Lager");
            categoryLager.save();
          }

          for (int i = 0; i < rowCount; i++) {
            if (isVertex) {
              final MutableVertex v = db.newVertex("Brewery");
              v.set("name", "Brewery " + i);
              v.set("city", "City " + (i % 25));
              v.save();
              if (withEdges) {
                v.newEdge("HasCategory", i % 2 == 0 ? categoryAle : categoryLager).save();
              }
            } else {
              final MutableDocument doc = db.newDocument("Brewery");
              doc.set("name", "Brewery " + i);
              doc.set("city", "City " + (i % 25));
              doc.save();
            }
          }
        });
      }
    }

    // Reopen to ensure a clean page-cache state before measuring.
    try (final DatabaseFactory factory = new DatabaseFactory(DB_PATH)) {
      try (final Database db = factory.open()) {

        for (int w = 0; w < WARMUP_ITERATIONS; w++)
          drain(db.query("sql", "SELECT FROM Brewery LIMIT " + LIMIT));

        final long beforeHits = PageManager.INSTANCE.getStats().cacheHits;
        final long beforeMiss = PageManager.INSTANCE.getStats().cacheMiss;

        for (int r = 0; r < MEASURED_ITERATIONS; r++)
          drain(db.query("sql", "SELECT FROM Brewery LIMIT " + LIMIT));

        final long afterHits = PageManager.INSTANCE.getStats().cacheHits;
        final long afterMiss = PageManager.INSTANCE.getStats().cacheMiss;

        final long deltaHits = afterHits - beforeHits;
        final long deltaMiss = afterMiss - beforeMiss;
        final long hitsPerExecution = deltaHits / MEASURED_ITERATIONS;
        final long hitsPerRecord = hitsPerExecution / LIMIT;

        System.out.printf(
            """
            [SelectCacheHitsBenchmark/%s] iterations=%d limit=%d %n\
              total cacheHits delta = %d (avg %d per execution, ~%d per returned record) %n\
              total cacheMiss delta = %d %n""",
            label, MEASURED_ITERATIONS, LIMIT, deltaHits, hitsPerExecution, hitsPerRecord,
            deltaMiss);
      }
    } finally {
      FileUtils.deleteRecursively(new File(DB_PATH));
    }
  }

  private static void drain(final ResultSet rs) {
    try (rs) {
      while (rs.hasNext())
        rs.next();
    }
  }
}
