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
package com.arcadedb.index.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #5791 (follow-up to #4732): reproduces the reporter's exact shape - a single-bucket
 * vertex type whose rows were repeatedly inserted and updated over time (an SCD2-style churn pattern), rather than
 * seeded in one batch, before {@code CREATE INDEX ... FULL_TEXT} runs over the already-populated type. The reporter
 * could not reproduce the defect with freshly-seeded data at the same or larger volumes; the distinguishing factor
 * was age/churn, which fragments the bucket with placeholder-relocated records (updates that outgrew their page slot)
 * and tombstoned slots (deletes), unlike a clean bulk insert.
 * <p>
 * Also exercises the {@code REBUILD INDEX} repair path and a prefix query on the same churned index, the other two
 * symptoms reported in #5791.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class FullTextBuildOverChurnedDataTest extends TestHelper {

  private static final int    ROW_COUNT    = 6800; // crosses the 5,000 build-batch-commit boundary, like the report's 6769
  private static final String NEEDLE       = "lorevoritmarker";
  private static final String NEEDLE_CYR   = "релизмаркер";

  @Test
  void fullTextIndexBuiltOverChurnedBucketMatchesEveryRow() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE TaskHist");
      database.command("sql", "CREATE PROPERTY TaskHist.note_md STRING");
      database.command("sql", "CREATE PROPERTY TaskHist.seq INTEGER");
    });

    churnData();

    // Build the FULL_TEXT index AFTER the churn, exactly like the report: an explicit name, over a type that
    // already has thousands of aged/updated rows and no index yet.
    database.transaction(() -> {
      final ResultSet created = database.command("sql", "CREATE INDEX ftTaskHist ON TaskHist (note_md) FULL_TEXT");
      final Result createdResult = created.next();
      final long totalIndexed = ((Number) createdResult.getProperty("totalIndexed")).longValue();
      assertThat(totalIndexed).isEqualTo(countLiveRows());
    });

    database.transaction(() -> {
      final long scanLore = countLike("%" + NEEDLE + "%");
      final long scanCyr = countLike("%" + NEEDLE_CYR + "%");
      assertThat(scanLore).isGreaterThan(0L);
      assertThat(scanCyr).isGreaterThan(0L);

      final long searchLore = countSearchIndex(NEEDLE);
      final long searchCyr = countSearchIndex(NEEDLE_CYR);

      // The defect reported in #5791: the index reports every row as indexed but only rows written AFTER index
      // creation are actually matchable. Every pre-existing row carrying the marker must be found.
      assertThat(searchLore).as("SEARCH_INDEX must match every pre-existing row containing '%s'", NEEDLE).isEqualTo(scanLore);
      assertThat(searchCyr).as("SEARCH_INDEX must match every pre-existing row containing '%s'", NEEDLE_CYR).isEqualTo(scanCyr);
    });

    // A row inserted AFTER the index exists must also match (sanity: incremental maintenance still works).
    database.transaction(() -> {
      database.command("sql", "INSERT INTO TaskHist SET seq = -1, note_md = 'post-create row with " + NEEDLE + "'");
    });
    database.transaction(() -> assertThat(countSearchIndex(NEEDLE)).isEqualTo(countLike("%" + NEEDLE + "%")));

    // The obvious repair must not lose the explicitly-given index name (#5791's second symptom) and must keep
    // every row matchable.
    database.transaction(() -> database.command("sql", "REBUILD INDEX `ftTaskHist`"));
    database.transaction(() -> {
      assertThat(database.getSchema().existsIndex("ftTaskHist")).isTrue();
      assertThat(countSearchIndex(NEEDLE)).isEqualTo(countLike("%" + NEEDLE + "%"));
    });

    // Prefix queries on a churned index must not throw (#5791's third symptom).
    database.transaction(() -> {
      final ResultSet result = database.query("sql",
          "SELECT count(*) AS total FROM TaskHist WHERE SEARCH_INDEX('ftTaskHist', 'zzzzznomatch*') = true");
      assertThat(result.hasNext()).isTrue();
      assertThat(((Number) result.next().getProperty("total")).longValue()).isEqualTo(0L);
    });
  }

  /**
   * Simulates months of SCD2-style churn on a single bucket: staggered inserts, several rounds of growing updates
   * (to force some records past their page slot into a placeholder relocation), and interleaved deletes - rather
   * than one clean bulk insert.
   */
  private void churnData() {
    final Random random = new Random(42);

    // Initial load, short bodies.
    for (int batch = 0; batch < ROW_COUNT / 200; batch++) {
      final int base = batch * 200;
      database.transaction(() -> {
        for (int i = 0; i < 200; i++) {
          final int seq = base + i;
          database.command("sql", "INSERT INTO TaskHist SET seq = ?, note_md = ?", seq, shortBody(seq, random));
        }
      });
    }

    // Several rounds of growing UPDATEs on a subset of rows: each round makes the body longer, which forces
    // some records to outgrow their current page slot and become placeholder-relocated (issue #5791's "age").
    for (int round = 1; round <= 4; round++) {
      final int finalRound = round;
      for (int batch = 0; batch < ROW_COUNT / 200; batch++) {
        final int base = batch * 200;
        database.transaction(() -> {
          for (int i = 0; i < 200; i += 3) { // touch about a third of the rows each round
            final int seq = base + i;
            database.command("sql", "UPDATE TaskHist SET note_md = ? WHERE seq = ?", longerBody(seq, finalRound, random), seq);
          }
        });
      }
    }

    // A little delete/reinsert churn, like an SCD2 correction.
    for (int batch = 0; batch < ROW_COUNT / 200; batch += 5) {
      final int base = batch * 200;
      database.transaction(() -> {
        database.command("sql", "DELETE FROM TaskHist WHERE seq = ?", base + 5);
        database.command("sql", "INSERT INTO TaskHist SET seq = ?, note_md = ?", base + 5, shortBody(base + 5, random));
      });
    }
  }

  private static String shortBody(final int seq, final Random random) {
    final StringBuilder sb = new StringBuilder();
    sb.append("record ").append(seq).append(" ");
    if (seq % 7 == 0)
      sb.append(NEEDLE).append(" ");
    if (seq % 11 == 0)
      sb.append(NEEDLE_CYR).append(" ");
    for (int i = 0; i < 20; i++)
      sb.append("word").append(random.nextInt(500)).append(' ');
    return sb.toString();
  }

  private static String longerBody(final int seq, final int round, final Random random) {
    final StringBuilder sb = new StringBuilder();
    sb.append("record ").append(seq).append(" round ").append(round).append(" ");
    if (seq % 7 == 0)
      sb.append(NEEDLE).append(" ");
    if (seq % 11 == 0)
      sb.append(NEEDLE_CYR).append(" ");
    // Roughly matches the report's ~800 char average, growing with each round.
    for (int i = 0; i < 40 + round * 15; i++)
      sb.append("longword").append(random.nextInt(2000)).append(' ');
    return sb.toString();
  }

  private long countLiveRows() {
    final ResultSet result = database.query("sql", "SELECT count() AS total FROM TaskHist");
    return ((Number) result.next().getProperty("total")).longValue();
  }

  private long countLike(final String pattern) {
    final ResultSet result = database.query("sql", "SELECT count() AS total FROM TaskHist WHERE note_md LIKE ?", pattern);
    return ((Number) result.next().getProperty("total")).longValue();
  }

  private long countSearchIndex(final String term) {
    final ResultSet result = database.query("sql",
        "SELECT count(*) AS total FROM TaskHist WHERE SEARCH_INDEX('ftTaskHist', ?) = true", term);
    return ((Number) result.next().getProperty("total")).longValue();
  }
}
