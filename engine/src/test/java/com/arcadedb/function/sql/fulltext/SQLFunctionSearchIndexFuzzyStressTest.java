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
package com.arcadedb.function.sql.fulltext;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

/**
 * Regression test for issue #5118: a Lucene fuzzy query ({@code term~}) executed through {@code SEARCH_INDEX} on a
 * {@code FULL_TEXT} index that has accumulated deletes/updates threw a server-side {@code NullPointerException}. The fuzzy path
 * (default {@code prefixLength = 0}) triggers a full scan of the underlying LSM index, and the query executor dereferenced
 * {@code cursor.next().getIdentity()} without guarding against the {@code null} that {@link com.arcadedb.index.lsm.LSMTreeIndexCursor#next()}
 * may legitimately return (a deleted/tombstoned entry it steps over while its internal iterators are still considered alive).
 * <p>
 * The test also asserts the fuzzy result matches a full-scan ground truth so the null-skip fix cannot silently drop real matches.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
@Tag("slow")
class SQLFunctionSearchIndexFuzzyStressTest extends TestHelper {

  @Test
  void fuzzyQueryAfterStressDoesNotThrowAndReturnsAllMatches() {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc");
      database.command("sql", "CREATE PROPERTY Doc.body STRING");
      database.command("sql", "CREATE PROPERTY Doc.seq INTEGER");
      database.command("sql", "CREATE INDEX ON Doc (body) FULL_TEXT");
    });

    // Stress the full-text index with repeated inserts/deletes/updates so its underlying LSM index accumulates tombstones across
    // many pages. Deletes/updates target the non-full-text 'seq' property so the surviving dataset stays deterministic (an
    // equality filter on the full-text 'body' property would match by token, not by exact value, and delete far more than one row).
    for (int i = 1; i <= 400; i++) {
      final int id = i;
      database.transaction(() -> {
        database.command("sql", "INSERT INTO Doc SET seq = " + id + ", body = 'juncos rogelio alpha " + id + "'");
        database.command("sql", "INSERT INTO Doc SET seq = " + (10_000 + id) + ", body = 'junco alfredo beta " + id + "'");

        if (id % 2 == 0)
          database.command("sql", "DELETE FROM Doc WHERE seq = " + id);

        if (id % 3 == 0)
          database.command("sql",
              "UPDATE Doc SET body = 'juncos updated gamma " + id + "' WHERE seq = " + (10_000 + id));
      });
    }

    database.transaction(() -> {
      // Ground truth computed with a plain full scan (no full-text index): the only tokens within edit distance 2 of 'juncos'
      // present in the data are 'juncos' and 'junco', both substrings of "junco", so a body containing "junco" is exactly a
      // fuzzy match.
      long expected = 0;
      final ResultSet all = database.query("sql", "SELECT body FROM Doc");
      while (all.hasNext()) {
        final Object body = all.next().getProperty("body");
        if (body != null && body.toString().contains("junco"))
          expected++;
      }
      final long groundTruth = expected;
      assertThat(groundTruth).isGreaterThan(0L);

      // The fuzzy query must complete without throwing (the reported NPE) and return every surviving match.
      assertThatCode(() -> {
        final ResultSet result = database.query("sql",
            "SELECT count(*) AS total FROM Doc WHERE search_index('Doc[body]', 'juncos~') = true");
        assertThat(result.hasNext()).isTrue();
        final Result r = result.next();
        final long total = ((Number) r.getProperty("total")).longValue();
        assertThat(total).isEqualTo(groundTruth);
      }).doesNotThrowAnyException();
    });
  }
}
