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
package com.arcadedb.engine.timeseries;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.LocalTimeSeriesType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #12807: a TIMESERIES sample carrying a tag value the type had never seen could not be written inside a script's
 * {@code BEGIN ... COMMIT}. The append commits a transaction of its own (#7410), and interning the new value used to commit one
 * more, nested inside the append's: with the transaction the HTTP command handler opens and the script's {@code BEGIN}, that was a
 * fourth level, past the per-thread limit of 3. A known tag value, a type without tags and the same INSERT without {@code BEGIN}
 * all worked, so whether a script worked depended on whether an earlier write had introduced its tag values.
 * <p>
 * Each script runs inside a transaction of the caller's, as the HTTP command handler runs it.
 */
class Issue12807NewTagValueInScriptTransactionTest extends TestHelper {

  private void createType() {
    database.command("sql", "CREATE TIMESERIES TYPE X TIMESTAMP ts TAGS (h STRING) FIELDS (v DOUBLE) SHARDS 1").close();
  }

  private void runScriptInsideACallerTransaction(final String script) {
    database.transaction(() -> database.command("sqlscript", script).close());
  }

  private List<String> tagsInTimestampOrder(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT h FROM " + type + " ORDER BY ts")) {
      return rs.stream().map(r -> r.<String>getProperty("h")).toList();
    }
  }

  @Test
  void aNewTagValueIsWrittenInsideAScriptBeginCommit() {
    createType();
    database.command("sql", "INSERT INTO X SET ts = 1, h = 'a', v = 1.0").close();

    runScriptInsideACallerTransaction("BEGIN; INSERT INTO X SET ts = 3, h = 'b', v = 1.0; COMMIT; RETURN 1;");

    assertThat(tagsInTimestampOrder("X")).containsExactly("a", "b");

    // The dictionary entry the sample references was committed, not only published in RAM
    reopenDatabase();
    assertThat(tagsInTimestampOrder("X")).containsExactly("a", "b");
  }

  @Test
  void aNewTagValueInAContentInsertIsWrittenInsideAScriptBeginCommit() {
    createType();
    database.command("sql", "INSERT INTO X SET ts = 1, h = 'a', v = 1.0").close();

    runScriptInsideACallerTransaction("BEGIN; INSERT INTO X CONTENT [{\"ts\": 4, \"h\": \"c\", \"v\": 1.0}]; COMMIT; RETURN 1;");

    assertThat(tagsInTimestampOrder("X")).containsExactly("a", "c");
  }

  /** An empty type has no tag value at all, so its first sample in a script used to fail every time. */
  @Test
  void theFirstSamplesOfAnEmptyTypeAreWrittenInsideAScriptBeginCommit() {
    createType();

    for (int i = 0; i < 5; i++)
      runScriptInsideACallerTransaction("BEGIN; INSERT INTO X SET ts = " + i + ", h = 'h" + i + "', v = 1.0; COMMIT; RETURN 1;");

    assertThat(tagsInTimestampOrder("X")).containsExactly("h0", "h1", "h2", "h3", "h4");
  }

  /**
   * The engine path, without SQL: an append is one level of nesting whatever its tag values, so a caller one level below the
   * limit can write a value never seen before.
   */
  @Test
  void anAppendWithANewTagValueNeedsOneLevelOfNestingLikeAnyOtherAppend() throws Exception {
    createType();
    final TimeSeriesEngine engine = ((LocalTimeSeriesType) database.getSchema().getType("X")).getEngine();
    final DatabaseContext.DatabaseContextTL context = DatabaseContext.INSTANCE.getContext(database.getDatabasePath());

    final int callerLevels = context.getMaxNested() - 1;
    database.begin();
    while (context.getTransactionDepth() < callerLevels)
      database.begin();
    try {
      assertThat(context.getTransactionDepth()).isEqualTo(callerLevels);
      engine.appendSamples(new long[] { 1L, 2L }, new Object[] { "new-1", "new-2" }, new Object[] { 1.0, 2.0 });
      assertThat(context.getTransactionDepth()).isEqualTo(callerLevels);
    } finally {
      while (database.isTransactionActive())
        database.rollback();
    }

    // Rolling the caller back does not take the samples nor their tag values (#7410): the append committed both on its own
    assertThat(tagsInTimestampOrder("X")).containsExactly("new-1", "new-2");
  }

  @Test
  void aKnownTagValueAndATypeWithoutTagsAreWrittenInsideAScriptBeginCommit() {
    createType();
    database.command("sql", "CREATE TIMESERIES TYPE NT TIMESTAMP ts FIELDS (v DOUBLE) SHARDS 1").close();
    database.command("sql", "INSERT INTO X SET ts = 1, h = 'a', v = 1.0").close();

    runScriptInsideACallerTransaction("BEGIN; INSERT INTO X SET ts = 2, h = 'a', v = 1.0; COMMIT; RETURN 1;");
    runScriptInsideACallerTransaction("BEGIN; INSERT INTO NT SET ts = 3, v = 1.0; COMMIT; RETURN 1;");

    assertThat(tagsInTimestampOrder("X")).containsExactly("a", "a");
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM NT")) {
      assertThat(rs.next().<Long>getProperty("c")).isEqualTo(1L);
    }
  }
}
