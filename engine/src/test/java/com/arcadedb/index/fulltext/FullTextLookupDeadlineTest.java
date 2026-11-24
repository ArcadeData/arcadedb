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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.index.IndexCursor;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.FetchFromIndexStep;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import com.arcadedb.query.sql.parser.SelectStatement;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * A {@code CONTAINSTEXT} answered by a FULL_TEXT index reads every posting of every query word before its first row, so the
 * deadline of the statement has to be checked inside that read: a check per emitted row comes after the whole lookup.
 * <p>
 * Each test pins an already expired deadline on the context of the statement rather than racing a real clock, so the outcome does
 * not depend on the speed of the machine.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class FullTextLookupDeadlineTest extends TestHelper {
  /** Well past the 1024 postings between two checks of a guard. */
  private static final int DOCUMENTS = 3_000;

  @ParameterizedTest
  @CsvSource({ "BM25,1,1", "BM25,4,1", "CLASSIC,1,1", "CLASSIC,4,1", "BM25,1,2", "BM25,4,2", "CLASSIC,1,2", "CLASSIC,4,2" })
  void containsTextStopsInsideTheLookupAtTheDeadline(final String similarity, final int buckets, final int properties) {
    createCorpus(similarity, buckets, properties);
    final String sql = properties == 1 ?
        "SELECT FROM Doc WHERE content CONTAINSTEXT 'common'" :
        "SELECT FROM Doc WHERE title CONTAINSTEXT 'common' AND content CONTAINSTEXT 'common'";
    final String variant = similarity + " buckets=" + buckets + " properties=" + properties;

    // Without a bound the index answers every document, and it is the index that answers
    try (final ResultSet rs = database.query("sql", sql)) {
      assertThat(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2)).as(variant).contains("FETCH FROM INDEX");
      int rows = 0;
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
      assertThat(rows).as(variant).isEqualTo(DOCUMENTS);
    }

    // The statement fails once its deadline has passed
    assertThat(catchThrowable(() -> drain(sql, expiredContext()))).as(variant).isInstanceOf(TimeoutException.class);

    // The index step of the plan, pulled with the deadline of its context already past. The checks of the steps that consume the
    // entries also fail the statement, but they run after the lookup read everything: the failure has to come from the lookup
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
    final FetchFromIndexStep step = indexStepOf(statement.createExecutionPlanNoCache(context).getSteps());
    assertThat(step).as(variant).isNotNull();
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");

    final Throwable failure = catchThrowable(() -> {
      try (final ResultSet rs = step.syncPull(context, DOCUMENTS)) {
        while (rs.hasNext())
          rs.next();
      }
    });
    assertThat(failure).as(variant).isInstanceOf(TimeoutException.class);
    assertThat(raisedByTheIndexLookup(failure)).as(variant + " must stop inside the lookup, not after it: " + failure).isTrue();
  }

  /**
   * The lookup of a classic index reads the postings of every word of the query. A word the query repeats is one lookup that
   * counts as many times, so what a repeated word contributes to the score is what it always was.
   */
  @Test
  void aRepeatedWordKeepsTheScoreItAlwaysGave() {
    createCorpus("CLASSIC", 4, 1);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");

    final Map<String, Integer> once = scoresOf(index, "common");
    final Map<String, Integer> thrice = scoresOf(index, "common common common");
    final Map<String, Integer> mixed = scoresOf(index, "word7 common word7 common word7");

    assertThat(once).hasSize(DOCUMENTS);
    assertThat(thrice).hasSize(DOCUMENTS);
    for (final Map.Entry<String, Integer> entry : once.entrySet())
      assertThat(thrice.get(entry.getKey())).isEqualTo(entry.getValue() * 3);

    // 'common' is in every document and counted twice, 'word7' is in one and counted three times
    assertThat(mixed).hasSize(DOCUMENTS);
    for (final Map.Entry<String, Integer> entry : mixed.entrySet())
      assertThat(entry.getValue()).isEqualTo(entry.getKey().equals("7") ? 5 : 2);
  }

  private Map<String, Integer> scoresOf(final TypeIndex index, final String text) {
    final Map<String, Integer> scores = new HashMap<>();
    try (final IndexCursor cursor = index.get(new Object[] { text })) {
      while (cursor.hasNext()) {
        final String id = database.lookupByRID(cursor.next().getIdentity(), true).asDocument().getString("id");
        scores.put(id, cursor.getScore());
      }
    }
    return scores;
  }

  private BasicCommandContext expiredContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");
    return context;
  }

  private int drain(final String sql, final BasicCommandContext parent) {
    final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
    int rows = 0;
    try (final ResultSet rs = statement.execute(database, new Object[0], parent, false)) {
      while (rs.hasNext()) {
        rs.next();
        ++rows;
      }
    }
    return rows;
  }

  private static FetchFromIndexStep indexStepOf(final List<ExecutionStep> steps) {
    if (steps == null)
      return null;
    for (final ExecutionStep step : steps) {
      if (step instanceof final FetchFromIndexStep indexStep)
        return indexStep;
      final FetchFromIndexStep nested = indexStepOf(step.getSubSteps());
      if (nested != null)
        return nested;
    }
    return null;
  }

  /** Whether the check that failed the statement ran in the index code, not in the executor that consumes what it returned. */
  private static boolean raisedByTheIndexLookup(final Throwable failure) {
    for (final StackTraceElement frame : failure.getStackTrace()) {
      if (frame.getClassName().equals(WorkGuard.class.getName()))
        continue;
      return frame.getClassName().startsWith("com.arcadedb.index.");
    }
    return false;
  }

  private void createCorpus(final String similarity, final int buckets, final int properties) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY Doc.id STRING");
      database.command("sql", "CREATE PROPERTY Doc.title STRING");
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (" + (properties == 1 ? "content" : "title, content")
          + ") FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
      for (int i = 0; i < DOCUMENTS; i++)
        database.newDocument("Doc")
            .set("id", String.valueOf(i))
            .set("title", "common headline" + i)
            .set("content", "common word" + i + " " + "common ".repeat(i % 7))
            .save();
    });
  }
}
