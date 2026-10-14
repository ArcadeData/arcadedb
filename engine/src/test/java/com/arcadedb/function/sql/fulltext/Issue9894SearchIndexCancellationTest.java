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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.exception.QueryTerminatedException;
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.text.SQLFunctionSearchFields;
import com.arcadedb.function.sql.text.SQLFunctionSearchFieldsMore;
import com.arcadedb.function.sql.text.SQLFunctionSearchIndexMore;
import com.arcadedb.index.TypeIndex;
import com.arcadedb.index.fulltext.FullTextSearch;
import com.arcadedb.query.RunningQuery;
import com.arcadedb.query.RunningQueryRegistry;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.WorkGuard;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #9894: SEARCH_INDEX and the other full-text functions did all their work - posting walks, scoring, sorting and
 * loading every match - before the first row, where no check of the executor reaches. A terminated statement, or one past
 * its deadline, kept a thread and the heap busy until the whole search was over.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9894SearchIndexCancellationTest extends TestHelper {
  /** Well past the 1024 postings between two checks of a guard. */
  private static final int DOCUMENTS = 3_000;

  private final RunningQueryRegistry registry = new RunningQueryRegistry();

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
  void terminatedStatementStopsInsideTheSearch(final String variant) {
    createCorpus(variant);

    for (final String query : new String[] { "common", "common OR word*", "+common +word1*" }) {
      final Throwable failure;
      try (final RunningQuery q = registry.register(database.getName(), "admin", "test", null, null, RunningQuery.Client.NONE)) {
        q.terminate("root");
        failure = catchThrowable(() -> {
          try (final ResultSet rs = database.query("sql",
              "SELECT FROM Doc WHERE SEARCH_INDEX('Doc[content]', ?) = true LIMIT 5", query)) {
            while (rs.hasNext())
              rs.next();
          }
        });
      }

      assertThat(failure).as(variant + " '" + query + "'").isInstanceOf(QueryTerminatedException.class);
      // Before the fix the search ran to the end and a later check of the executor reported the termination
      assertThat(raisedInsideTheFullTextSearch(failure))
          .as(variant + " '" + query + "' must stop inside the search, not after it: " + Arrays.toString(failure.getStackTrace()))
          .isTrue();
    }
  }

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "BM25_4", "CLASSIC_1", "CLASSIC_4" })
  void searchPastTheDeadlineTimesOut(final String variant) {
    createCorpus(variant);
    final TypeIndex index = (TypeIndex) database.getSchema().getIndexByName("Doc[content]");

    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");

    final Throwable failure = catchThrowable(() -> FullTextSearch.search(index, "common", -1, WorkGuard.forCommandDeadline(context)));
    assertThat(failure).as(variant).isInstanceOf(TimeoutException.class);

    // The same search with no bound answers every document
    final Map<RID, Float> all = FullTextSearch.search(index, "common", -1, WorkGuard.forCommandDeadline(null));
    assertThat(all).hasSize(DOCUMENTS);
  }

  @ParameterizedTest
  @ValueSource(strings = { "BM25_1", "CLASSIC_4" })
  void searchFieldsAndMoreLikeThisStopInsideTheSearch(final String variant) {
    createCorpus(variant);
    // These functions are evaluated row by row after a scan, whose own check catches a statement terminated before its first
    // row: what was unguarded is the search their first evaluation runs, so they are called with a deadline already past
    final Document source = database.iterateType("Doc", false).next().asDocument();
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");

    assertThat(catchThrowable(() -> new SQLFunctionSearchFields().execute(null, source, null,
        new Object[] { List.of("content"), "common" }, context))).as("SEARCH_FIELDS").isInstanceOf(TimeoutException.class);
    assertThat(catchThrowable(() -> new SQLFunctionSearchIndexMore().execute(null, source, null,
        new Object[] { "Doc[content]", List.of(source.getIdentity()) }, context))).as("SEARCH_INDEX_MORE")
        .isInstanceOf(TimeoutException.class);
    assertThat(catchThrowable(() -> new SQLFunctionSearchFieldsMore().execute(null, source, null,
        new Object[] { List.of("content"), List.of(source.getIdentity()) }, context))).as("SEARCH_FIELDS_MORE")
        .isInstanceOf(TimeoutException.class);
  }

  private void createCorpus(final String variant) {
    final String similarity = variant.substring(0, variant.indexOf('_'));
    final int buckets = Integer.parseInt(variant.substring(variant.indexOf('_') + 1));
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"" + similarity + "\"}");
      for (int i = 0; i < DOCUMENTS; i++)
        database.newDocument("Doc").set("content", "common word" + i + " " + "common ".repeat(i % 7)).save();
    });
  }

  private static boolean raisedInsideTheFullTextSearch(final Throwable failure) {
    for (final StackTraceElement frame : failure.getStackTrace())
      if (frame.getClassName().startsWith("com.arcadedb.index.fulltext."))
        return true;
    return false;
  }
}
