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
package com.arcadedb.query.search;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.index.IndexException;
import com.arcadedb.index.fulltext.FullTextQueryParseException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #7862: issue #7393 made a malformed full-text query a client error at the two call sites the report came
 * in through - the hybrid search's full-text leg and the standalone full-text search API - by re-typing the
 * parser's exception where those two raise it. The other four production callers of {@code FullTextSearch}
 * hand the caller's own query text to the Lucene parser unwrapped, so a syntax error there still left the
 * server holding a {@code FullTextQueryParseException}: it extends {@link IndexException}, no arm of any error
 * ladder matched it, and a typo came out as an internal server fault with a stack trace in the log.
 * <p>
 * Fixed by classification rather than by a fifth and sixth wrap: {@link ErrorCategory} - the one place
 * ArcadeDB's exception hierarchy is read, and what Postgres, MongoDB, Redis, GraphQL, gRPC and Bolt all answer
 * from - now calls it {@link ErrorCategory#PARSING}, and the HTTP ladder has an arm of its own for it. That
 * covers the four call sites named here AND the next one added, which is what the per-call-site wrap could not
 * do; the two #7393 wraps stay as they are, because their messages are part of a shipped contract.
 * <p>
 * {@link IndexException} itself is deliberately NOT classified: a tokenizer, an analyzer or an index read
 * failing IS a server fault, and the last test here is what keeps the two apart.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7862MalformedFullTextQueryAtEveryCallSiteTest extends TestHelper {
  /** Valid-looking Lucene expressions the parser refuses: a dangling operator, an unbalanced group, a bare fuzzy. */
  private static final String[] MALFORMED = { "foo AND", "title:(foo AND", "~", "\"unbalanced" };

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Article");
      database.command("sql", "CREATE PROPERTY Article.content STRING");
      database.command("sql", "CREATE INDEX ON Article (content) FULL_TEXT");
      database.command("sql", "INSERT INTO Article SET content = 'gearbox manual'");

      // db.index.fulltext.queryRelationships() refuses a node-type index by name, so it needs one of its own.
      database.command("sql", "CREATE EDGE TYPE Cites");
      database.command("sql", "CREATE PROPERTY Cites.note STRING");
      database.command("sql", "CREATE INDEX ON Cites (note) FULL_TEXT");
    });
  }

  @Test
  void searchIndexReportsAMalformedQueryAsTheCallersMistake() {
    for (final String malformed : MALFORMED)
      assertIsAParseError(malformed, catchThrowable(() -> consume(
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', ?) = true", malformed)));
  }

  @Test
  void searchFieldsReportsAMalformedQueryAsTheCallersMistake() {
    for (final String malformed : MALFORMED)
      assertIsAParseError(malformed,
          catchThrowable(() -> consume("SELECT FROM Article WHERE SEARCH_FIELDS(['content'], ?) = true", malformed)));
  }

  /**
   * The one arm of the report that turns out NOT to be a defect, pinned so it is not "fixed" later. The EXPLAIN
   * path does reach the same parser, through {@code FullTextSearch.explainScoring}, but
   * {@code FetchFromIndexedFunctionStep.prettyPrint} already treats the scoring metadata as informational and
   * catches whatever collecting it throws, logging at FINE. So an EXPLAIN of a query with a malformed search
   * expression renders its plan without the {@code SCORING} line rather than answering 500 - and EXPLAIN does
   * not execute the query, so nothing else in it reaches the parser either.
   */
  @Test
  void explainingAQueryWithAMalformedSearchExpressionStillRendersItsPlan() {
    assertThat(catchThrowable(() -> consume(
        "EXPLAIN SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', ?) = true", "foo AND"))).isNull();
  }

  @Test
  void theCypherFullTextProceduresReportAMalformedQueryAsTheCallersMistake() {
    for (final String malformed : MALFORMED) {
      assertIsAParseError(malformed, catchThrowable(() -> consumeCypher(
          "CALL db.index.fulltext.queryNodes('Article[content]', $q)", malformed)));
      assertIsAParseError(malformed, catchThrowable(() -> consumeCypher(
          "CALL db.index.fulltext.queryRelationships('Cites[note]', $q)", malformed)));
    }
  }

  /** A well-formed query still runs, so the assertions above cannot be passing because nothing works. */
  @Test
  void aWellFormedQueryStillRuns() {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql",
          "SELECT FROM Article WHERE SEARCH_INDEX('Article[content]', 'gearbox') = true")) {
        assertThat(rs.stream().count()).isEqualTo(1);
      }
    });
  }

  /**
   * The line the fix must not cross: an {@link IndexException} that is NOT the parser's stays a server fault, so
   * an index read or a tokenizer failing is still reported as one. Classifying the supertype would have been the
   * cheap way to close this issue and the wrong one.
   */
  @Test
  void aPlainIndexFaultIsStillAServerFault() {
    assertThat(ErrorCategory.of(new IndexException("Error on tokenizer"))).isEqualTo(ErrorCategory.SERVER);
    assertThat(ErrorCategory.of(new FullTextQueryParseException("Invalid search query: foo AND", null)))
        .isEqualTo(ErrorCategory.PARSING);
  }

  private void assertIsAParseError(final String malformed, final Throwable thrown) {
    assertThat(thrown).as("query <%s> must be refused", malformed).isNotNull();
    assertThat(causeChainOf(thrown)).as("query <%s>", malformed).anyMatch(FullTextQueryParseException.class::isInstance);
    assertThat(ErrorCategory.of(thrown)).as("query <%s>", malformed).isEqualTo(ErrorCategory.PARSING);
  }

  private static List<Throwable> causeChainOf(final Throwable error) {
    final List<Throwable> chain = new ArrayList<>();
    for (Throwable t = error; t != null && chain.size() < 32; t = t.getCause())
      chain.add(t);
    return chain;
  }

  /** Drains the result set: these functions are evaluated lazily, so an unconsumed query refuses nothing. */
  private void consume(final String sql, final String queryText) {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("sql", sql, queryText)) {
        rs.stream().forEach(r -> {
        });
      }
    });
  }

  private void consumeCypher(final String cypher, final String queryText) {
    database.transaction(() -> {
      try (final ResultSet rs = database.query("cypher", cypher, "q", queryText)) {
        rs.stream().forEach(r -> {
        });
      }
    });
  }
}
