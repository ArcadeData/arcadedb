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
import com.arcadedb.exception.TimeoutException;
import com.arcadedb.function.sql.text.SQLFunctionSearchFields;
import com.arcadedb.function.sql.text.SQLFunctionSearchIndex;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.BaseExpression;
import com.arcadedb.query.sql.parser.Expression;
import com.arcadedb.query.sql.parser.FromClause;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The scoring explanation that {@code SEARCH_INDEX} and {@code SEARCH_FIELDS} give for a plan expands the terms of the query and
 * counts the postings of each, so the deadline of the statement is checked while it does.
 * <p>
 * The explanation is called here through the function, as the plan does: the plan itself treats the explanation as informational
 * and renders without it when collecting it fails, so a plan would hide the failure these tests look for. Each test pins an
 * already expired deadline on the context rather than racing a real clock.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class FullTextExplainDeadlineTest extends TestHelper {
  private static final int DOCUMENTS = 50;

  @ParameterizedTest
  @ValueSource(ints = { 1, 4 })
  void searchIndexExplainStopsAtTheDeadline(final int buckets) {
    createCorpus(buckets);
    final String sql = "SELECT SEARCH_INDEX('Doc[content]', 'common') FROM Doc";

    assertThat(catchThrowable(() -> explainSearchIndex(sql, expiredContext()))).as("buckets=" + buckets)
        .isInstanceOf(TimeoutException.class);

    // The same explanation with no bound is given: the failure above is the deadline, not a call that cannot be explained
    final JSONObject explain = (JSONObject) explainSearchIndex(sql, unboundedContext());
    assertThat(explain).as("buckets=" + buckets).isNotNull();
    assertThat(explain.toString()).contains("common");
  }

  @ParameterizedTest
  @ValueSource(ints = { 1, 4 })
  void searchFieldsExplainStopsAtTheDeadline(final int buckets) {
    createCorpus(buckets);
    final String sql = "SELECT SEARCH_FIELDS(['content'], 'common') FROM Doc";

    assertThat(catchThrowable(() -> explainSearchFields(sql, expiredContext()))).as("buckets=" + buckets)
        .isInstanceOf(TimeoutException.class);

    final JSONObject explain = (JSONObject) explainSearchFields(sql, unboundedContext());
    assertThat(explain).as("buckets=" + buckets).isNotNull();
    assertThat(explain.toString()).contains("common");
  }

  /** A logical index over several types is explained member by member, and each member is read under the same deadline. */
  @Test
  void aLogicalIndexExplainStopsAtTheDeadline() {
    database.transaction(() -> {
      database.command("sql", "CREATE VERTEX TYPE Doc");
      database.command("sql", "CREATE VERTEX TYPE Article EXTENDS Doc");
      database.command("sql", "CREATE VERTEX TYPE Post EXTENDS Doc");
      cypher("CREATE FULLTEXT INDEX docs FOR (n:Article|Post) ON EACH [n.title, n.body]");
      cypher("CREATE (:Article {title: 'graph databases', body: 'fast lookups in a graph'})");
      cypher("CREATE (:Post {title: 'graph recipes', body: 'a graph of recipes'})");
    });
    final String sql = "SELECT SEARCH_INDEX('docs', 'graph') FROM Doc";

    assertThat(catchThrowable(() -> explainSearchIndex(sql, expiredContext()))).isInstanceOf(TimeoutException.class);

    // One explanation per member index, under its name
    final JSONObject explain = (JSONObject) explainSearchIndex(sql, unboundedContext());
    assertThat(explain).isNotNull();
    assertThat(explain.keySet()).hasSize(2);
    for (final String member : explain.keySet())
      assertThat(explain.getJSONObject(member).toString()).contains("graph");
  }

  private Object explainSearchIndex(final String sql, final BasicCommandContext context) {
    final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
    return new SQLFunctionSearchIndex().getScoringExplain(statement.getTarget(), null, null, context, argumentsOf(statement));
  }

  private Object explainSearchFields(final String sql, final BasicCommandContext context) {
    final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
    final FromClause target = statement.getTarget();
    return new SQLFunctionSearchFields().getScoringExplain(target, null, null, context, argumentsOf(statement));
  }

  private static Expression[] argumentsOf(final SelectStatement statement) {
    final BaseExpression call = (BaseExpression) statement.getProjection().getItems().getFirst().getExpression().mathExpression;
    return call.identifier.levelZero.functionCall.getParams().toArray(new Expression[0]);
  }

  private BasicCommandContext unboundedContext() {
    final BasicCommandContext context = new BasicCommandContext();
    context.setDatabase((DatabaseInternal) database);
    return context;
  }

  private BasicCommandContext expiredContext() {
    final BasicCommandContext context = unboundedContext();
    context.setCommandDeadline(System.currentTimeMillis() - 1, "test deadline");
    return context;
  }

  private void cypher(final String statement) {
    try (final ResultSet rs = database.command("opencypher", statement)) {
      while (rs.hasNext())
        rs.next();
    }
  }

  private void createCorpus(final int buckets) {
    database.transaction(() -> {
      database.command("sql", "CREATE DOCUMENT TYPE Doc BUCKETS " + buckets);
      database.command("sql", "CREATE PROPERTY Doc.content STRING");
      database.command("sql", "CREATE INDEX ON Doc (content) FULL_TEXT METADATA {\"similarity\": \"BM25\"}");
      for (int i = 0; i < DOCUMENTS; i++)
        database.newDocument("Doc").set("content", "common word" + i).save();
    });
  }
}
