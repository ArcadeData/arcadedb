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
package com.arcadedb.database.async;

import com.arcadedb.TestHelper;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #9941: {@code async().query(language, query, callback, args...)} bound the executor's own
 * {@code ContextConfiguration} as the first query parameter, for positional parameters and for a named-parameter map
 * alike, because {@link DatabaseAsyncCommand#execute} called {@code database.query(language, command, configuration,
 * parameters)} and {@code Database} has no {@code query} overload taking a configuration, so the call resolved to the
 * varargs {@code query(String, String, Object...)}. The synchronous query is the control.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9941AsyncQueryParametersTest extends TestHelper {

  private static final String TYPE = "Issue9941V";

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType(TYPE);
    database.transaction(() -> {
      database.newVertex(TYPE).set("name", "a").set("v", 1).save();
      database.newVertex(TYPE).set("name", "b").set("v", 2).save();
      database.newVertex(TYPE).set("name", "c").set("v", 3).save();
    });
  }

  @Test
  void asyncQueryBindsPositionalParameters() {
    try (final ResultSet rs = database.query("sql", "SELECT ? AS a, ? AS b", 1, "x")) {
      final Result row = rs.next();
      assertThat((Integer) row.getProperty("a")).isEqualTo(1);
      assertThat((String) row.getProperty("b")).isEqualTo("x");
    }

    final List<Result> rows = asyncQuery("sql", "SELECT ? AS a, ? AS b", 1, "x");
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.getFirst().getProperty("a")).isEqualTo(1);
    assertThat((Object) rows.getFirst().getProperty("b")).isEqualTo("x");
  }

  @Test
  void asyncQueryBindsNamedParameters() {
    final Map<String, Object> named = Map.of("k", 5);
    try (final ResultSet rs = database.query("sql", "SELECT :k AS a", named)) {
      assertThat((Integer) rs.next().getProperty("a")).isEqualTo(5);
    }

    final List<Result> rows = asyncQuery("sql", "SELECT :k AS a", named);
    assertThat(rows).hasSize(1);
    assertThat((Object) rows.getFirst().getProperty("a")).isEqualTo(5);
  }

  @Test
  void asyncQueryFiltersWithParametersAsTheSynchronousQueryDoes() {
    final List<Result> positional = asyncQuery("sql", "SELECT FROM " + TYPE + " WHERE v >= ? ORDER BY v", 2);
    assertThat(positional).extracting(r -> (String) r.getProperty("name")).containsExactly("b", "c");

    final List<Result> named = asyncQuery("sql", "SELECT FROM " + TYPE + " WHERE name = :name", Map.of("name", "a"));
    assertThat(named).extracting(r -> (Integer) r.getProperty("v")).containsExactly(1);
  }

  @Test
  void asyncOpenCypherQueryBindsNamedParameters() {
    final String cypher = "MATCH (n:" + TYPE + ") WHERE n.v = $v RETURN n.name AS name";
    try (final ResultSet rs = database.query("opencypher", cypher, Map.of("v", 3))) {
      assertThat((String) rs.next().getProperty("name")).isEqualTo("c");
    }

    final List<Result> rows = asyncQuery("opencypher", cypher, Map.of("v", 3));
    assertThat(rows).extracting(r -> (String) r.getProperty("name")).containsExactly("c");
  }

  @Test
  void asyncQueryWithoutParametersStillAnswers() {
    final List<Result> rows = asyncQuery("sql", "SELECT count(*) AS total FROM " + TYPE);
    assertThat(rows).hasSize(1);
    assertThat(((Number) rows.getFirst().getProperty("total")).longValue()).isEqualTo(3L);
  }

  private List<Result> asyncQuery(final String language, final String query, final Object... args) {
    final Collector collector = new Collector();
    database.async().query(language, query, collector, args);
    return collector.await();
  }

  private List<Result> asyncQuery(final String language, final String query, final Map<String, Object> args) {
    final Collector collector = new Collector();
    database.async().query(language, query, collector, args);
    return collector.await();
  }

  private class Collector implements AsyncResultsetCallback {
    private final List<Result>               rows  = new CopyOnWriteArrayList<>();
    private final AtomicReference<Exception> error = new AtomicReference<>();

    @Override
    public void onComplete(final ResultSet resultset) {
      while (resultset.hasNext())
        rows.add(resultset.next());
    }

    @Override
    public void onError(final Exception exception) {
      error.set(exception);
    }

    private List<Result> await() {
      database.async().waitCompletion();
      assertThat(error.get()).as("the asynchronous query must not fail").isNull();
      return rows;
    }
  }
}
