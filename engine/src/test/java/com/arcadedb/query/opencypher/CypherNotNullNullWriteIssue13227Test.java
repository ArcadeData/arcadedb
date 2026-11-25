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
package com.arcadedb.query.opencypher;

import com.arcadedb.TestHelper;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Issue #13227: a null written through openCypher to a NOTNULL property was accepted, because openCypher never stores a null
 * (it removes the property, or leaves it out of a record it creates) and the NOTNULL rule of the validator is read from the
 * presence of the property. SQL refuses the same write with "cannot be null". Every write form below is refused the same way
 * now, and leaves the stored state as it was.
 *
 * @author robfrank (ro.franchini@gmail.com)
 */
class CypherNotNullNullWriteIssue13227Test extends TestHelper {

  @Override
  protected void beginTest() {
    database.command("sql", "CREATE VERTEX TYPE T");
    database.command("sql", "CREATE EDGE TYPE R");
    database.command("sql", "CREATE PROPERTY T.id STRING");
    database.command("sql", "CREATE INDEX ON T (id) UNIQUE");
    database.command("sql", "CREATE PROPERTY T.nn STRING (notnull true)");
    database.command("sql", "CREATE PROPERTY R.nn STRING (notnull true)");
    database.command("sql", "CREATE VERTEX TYPE U");
    database.command("sql", "CREATE PROPERTY U.nn STRING (notnull true)");
    database.transaction(() -> {
      database.command("sql", "INSERT INTO T SET id = 'a', nn = 'x'");
      database.command("sql", "INSERT INTO T SET id = 'b', nn = 'y'");
      database.command("sql",
          "CREATE EDGE R FROM (SELECT FROM T WHERE id = 'a') TO (SELECT FROM T WHERE id = 'b') SET nn = 'z'");
    });
  }

  static Stream<Arguments> nullWrites() {
    return Stream.of(
        // single-property forms
        Arguments.of("MATCH (n:T {id:'a'}) SET n.nn = null", false),
        Arguments.of("UNWIND $rows AS row MATCH (n:T {id: row.id}) SET n.nn = row.v", false),
        Arguments.of("MERGE (n:T {id:'a'}) ON MATCH SET n.nn = null", false),
        Arguments.of("MATCH (n:T {id:'a'}) FOREACH (_ IN [1] | SET n.nn = null)", false),
        Arguments.of("MATCH (n:T {id:'a'}) SET n['nn'] = null", false),
        Arguments.of("MATCH (n:T {id:'a'}) SET (CASE WHEN true THEN n END).nn = null", false),
        // map forms
        Arguments.of("MATCH (n:T {id:'a'}) SET n += {nn: null}", false),
        Arguments.of("MATCH (n:T {id:'a'}) SET n += $m", false),
        Arguments.of("MATCH (n:T {id:'a'}) SET n = {id: 'a', nn: null}", false),
        Arguments.of("MATCH (n:T {id:'a'}) SET n = $m", false),
        Arguments.of("UNWIND $rows AS row MATCH (n:T {id: row.id}) SET n += row.props", false),
        Arguments.of("UNWIND $rows AS row MATCH (n:T {id: row.id}) SET n = row.props", false),
        Arguments.of("UNWIND $rows AS row MERGE (n:T {id: row.id}) SET n += row.props", false),
        Arguments.of("MERGE (n:T {id:'a'}) ON MATCH SET n += $m", false),
        Arguments.of("MERGE (n:T {id:'a'}) ON MATCH SET n = $m", false),
        Arguments.of("MATCH (n:T {id:'a'}) FOREACH (_ IN [1] | SET n += $m)", false),
        Arguments.of("UNWIND $rows AS row MERGE (n:T {id: row.id}) WITH n, row "
            + "FOREACH (_ IN CASE WHEN true THEN [1] ELSE [] END | SET n += row.props)", false),
        // an earlier item of the clause must not be written either
        Arguments.of("MATCH (n:T {id:'a'}), (m:T {id:'b'}) SET m.nn = 'changed', n += {nn: null}", false),
        // create forms
        Arguments.of("CREATE (n:T {id:'c', nn: null})", true),
        Arguments.of("CREATE (n:T $m)", true),
        Arguments.of("CREATE (n:T {id:'c'}) SET n.nn = null", true),
        Arguments.of("CREATE (n:T) SET n += $m", true),
        Arguments.of("CREATE (n:T) SET n = $m", true),
        Arguments.of("UNWIND $rows AS row CREATE (n:T {id: row.id, nn: row.v})", true),
        Arguments.of("UNWIND $rows AS row CREATE (n:T) SET n += row.props", true),
        Arguments.of("MERGE (n:T {id:'c'}) ON CREATE SET n.nn = null", true),
        Arguments.of("MERGE (n:T {id:'c'}) ON CREATE SET n += $m", true),
        // relationships
        Arguments.of("MATCH ()-[r:R]->() SET r.nn = null", false),
        Arguments.of("MATCH ()-[r:R]->() SET r += {nn: null}", false),
        Arguments.of("MATCH ()-[r:R]->() SET r = {nn: null}", false),
        Arguments.of("CREATE (:T {id:'e1'})-[:R {nn: null}]->(:T {id:'e2'})", true),
        Arguments.of("CREATE (:T {id:'e1'})-[:R $m]->(:T {id:'e2'})", true),
        Arguments.of("MATCH (a:T {id:'a'}), (b:T {id:'b'}) CREATE (a)-[:R {nn: $v}]->(b)", false),
        Arguments.of("MATCH (a:T {id:'a'}), (b:T {id:'b'}) MERGE (a)-[r:R {k: 1}]->(b) ON CREATE SET r += $m", false),
        // the label written by the same clause moves the node to a type with a NOTNULL property
        Arguments.of("CREATE (n:V {id:'c'}) WITH n SET n:U, n.nn = null", true));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("nullWrites")
  void aNullWrittenToANotNullPropertyIsRefused(final String statement, final boolean create) {
    final String before = state();

    final Throwable thrown = catchThrowable(() -> run(statement, params(create)));

    assertThat(thrown).as("statement accepted a null for a NOTNULL property").isNotNull();
    assertThat(rootValidation(thrown)).as("refusal %s", thrown).isNotNull();
    assertThat(rootValidation(thrown).getMessage()).contains("cannot be null");
    assertThat(state()).isEqualTo(before);
  }

  @Test
  void sqlRefusesTheSameNull() {
    final Throwable thrown = catchThrowable(
        () -> database.transaction(() -> database.command("sql", "UPDATE T SET nn = null WHERE id = 'a'")));
    assertThat(rootValidation(thrown)).isNotNull();
    assertThat(rootValidation(thrown).getMessage()).contains("cannot be null");
  }

  /** A removal is not a null write (#13039): REMOVE and the keys SET n = map leaves out stay accepted. */
  @Test
  void removalsStayAccepted() {
    run("MATCH (n:T {id:'a'}) REMOVE n.nn", Map.of());
    run("MATCH (n:T {id:'b'}) SET n = {id: 'b'}", Map.of());
    run("MATCH ()-[r:R]->() SET r = {other: 1}", Map.of());
    assertThat(state()).isEqualTo("a:<absent> b:<absent> R:<absent>");
  }

  /** A null for a property that is not NOTNULL keeps the openCypher meaning: it removes the property, or is not stored. */
  @Test
  void aNullForAnUnconstrainedPropertyIsStillARemoval() {
    run("MATCH (n:T {id:'a'}) SET n += {free: 1}", Map.of());
    run("MATCH (n:T {id:'a'}) SET n += {free: null}", Map.of());
    run("CREATE (n:T {id:'c', nn: 'w', free: null})", Map.of());
    try (final ResultSet rs = database.query("sql", "SELECT FROM T WHERE id IN ['a', 'c']")) {
      while (rs.hasNext())
        assertThat(rs.next().getPropertyNames()).doesNotContain("free");
    }
  }

  /** A non-null value for the NOTNULL property is still written by every map form. */
  @Test
  void nonNullValuesAreStillWritten() {
    run("MATCH (n:T {id:'a'}) SET n += {nn: 'p'}", Map.of());
    run("MATCH (n:T {id:'b'}) SET n = {id: 'b', nn: 'q'}", Map.of());
    run("MATCH ()-[r:R]->() SET r += {nn: 's'}", Map.of());
    assertThat(state()).isEqualTo("a:p b:q R:s");
  }

  private void run(final String statement, final Map<String, Object> params) {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("opencypher", statement, params)) {
        while (rs.hasNext())
          rs.next();
      }
    });
  }

  private static Map<String, Object> params(final boolean create) {
    final String id = create ? "c" : "a";
    final Map<String, Object> props = new LinkedHashMap<>();
    props.put("id", id);
    props.put("nn", null);
    final Map<String, Object> row = new LinkedHashMap<>();
    row.put("id", id);
    row.put("v", null);
    row.put("props", props);
    final Map<String, Object> params = new LinkedHashMap<>();
    params.put("m", props);
    params.put("rows", List.of(row));
    params.put("v", null);
    return params;
  }

  private String state() {
    final StringBuilder sb = new StringBuilder();
    try (final ResultSet rs = database.query("sql", "SELECT FROM V ORDER BY id")) {
      while (rs.hasNext())
        sb.append(rs.next().getIdentity().get()).append(' ');
    } catch (final Exception ignored) {
      // type V does not exist yet
    }
    for (final String type : List.of("T", "U")) {
      try (final ResultSet rs = database.query("sql", "SELECT FROM " + type + " ORDER BY id")) {
        while (rs.hasNext()) {
          final Result r = rs.next();
          sb.append(r.<Object>getProperty("id")).append(':').append(nn(r)).append(' ');
        }
      }
    }
    try (final ResultSet rs = database.query("sql", "SELECT FROM R")) {
      while (rs.hasNext())
        sb.append("R:").append(nn(rs.next())).append(' ');
    }
    return sb.toString().trim();
  }

  private static Object nn(final Result r) {
    return r.getPropertyNames().contains("nn") ? r.getProperty("nn") : "<absent>";
  }

  private static ValidationException rootValidation(Throwable t) {
    while (t != null) {
      if (t instanceof ValidationException v)
        return v;
      t = t.getCause();
    }
    return null;
  }
}
