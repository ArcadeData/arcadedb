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
package com.arcadedb.query.sql;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Identifiable;
import com.arcadedb.exception.CommandSemanticException;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.SchemaException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.CommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.RecordAttribute;
import com.arcadedb.query.sql.parser.SuffixIdentifier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #12795: eight statement shapes that are caller mistakes, or forms the engine refuses on purpose, answered HTTP 500. Each
 * is now refused with an exception of the family the HTTP layer answers with a 400, and a message that names the mistake.
 */
class Issue12795CallerMistakesTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE PROPERTY P.name STRING");
    database.command("sql", "CREATE INDEX ON P (name) FULL_TEXT");
    database.command("sql", "CREATE VERTEX TYPE Q");
    database.transaction(
        () -> database.command("sql", "INSERT INTO P CONTENT {\"id\": 1, \"name\": \"a b\", \"tags\": [\"a\"], \"m\": {\"k\": 1}, \"n\": 1}"));
  }

  @Test
  void updateAddPutIncrementAreRefusedNamingTheSetAlternative() {
    assertRefused("UPDATE P ADD tags = 'x' WHERE id = 1", "tags = tags || 'x'");
    assertRefused("UPDATE P PUT m = 'k2', 2 WHERE id = 1", "SET m.k2 = 2");
    assertRefused("UPDATE P INCREMENT n = 1 WHERE id = 1", "SET n = n + 1");
    assertRefused("MOVE VERTEX (SELECT FROM P) TO TYPE:Q ADD tags = 'x'", "tags = tags || 'x'");

    // the alternatives the messages name do what the refused forms meant
    database.transaction(() -> database.command("sql", "UPDATE P SET tags = tags || 'x', m.k2 = 2, n = n + 1 WHERE id = 1"));
    try (final ResultSet rs = database.query("sql", "SELECT tags, m, n FROM P WHERE id = 1")) {
      final Result row = rs.next();
      assertThat(row.<List<Object>>getProperty("tags")).containsExactly("a", "x");
      assertThat(row.<Map<String, Object>>getProperty("m")).containsEntry("k", 1).containsEntry("k2", 2);
      assertThat(row.<Integer>getProperty("n")).isEqualTo(2);
    }
  }

  @Test
  void nestedDistinctIsASemanticRefusal() {
    assertThatThrownBy(() -> drain("SELECT FROM P WHERE distinct(n) = 1")).isInstanceOf(CommandSemanticException.class)
        .hasMessageContaining("'distinct' is supported only as the whole SELECT projection");
  }

  @Test
  void anEmptyRidArrayTargetAnswersNoRows() {
    // the grammar accepts `FROM []` as the empty target, as `FROM :p` with an empty list bound already answers
    assertThat(drain("SELECT FROM []")).isEmpty();
    assertThat(drain("TRAVERSE out() FROM []")).isEmpty();
    assertThat(drain("SELECT FROM :p", Map.of("p", List.of()))).isEmpty();
  }

  @Test
  void parameterTargetOfTheWrongKindIsATypeMismatch() {
    assertThatThrownBy(() -> drain("SELECT FROM :p", Map.of("p", 1))).isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("1").hasMessageContaining("Integer");
    assertThatThrownBy(() -> drain("SELECT FROM :p", Map.of("p", List.of(1)))).isInstanceOf(TypeMismatchException.class);
    assertThatThrownBy(() -> drain("TRAVERSE out() FROM :p", Map.of("p", 1))).isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("Integer");
  }

  @Test
  void contentParameterOfTheWrongKindIsATypeMismatchNamingTheStatement() {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "INSERT INTO P CONTENT :p", Map.of("p", 1)).close()))
        .isInstanceOf(TypeMismatchException.class).hasMessageContaining("INSERT").hasMessageNotContaining("UPDATE");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "UPDATE P CONTENT :p", Map.of("p", 1)).close()))
        .isInstanceOf(TypeMismatchException.class).hasMessageContaining("UPDATE");
    assertThatThrownBy(
        () -> database.transaction(() -> database.command("sql", "INSERT INTO P CONTENT :p", Map.of("p", List.of(1))).close()))
        .isInstanceOf(TypeMismatchException.class).hasMessageContaining("INSERT");
  }

  @Test
  void expandOfAWholeRecordNamesItInsteadOfDumpingIt() {
    final String rid;
    try (final ResultSet rs = database.query("sql", "SELECT FROM P")) {
      rid = rs.next().getIdentity().orElseThrow().toString();
    }
    for (final String sql : new String[] { "SELECT expand(@this) FROM P", "SELECT expand(*) FROM P" })
      assertThatThrownBy(() -> drain(sql)).as(sql).isInstanceOf(TypeMismatchException.class).hasMessageContaining(rid)
          .hasMessageContaining("P").hasMessageNotContaining("a b");
  }

  @Test
  void searchIndexNamedByARecordExpressionIsEvaluatedPerRecord() {
    // @type cannot be resolved while the plan is built: the call is evaluated per record, where it names an index that is not there
    assertThatThrownBy(() -> drain("SELECT FROM P WHERE SEARCH_INDEX(@type, 'a') = true")).isInstanceOf(SchemaException.class)
        .hasMessageContaining("Index with name 'P' was not found");
    // SEARCH_FIELDS shares the plan-time resolution: its field list is read per record the same way
    assertThatThrownBy(() -> drain("SELECT FROM P WHERE SEARCH_FIELDS(@type, 'a') = true")).isInstanceOf(
        InvalidArgumentException.class).hasMessageContaining("No full-text index found for fields: [P]");

    // a name that resolves per record to a full-text index finds the record
    try (final ResultSet rs = database.query("sql", "SELECT FROM P WHERE SEARCH_INDEX(@type + '[name]', 'a') = true")) {
      assertThat(rs.next().<String>getProperty("name")).isEqualTo("a b");
      assertThat(rs.hasNext()).isFalse();
    }
  }

  @Test
  void searchArgumentsReadFromTheParentRowOfASubQueryStillSearchTheIndex() {
    // $parent.$current is known only when the sub-query runs, and is not a function of the record being searched: the call is
    // still answered by the index, with the value of the parent row
    database.transaction(() -> {
      database.command("sql", "INSERT INTO Q SET term = 'a', idx = 'P[name]', field = 'name'");
      database.command("sql", "INSERT INTO Q SET term = 'zzz', idx = 'P[name]', field = 'name'");
    });
    for (final String call : new String[] { "SEARCH_INDEX('P[name]', $parent.$current.term)",
        "SEARCH_INDEX($parent.$current.idx, $parent.$current.term)", "SEARCH_FIELDS(['name'], $parent.$current.term)",
        "SEARCH_FIELDS($parent.$current.field, $parent.$current.term)" }) {
      final String sql = "SELECT term, (SELECT name FROM P WHERE " + call + " = true) AS hits FROM Q ORDER BY term";
      final List<Result> rows = drain(sql);
      assertThat(rows).as(sql).hasSize(2);
      assertThat(rows.get(0).<List<Object>>getProperty("hits")).as(sql).hasSize(1);
      assertThat(rows.get(1).<List<Object>>getProperty("hits")).as(sql).isEmpty();
    }
  }

  @Test
  void aRecordAttributeEvaluatedWithNoRecordIsNull() {
    // the root of the SEARCH_INDEX(@type, ...) failure: every record attribute dereferenced the record the planner does not have
    final String[] attributes = { "@type", "@rid", "@cat", "@in", "@out", "@this", "@props" };
    final CommandContext context = new BasicCommandContext();
    for (final String attribute : attributes) {
      final RecordAttribute recordAttribute = new RecordAttribute();
      recordAttribute.setName(attribute);
      // the value itself, not only the absence of an NPE: null, as a property of no record is
      assertThat(new SuffixIdentifier(recordAttribute).execute((Identifiable) null, context)).as(attribute).isNull();
    }
    for (final String attribute : new String[] { "@type", "@rid", "@cat", "@in", "@out" })
      assertThatThrownBy(() -> drain("SELECT FROM P WHERE SEARCH_INDEX(" + attribute + ", 'a') = true")).as(attribute)
          .isNotInstanceOf(NullPointerException.class);
    // as the query, on the indexed path: no record, no text to search
    assertThat(drain("SELECT FROM P WHERE SEARCH_INDEX('P[name]', @type) = true")).isEmpty();
  }

  @Test
  void loadCsvFromNullIsAnInvalidArgument() {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("opencypher", "LOAD CSV FROM null AS row RETURN row")) {
        rs.next();
      }
    }).isInstanceOf(InvalidArgumentException.class).hasMessageContaining("LOAD CSV");
  }

  private void assertRefused(final String sql, final String alternative) {
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", sql).close())).as(sql)
        .isInstanceOf(CommandSemanticException.class).hasMessageContaining(alternative);
  }

  private List<Result> drain(final String sql) {
    return drain(sql, Map.of());
  }

  private List<Result> drain(final String sql, final Map<String, Object> params) {
    final List<Result> rows = new ArrayList<>();
    try (final ResultSet rs = database.query("sql", sql, params)) {
      while (rs.hasNext())
        rows.add(rs.next());
    }
    return rows;
  }
}
