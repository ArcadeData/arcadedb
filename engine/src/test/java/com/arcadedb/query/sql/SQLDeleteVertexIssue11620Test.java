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
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.antlr.SQLAntlrParser;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.Statement;
import com.arcadedb.schema.Schema;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11620: the grammar accepted {@code DELETE VERTEX FROM ...} but the builder never read the keyword, so the statement
 * rendered (in EXPLAIN, and in every stored rendering) as a plain {@code DELETE FROM}, and the keyword meant nothing. It is now
 * kept on the AST, rendered back, and does what it says: only vertices are deleted, a target that is not a vertex type, or a
 * record that is not a vertex, is refused before anything is deleted for it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class SQLDeleteVertexIssue11620Test extends TestHelper {
  private RID vertex1;
  private RID vertex2;
  private RID document;

  @BeforeEach
  void createGraph() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person");
      database.getSchema().createEdgeType("Knows");
      database.getSchema().createDocumentType("Note");
      database.getSchema().getType("Person").createProperty("name", "STRING");
      database.getSchema().createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "Person", "name");

      final MutableVertex a = database.newVertex("Person").set("name", "a").save();
      final MutableVertex b = database.newVertex("Person").set("name", "b").save();
      a.newEdge("Knows", b);
      vertex1 = a.getIdentity();
      vertex2 = b.getIdentity();
      document = database.newDocument("Note").set("text", "n").save().getIdentity();
    });
  }

  @Test
  void theModifierRoundTrips() {
    for (final String query : List.of(//
        "DELETE VERTEX FROM Person",//
        "DELETE VERTEX FROM Person WHERE name = 'a'",//
        "DELETE VERTEX FROM Person RETURN BEFORE WHERE name = 'a' LIMIT 1",//
        "DELETE VERTEX FROM Person WHERE name = 'a' RETURNING @rid",//
        // control: the plain form stays plain
        "DELETE FROM Person WHERE name = 'a'")) {
      final String rendered = render(query);
      assertThat(rendered).as("'%s' rendered as '%s'", query, rendered).startsWith(query.substring(0, query.indexOf(" FROM")));
      assertThat(parse(rendered)).as("'%s' rendered as '%s'", query, rendered).isEqualTo(parse(query));
      assertThat(render(rendered)).isEqualTo(rendered);
    }
    assertThat(parse("DELETE VERTEX FROM Person")).isNotEqualTo(parse("DELETE FROM Person"));
    assertThat(parse("DELETE VERTEX FROM Person").copy()).isEqualTo(parse("DELETE VERTEX FROM Person"));
  }

  @Test
  void explainShowsTheModifier() {
    try (final ResultSet rs = database.command("sql", "EXPLAIN DELETE VERTEX FROM Person WHERE name = 'a'")) {
      final String plan = rs.next().getProperty("executionPlanAsString");
      assertThat(plan).contains("+ DELETE VERTEX");
    }
  }

  @Test
  void deletesTheVertexAndItsEdges() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE VERTEX FROM Person WHERE name = 'a'")) {
        assertThat(rs.next().<Long>getProperty("count")).isEqualTo(1L);
      }
    });
    assertThat(count("SELECT count(*) AS c FROM Person")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS c FROM Knows")).isEqualTo(0L);
  }

  @Test
  void deletesVerticesNamedByRidAndBySubquery() {
    database.transaction(() -> database.command("sql", "DELETE VERTEX FROM " + vertex1).close());
    database.transaction(() -> database.command("sql", "DELETE VERTEX FROM (SELECT FROM Person WHERE name = 'b')").close());
    assertThat(count("SELECT count(*) AS c FROM Person")).isEqualTo(0L);
  }

  @Test
  void returnBeforeAnswersTheDeletedVertices() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE VERTEX FROM Person RETURN BEFORE WHERE name = 'b'")) {
        final Result row = rs.next();
        assertThat(row.<String>getProperty("name")).isEqualTo("b");
        assertThat(rs.hasNext()).isFalse();
      }
    });
  }

  @Test
  void aTypeThatIsNotAVertexTypeIsRefusedWhetherOrNotItHoldsRecords() {
    database.transaction(() -> database.getSchema().createDocumentType("Empty"));
    for (final String type : List.of("Note", "Knows", "Empty"))
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "DELETE VERTEX FROM " + type).close()))//
          .isInstanceOf(CommandExecutionException.class).hasMessageContaining("is not a vertex type");
    assertThat(count("SELECT count(*) AS c FROM Note")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS c FROM Knows")).isEqualTo(1L);
  }

  @Test
  void aRecordThatIsNotAVertexIsRefusedAndNothingIsDeleted() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "DELETE VERTEX FROM [" + vertex2 + ", " + document + "]").close()))//
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("is not a vertex");
    assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "DELETE VERTEX FROM (SELECT FROM Note)").close()))//
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("is not a vertex");
    assertThat(count("SELECT count(*) AS c FROM Person")).isEqualTo(2L);
    assertThat(count("SELECT count(*) AS c FROM Note")).isEqualTo(1L);
  }

  @Test
  void anIndexTargetIsRefused() {
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("sql", "DELETE VERTEX FROM INDEX:`Person[name]` WHERE key = 'a'").close()))//
        .isInstanceOf(CommandExecutionException.class).hasMessageContaining("Cannot apply VERTEX on a delete from index");
    assertThat(count("SELECT count(*) AS c FROM Person WHERE name = 'a'")).isEqualTo(1L);
  }

  @Test
  void thePlainDeleteStillDeletesAnyRecord() {
    database.transaction(() -> database.command("sql", "DELETE FROM Note").close());
    database.transaction(() -> database.command("sql", "DELETE FROM Person WHERE name = 'a'").close());
    assertThat(count("SELECT count(*) AS c FROM Note")).isEqualTo(0L);
    assertThat(count("SELECT count(*) AS c FROM Person")).isEqualTo(1L);
    assertThat(count("SELECT count(*) AS c FROM Knows")).isEqualTo(0L);
  }

  private long count(final String query) {
    try (final ResultSet rs = database.query("sql", query)) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }

  private static Statement parse(final String query) {
    return new SQLAntlrParser(null).parse(query);
  }

  private static String render(final String query) {
    final StringBuilder builder = new StringBuilder();
    parse(query).toString((Map<String, Object>) null, builder);
    return builder.toString();
  }
}
