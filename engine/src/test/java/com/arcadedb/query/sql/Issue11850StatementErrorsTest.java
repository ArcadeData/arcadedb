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
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.CommandParsingException;
import com.arcadedb.exception.InvalidArgumentException;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.serializer.JsonGraphSerializer;
import com.arcadedb.serializer.JsonSerializer;
import com.arcadedb.serializer.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #11850: statements with a user error answered HTTP 500. The engine side of it: {@code @rid} read while an INSERT builds
 * the record threw a {@code NullPointerException}, a record holding itself overflowed the stack when its JSON was written (and
 * an INSERT stored a link to a position that does not exist), and the refusals of a statement's text were raised as plain
 * {@code CommandExecutionException}s, which every protocol reports as a server fault. Each is now null, a link, or a client
 * error of the family the HTTP layer answers with a 400.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue11850StatementErrorsTest extends TestHelper {

  @BeforeEach
  void createSchema() {
    database.command("sql", "CREATE VERTEX TYPE V");
    database.command("sql", "CREATE PROPERTY V.i INTEGER");
    database.command("sql", "CREATE PROPERTY V.s STRING");
    database.command("sql", "CREATE INDEX ON V (i) NOTUNIQUE");
    database.command("sql", "CREATE INDEX ON V (s) FULL_TEXT");
    database.command("sql", "CREATE DOCUMENT TYPE D");
    database.command("sql", "CREATE VERTEX TYPE P");
    database.command("sql", "CREATE EDGE TYPE K");
    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.command("sql", "INSERT INTO V SET i = ?, s = ?", i, "s " + i + " word");
      database.command("sql", "INSERT INTO D SET x = 1");
      database.command("opencypher", "CREATE (a:P {i: 1})-[:K]->(b:P {i: 2})");
    });
  }

  @Test
  void ridReadWhileAnInsertBuildsTheRecordIsNull() {
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "INSERT INTO D SET m = @rid")) {
        final Result row = rs.next();
        assertThat(row.getIdentity()).isPresent();
        assertThat(row.<Object>getProperty("m")).isNull();
        assertThat(row.hasProperty("m")).isTrue();
      }
      try (final ResultSet rs = database.command("sql", "INSERT INTO V SET z = @rid")) {
        assertThat(rs.next().<Object>getProperty("z")).isNull();
      }
    });
  }

  @Test
  void aRecordCannotLinkItselfBeforeItIsSaved() {
    final long before = countD();
    // the link was stored as #<bucket>:-1, a position that is no record
    for (final String insert : new String[] { "INSERT INTO D SET s = @this", "INSERT INTO D SET s = [1, @this]",
        "INSERT INTO D SET s = {'k': @this}" })
      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", insert).close())).as(insert)
          .isInstanceOf(ValidationException.class).hasMessageContaining("cannot hold a link to itself before it is saved");

    // the same through the Java API
    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument doc = database.newDocument("D");
      doc.set("me", doc);
      doc.save();
    })).isInstanceOf(ValidationException.class);

    assertThat(countD()).isEqualTo(before);
  }

  @Test
  void aSavedRecordHoldingItselfIsStoredAsALinkAndItsJsonEnds() {
    final List<RID> updated = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "UPDATE D SET self = @this RETURN AFTER")) {
        final Result row = rs.next();
        final RID rid = row.getIdentity().orElseThrow();
        updated.add(rid);

        // the result row serializes: the record met again inside itself is written as its RID
        final JSONObject json = JsonSerializer.createJsonSerializer().serializeResult(database, row);
        assertThat(json.getString("@rid")).isEqualTo(rid.toString());
        final Object self = json.get("self");
        final String selfText = self instanceof JSONObject nested ? nested.getString("self") : self.toString();
        assertThat(selfText).isEqualTo(rid.toString());

        // and so does the record itself, through every JSON writer
        final Document record = row.getElement().orElseThrow();
        assertThat(record.toJSON(true).toString()).contains(rid.toString());
        assertThat(JsonSerializer.createJsonSerializer().serializeDocument(record).toString()).contains(rid.toString());
        assertThat(JsonGraphSerializer.createJsonGraphSerializer().serializeGraphElement(record).toString()).contains(rid.toString());
      }
    });

    // stored as a link to itself
    database.transaction(() -> {
      final Document reloaded = updated.getFirst().asDocument();
      assertThat(reloaded.get("self")).isEqualTo(updated.getFirst());
    });
  }

  @Test
  void cypherNodeHoldingItselfIsReturned() {
    // stored as links already; the statement that wrote the link also returns it, and writing its JSON never ended
    for (final String cypher : new String[] { "MATCH (n:P {i: 1}) SET n.z = n RETURN n.z AS z, n",
        "MATCH (n:P {i: 1}) SET n.z = [1, n] RETURN n.z AS z, n", "MATCH (n:P {i: 1}) SET n += {z: n} RETURN n.z AS z, n",
        "MATCH p = (n:P {i: 1})-[:K]->() SET n.z = p RETURN n.z AS z, n" })
      database.transaction(() -> {
        try (final ResultSet rs = database.command("opencypher", cypher)) {
          final Result row = rs.next();
          final JSONObject json = JsonSerializer.createJsonSerializer().serializeResult(database, row);
          assertThat(json.has("z")).as(cypher).isTrue();
          final Document node = row.getProperty("n");
          assertThat(JsonGraphSerializer.createJsonGraphSerializer().serializeGraphElement(node).toString()).as(cypher)
              .contains(node.getIdentity().toString());
        }
      });
  }

  @Test
  void refusalsOfTheStatementTextAreClientErrors() {
    assertClientError("SELECT FROM index:`V[i]` WHERE asSet()", CommandParsingException.class, "asSet()");
    assertClientError("SELECT expand(true, 1)", CommandParsingException.class, "expand() takes exactly one argument");
    assertClientError("SELECT concat(list(mode(i))) FROM V", CommandParsingException.class,
        "Cannot calculate an aggregate function of another aggregate function");
    assertClientError("SELECT percentile(i, null) FROM V", InvalidArgumentException.class, "must not be null");
    assertClientError("SELECT search_index('V[s]', null) FROM V", InvalidArgumentException.class, "cannot be null");
  }

  @Test
  void cypherRefusalsAreClientErrors() {
    for (final String cypher : new String[] { "RETURN duration(1) AS d", "RETURN duration(true) AS d" })
      assertThatThrownBy(() -> {
        try (final ResultSet rs = database.query("opencypher", cypher)) {
          rs.next();
        }
      }).as(cypher).isInstanceOf(TypeMismatchException.class);
    // a MERGE property that is null at run time: nothing can match it (Neo4j: SemanticError)
    assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher", "WITH null AS x MERGE (n:P {i: x}) RETURN n")
        .close())).isInstanceOf(InvalidArgumentException.class).hasMessageContaining("MergeReadOwnWrites");
    // a relationship endpoint bound to null
    assertThatThrownBy(() -> database.transaction(
        () -> database.command("opencypher", "OPTIONAL MATCH (x:P {i: 99}) CREATE (x)-[:K]->(:P {i: 3})").close())).isInstanceOf(
        TypeMismatchException.class);
  }

  private void assertClientError(final String sql, final Class<? extends Throwable> expected, final String message) {
    assertThatThrownBy(() -> {
      try (final ResultSet rs = database.query("sql", sql)) {
        while (rs.hasNext())
          rs.next();
      }
    }).as(sql).isInstanceOf(expected).hasMessageContaining(message);
  }

  private long countD() {
    try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM D")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
