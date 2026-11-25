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

import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.Document;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.exception.TypeMismatchException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Concatenating a string with a node, a relationship or a path is a type error, as it is in Neo4j (issue #12622). The {@code +}
 * operator used to answer the Java {@code toString()} of the record ({@code x:#1:0[name=Alice]}), which leaks the internal RID
 * and the storage spelling into a user-facing string. Issue #12397 refused a map and a point the same way.
 *
 * @author lvca (l.garulli@arcadedata.com)
 */
class CypherStringConcatGraphEntityIssue12622Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/CypherStringConcatGraphEntityIssue12622Test");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.getSchema().createDocumentType("Doc");
    database.transaction(() -> database.command("opencypher", "CREATE (a:Person {name:'Alice'})-[:Knows]->(b:Person {name:'Bob'})"));
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void concatenatingAStringWithANodeIsRefused() {
    assertRefused("MATCH (a:Person {name:'Alice'}) RETURN 'x:' + a AS v", "Node");
  }

  @Test
  void concatenatingANodeWithAStringIsRefused() {
    assertRefused("MATCH (a:Person {name:'Alice'}) RETURN a + ':x' AS v", "Node");
  }

  @Test
  void concatenatingANodeCarriedThroughWithIsRefused() {
    assertRefused("MATCH (a:Person {name:'Alice'}) WITH a AS n RETURN 'x:' + n AS v", "Node");
  }

  @Test
  void concatenatingAStringWithARelationshipIsRefused() {
    assertRefused("MATCH ()-[r:Knows]->() RETURN 'r:' + r AS v", "Relationship");
  }

  @Test
  void concatenatingARelationshipWithAStringIsRefused() {
    assertRefused("MATCH ()-[r:Knows]->() RETURN r + ':r' AS v", "Relationship");
  }

  @Test
  void concatenatingAStringWithAPathIsRefused() {
    assertRefused("MATCH p = (a:Person {name:'Alice'})-->() RETURN 'p:' + p AS v", "Path");
  }

  @Test
  void concatenatingAPathWithAStringIsRefused() {
    assertRefused("MATCH p = (a:Person {name:'Alice'})-->() RETURN p + ':p' AS v", "Path");
  }

  @Test
  void concatenatingAStringWithAShortestPathIsRefused() {
    assertRefused("MATCH p = shortestPath((a:Person {name:'Alice'})-[*]->(b:Person {name:'Bob'})) RETURN 'p:' + p AS v", "Path");
  }

  @Test
  void concatenatingAStringWithAVariableLengthRelationshipListAppends() {
    // a variable-length relationship binds a LIST, and + appends a string to a list: no record is turned into a string
    final ResultSet rs = database.query("opencypher", "MATCH (a:Person {name:'Alice'})-[r*1..1]->() RETURN r + 'x' AS v");
    assertThat(rs.hasNext()).isTrue();
    final Object v = rs.next().getProperty("v");
    assertThat(v).isInstanceOf(List.class);
    assertThat((List<?>) v).hasSize(2);
    assertThat(((List<?>) v).get(1)).isEqualTo("x");
  }

  @Test
  void concatenatingAStringWithADocumentIsRefused() {
    final Document[] doc = new Document[1];
    database.transaction(() -> {
      final MutableDocument d = database.newDocument("Doc");
      d.set("name", "Carol");
      d.save();
      doc[0] = d;
    });
    final Document loaded = doc[0].getIdentity().asDocument();
    assertThatThrownBy(() -> database.query("opencypher", "RETURN 'd:' + $d AS v", Map.of("d", loaded)).next())
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("but was Map");
  }

  /** The controls: strings, numbers, booleans, a temporal value and a RID keep concatenating. */
  @Test
  void concatenatingScalarsIsUntouched() {
    assertThat(scalar("RETURN 'a' + 'b' AS v")).isEqualTo("ab");
    assertThat(scalar("RETURN 'a' + 1 AS v")).isEqualTo("a1");
    assertThat(scalar("RETURN 1.5 + 'a' AS v")).isEqualTo("1.5a");
    assertThat(scalar("RETURN 'a' + true AS v")).isEqualTo("atrue");
    assertThat(scalar("MATCH (a:Person {name:'Alice'}) RETURN 'x:' + a.name AS v")).isEqualTo("x:Alice");
    assertThat(scalar("RETURN 'd:' + date('2020-01-02') AS v")).startsWith("d:2020-01-02");
    final RID rid = new RID(1, 0);
    assertThat((String) database.query("opencypher", "RETURN 'r:' + $rid AS v", Map.of("rid", rid)).next().getProperty("v"))
        .isEqualTo("r:" + rid);
  }

  private void assertRefused(final String query, final String typeName) {
    assertThatThrownBy(() -> {
      final ResultSet rs = database.query("opencypher", query);
      while (rs.hasNext())
        rs.next();
    })
        .isInstanceOf(TypeMismatchException.class)
        .hasMessageContaining("but was " + typeName);
  }

  private String scalar(final String query) {
    final ResultSet rs = database.query("opencypher", query);
    assertThat(rs.hasNext()).as("no row for: %s", query).isTrue();
    return (String) rs.next().getProperty("v");
  }
}
