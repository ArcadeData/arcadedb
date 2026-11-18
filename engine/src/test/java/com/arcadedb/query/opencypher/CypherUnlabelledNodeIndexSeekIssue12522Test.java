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
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.FileUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regression test of issue #12522: a node pattern with no label names no type, so the index of the property it
 * is pinned on could not be used and the statement walked every vertex of every type. The index of the type that
 * owns the property answers it instead, and only the types without such an index are walked.
 */
class CypherUnlabelledNodeIndexSeekIssue12522Test extends TestHelper {
  private static final String DB_PATH = "target/test-databases/Issue12522UnlabelledNodeIndexSeek";

  @BeforeEach
  void setUp() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  @AfterEach
  void tearDown() {
    FileUtils.deleteRecursively(new File(DB_PATH));
  }

  private Database open() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH);
    final Database database = factory.create();
    database.transaction(() -> {
      final var entity = database.getSchema().createVertexType("Entity");
      entity.createProperty("uuid", String.class);
      entity.createProperty("name", String.class);
      entity.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uuid"); // UNIQUE

      final var other = database.getSchema().createVertexType("Other");
      other.createProperty("uuid", String.class);
      other.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");

      // a type with no index at all: its vertices have to be walked
      final var plain = database.getSchema().createVertexType("Plain");
      plain.createProperty("uuid", String.class);

      // a hierarchy: the index of the parent holds the vertices of the child too
      final var base = database.getSchema().createVertexType("Base");
      base.createProperty("uuid", String.class);
      base.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      final var sub = database.getSchema().createVertexType("Sub");
      sub.addSuperType(base);
      sub.createProperty("extra", String.class);
    });
    database.transaction(() -> {
      database.command("opencypher", "CREATE (e:Entity {uuid: 'entity-1', name: 'one'})");
      database.command("opencypher", "CREATE (e:Entity {uuid: 'entity-2', name: 'two'})");
      database.command("opencypher", "CREATE (o:Other {uuid: 'other-1'})");
      database.command("opencypher", "CREATE (p:Plain {uuid: 'plain-1'})");
      database.command("opencypher", "CREATE (b:Base {uuid: 'base-1'})");
      database.command("opencypher", "CREATE (s:Sub {uuid: 'sub-1', extra: 'x'})");
    });
    return database;
  }

  /**
   * The plan of the statement, as the PROFILE run printed it: the name of the index the step used, when it used one.
   */
  private String profilePlan(final Database database, final String cypher, final Map<String, Object> params) {
    final StringBuilder plan = new StringBuilder();
    database.transaction(() -> {
      final ResultSet rs = params != null ?
          database.command("opencypher", "PROFILE " + cypher, params) :
          database.command("opencypher", "PROFILE " + cypher);
      while (rs.hasNext())
        rs.next();
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });
    return plan.toString();
  }

  private List<String> uuidsOf(final Database database, final String cypher, final Map<String, Object> params) {
    final List<String> out = new ArrayList<>();
    database.transaction(() -> {
      final ResultSet rs = params != null ?
          database.command("opencypher", cypher, params) :
          database.command("opencypher", cypher);
      while (rs.hasNext()) {
        final Result row = rs.next();
        out.add(row.getProperty("uuid"));
      }
      rs.close();
    });
    return out;
  }

  @Test
  void unlabelledNodePinnedByAnIndexedPropertyUsesTheIndex() {
    try (final Database database = open()) {
      final Map<String, Object> params = new HashMap<>();
      params.put("u", "entity-1");

      final String plan = profilePlan(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params);

      assertThat(plan)
          .as("an unlabelled node pinned by an indexed property must use the index of the type that owns it\n%s", plan)
          .contains("Entity[uuid]");
    }
  }

  @Test
  void unlabelledNodeStillAnswersTheRightVertex() {
    try (final Database database = open()) {
      final Map<String, Object> params = new HashMap<>();
      params.put("u", "entity-2");

      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params)).containsExactly("entity-2");
    }
  }

  @Test
  void everyTypeWithAnIndexIsAnsweredByItsOwnIndex() {
    try (final Database database = open()) {
      final Map<String, Object> params = new HashMap<>();
      params.put("u", "other-1");

      final String plan = profilePlan(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params);

      assertThat(plan)
          .as("the index of the second type has to be used as well\n%s", plan)
          .contains("Other[uuid]");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params)).containsExactly("other-1");
    }
  }

  @Test
  void aTypeWithoutAnIndexIsStillWalked() {
    try (final Database database = open()) {
      final Map<String, Object> params = new HashMap<>();
      params.put("u", "plain-1");

      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params)).containsExactly("plain-1");
    }
  }

  @Test
  void aSubTypeIsAnsweredByTheIndexOfItsParentAndOnlyOnce() {
    try (final Database database = open()) {
      final Map<String, Object> params = new HashMap<>();
      params.put("u", "sub-1");

      // the index of the parent holds the vertices of the child too: the child must not be answered twice
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params)).containsExactly("sub-1");

      final Map<String, Object> parent = new HashMap<>();
      parent.put("u", "base-1");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", parent)).containsExactly("base-1");
    }
  }

  @Test
  void aValueNoVertexHasAnswersNoRow() {
    try (final Database database = open()) {
      final Map<String, Object> params = new HashMap<>();
      params.put("u", "nobody");

      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params)).isEmpty();
    }
  }

  @Test
  void aValueTheRowDecidesIsSeekedAgainForEveryRow() {
    try (final Database database = open()) {
      // the Graphiti shape: an UNWIND of the keys, each matched by an unlabelled node
      final Map<String, Object> params = new HashMap<>();
      params.put("keys", List.of("entity-1", "other-1", "plain-1", "sub-1"));

      final String plan = profilePlan(database, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params);

      assertThat(plan)
          .as("a value the row decides is answered by an index seek too, made again for every row\n%s", plan)
          .contains("Entity[uuid]");
      assertThat(uuidsOf(database, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params))
          .containsExactlyInAnyOrder("entity-1", "other-1", "plain-1", "sub-1");
    }
  }

  @Test
  void theLabelledFormIsUnchanged() {
    try (final Database database = open()) {
      final Map<String, Object> params = new HashMap<>();
      params.put("u", "entity-1");

      final String plan = profilePlan(database, "MATCH (n:Entity {uuid: $u}) RETURN n.uuid AS uuid", params);

      assertThat(plan).contains("Entity[uuid]");
      assertThat(uuidsOf(database, "MATCH (n:Entity {uuid: $u}) RETURN n.uuid AS uuid", params)).containsExactly("entity-1");
    }
  }
}
