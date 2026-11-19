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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
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
    for (final String path : paths())
      FileUtils.deleteRecursively(new File(path));
  }

  @AfterEach
  void tearDown() {
    for (final String path : paths())
      FileUtils.deleteRecursively(new File(path));
  }

  private static List<String> paths() {
    return List.of(DB_PATH, DB_PATH + "-noindex", DB_PATH + "-diamond", DB_PATH + "-mixed", DB_PATH + "-undeclared",
        DB_PATH + "-multilabel");
  }

  /**
   * The schema of the main tests: every vertex type has an index on {@code uuid}, except the ones that inherit it
   * from their parent. The types of the hierarchy are there so that the answers of a type under another one are
   * checked as well.
   */
  private Database open(final String path, final boolean withIndexes) {
    final DatabaseFactory factory = new DatabaseFactory(path);
    final Database database = factory.create();
    database.transaction(() -> {
      final var entity = database.getSchema().createVertexType("Entity");
      entity.createProperty("uuid", String.class);
      entity.createProperty("name", String.class);

      final var other = database.getSchema().createVertexType("Other");
      other.createProperty("uuid", String.class);

      final var plain = database.getSchema().createVertexType("Plain");
      plain.createProperty("uuid", String.class);

      // a numeric property, to tell the key of an index from the comparison of a walk
      final var numeric = database.getSchema().createVertexType("Numeric");
      numeric.createProperty("id", Long.class);
      numeric.createProperty("uuid", String.class);

      // a hierarchy: the index of the parent holds the vertices of the child too
      final var base = database.getSchema().createVertexType("Base");
      base.createProperty("uuid", String.class);
      final var mid = database.getSchema().createVertexType("Mid");
      mid.addSuperType(base);
      final var leaf = database.getSchema().createVertexType("Leaf");
      leaf.addSuperType(mid);

      // a type under a type that has an index of its own
      final var own = database.getSchema().createVertexType("Own");
      own.createProperty("uuid", String.class);
      final var underOwn = database.getSchema().createVertexType("UnderOwn");
      underOwn.addSuperType(own);

      if (withIndexes) {
        entity.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uuid");
        other.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
        plain.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
        base.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
        own.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
        numeric.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
        numeric.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "id");
      }
    });
    database.transaction(() -> {
      database.command("opencypher", "CREATE (e:Entity {uuid: 'entity-1', name: 'one'})");
      database.command("opencypher", "CREATE (e:Entity {uuid: 'entity-2', name: 'two'})");
      database.command("opencypher", "CREATE (o:Other {uuid: 'other-1'})");
      database.command("opencypher", "CREATE (p:Plain {uuid: 'plain-1'})");
      database.command("opencypher", "CREATE (n:Numeric {id: 1, uuid: 'numeric-1'})");
      database.command("opencypher", "CREATE (b:Base {uuid: 'base-1'})");
      database.command("opencypher", "CREATE (m:Mid {uuid: 'mid-1'})");
      database.command("opencypher", "CREATE (l:Leaf {uuid: 'leaf-1'})");
      database.command("opencypher", "CREATE (o:Own {uuid: 'own-1'})");
      database.command("opencypher", "CREATE (u:UnderOwn {uuid: 'under-own-1'})");
    });
    return database;
  }

  private Database open() {
    return open(DB_PATH, true);
  }

  /**
   * A schema with a type under two types: the two branches can each hold an index over the property, and each of
   * them holds the vertices of their common sub type.
   */
  private Database openDiamond() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH + "-diamond");
    final Database database = factory.create();
    database.transaction(() -> {
      final var left = database.getSchema().createVertexType("Left");
      left.createProperty("uuid", String.class);
      left.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      final var right = database.getSchema().createVertexType("Right");
      right.createProperty("uuid", String.class);
      right.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      final var diamond = database.getSchema().createVertexType("Diamond");
      diamond.addSuperType(left);
      diamond.addSuperType(right);
    });
    database.transaction(() -> {
      database.command("opencypher", "CREATE (l:Left {uuid: 'left-1'})");
      database.command("opencypher", "CREATE (r:Right {uuid: 'right-1'})");
      database.command("opencypher", "CREATE (d:Diamond {uuid: 'diamond-1'})");
    });
    return database;
  }

  /**
   * A schema whose index stands on a property the type does not declare, the way the graphiti benchmark builds it:
   * the type of the key is then the one of the index, not the one of the schema.
   */
  private Database openUndeclaredProperty() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH + "-undeclared");
    final Database database = factory.create();
    database.transaction(() -> {
      // the index is created on a type that does not exist yet, the way the graphiti benchmark creates it: the
      // type comes with the index, and the property is not declared
      database.command("opencypher", "CREATE INDEX entity_uuid IF NOT EXISTS FOR (n:Entity) ON (n.uuid)");
    });
    assertThat(database.getSchema().getType("Entity").getPropertyIfExists("uuid"))
        .as("the property of the index is not declared")
        .isNull();
    database.transaction(() -> database.command("opencypher", "CREATE (e:Entity {uuid: 'entity-1'})"));
    return database;
  }

  /**
   * A schema with a multi-label node: openCypher creates a composite type for it, and that type has two super
   * types, so the fix keeps the walk of the whole database for the database.
   */
  private Database openMultiLabel() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH + "-multilabel");
    final Database database = factory.create();
    database.transaction(() -> {
      final var a = database.getSchema().createVertexType("A");
      a.createProperty("uuid", String.class);
      final var b = database.getSchema().createVertexType("B");
      b.createProperty("uuid", String.class);
      a.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      b.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
    });
    database.transaction(() -> database.command("opencypher", "CREATE (n:A:B {uuid: 'multi-1'})"));
    return database;
  }

  /**
   * A schema with one type that has an index and one that has not: the type without an index has to be walked, and
   * a value the row decides makes that walk be repeated for every row, where it used to be walked once and cached.
   */
  private Database openMixed() {
    final DatabaseFactory factory = new DatabaseFactory(DB_PATH + "-mixed");
    final Database database = factory.create();
    database.transaction(() -> {
      final var entity = database.getSchema().createVertexType("Entity");
      entity.createProperty("uuid", String.class);
      entity.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uuid");
      final var plain = database.getSchema().createVertexType("Plain");
      plain.createProperty("uuid", String.class);
    });
    database.transaction(() -> {
      database.command("opencypher", "CREATE (e:Entity {uuid: 'entity-1'})");
      database.command("opencypher", "CREATE (p:Plain {uuid: 'plain-1'})");
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

  private Map<String, Object> params(final Object value) {
    final Map<String, Object> params = new HashMap<>();
    params.put("u", value);
    return params;
  }

  @Test
  void unlabelledNodePinnedByAnIndexedPropertyUsesTheIndex() {
    try (final Database database = open()) {
      final String plan = profilePlan(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("entity-1"));

      assertThat(plan)
          .as("an unlabelled node pinned by an indexed property must use the index of the type that owns it\n%s", plan)
          .contains("Entity[uuid]");
    }
  }

  @Test
  void unlabelledNodeStillAnswersTheRightVertex() {
    try (final Database database = open()) {
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("entity-2"))).containsExactly("entity-2");
    }
  }

  @Test
  void everyTypeWithAnIndexIsAnsweredByItsOwnIndex() {
    try (final Database database = open()) {
      final String plan = profilePlan(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("other-1"));

      assertThat(plan)
          .as("the index of the second type has to be used as well\n%s", plan)
          .contains("Other[uuid]");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("other-1"))).containsExactly("other-1");
    }
  }

  @Test
  void aTypeWithoutAnIndexIsStillWalked() {
    try (final Database database = open(DB_PATH + "-noindex", false)) {
      final String plan = profilePlan(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("entity-1"));

      assertThat(plan)
          .as("without an index there is nothing to seek\n%s", plan)
          .doesNotContain("[index:");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("entity-1"))).containsExactly("entity-1");
    }
  }

  @Test
  void aSubTypeIsAnsweredByTheIndexOfItsParentAndOnlyOnce() {
    try (final Database database = open()) {
      // the index of the parent holds the vertices of the child too: the child must not be answered twice
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("mid-1"))).containsExactly("mid-1");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("base-1"))).containsExactly("base-1");
    }
  }

  @Test
  void aGrandChildIsAnsweredOnce() {
    try (final Database database = open()) {
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("leaf-1"))).containsExactly("leaf-1");
    }
  }

  @Test
  void aTypeWithItsOwnIndexUnderATypeWithOneIsAnsweredOnce() {
    try (final Database database = open()) {
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("own-1"))).containsExactly("own-1");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("under-own-1"))).containsExactly("under-own-1");
    }
  }

  @Test
  void aTypeUnderTwoTypesIsAnsweredOnce() {
    try (final Database database = openDiamond()) {
      // the two branches of the diamond can each hold an index over the property, and each of them holds the
      // vertices of their common sub type: the sub type must not be answered twice
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("diamond-1"))).containsExactly("diamond-1");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("left-1"))).containsExactly("left-1");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("right-1"))).containsExactly("right-1");
    }
  }

  @Test
  void anIndexOnAnUndeclaredPropertyIsUsed() {
    // the index stands on a property the type does not declare: the type of the key is the one of the index
    try (final Database database = openUndeclaredProperty()) {
      final String plan = profilePlan(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("entity-1"));

      assertThat(plan)
          .as("an index on an undeclared property answers the pattern too\n%s", plan)
          .contains("Entity[uuid]");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("entity-1")))
          .containsExactly("entity-1");
    }
  }

  @Test
  void aMultiLabelNodeKeepsTheWalkOfTheWholeDatabase() {
    // openCypher creates a composite type for a node with two labels, and that type has two super types: the
    // two indexes of the branches would answer the vertices of the composite type twice, so the walk of the whole
    // database answers the statement
    try (final Database database = openMultiLabel()) {
      final String plan = profilePlan(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("multi-1"));

      assertThat(plan)
          .as("a database with a multi-label node keeps the walk of the whole database\n%s", plan)
          .doesNotContain("[index:");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("multi-1"))).containsExactly("multi-1");
    }
  }

  @Test
  void aValueNoVertexHasAnswersNoRow() {
    try (final Database database = open()) {
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("nobody"))).isEmpty();
    }
  }

  @Test
  void aValueTheRowDecidesIsSeekedAgainForEveryRow() {
    try (final Database database = open()) {
      // the Graphiti shape: an UNWIND of the keys, each matched by an unlabelled node
      final Map<String, Object> params = new HashMap<>();
      params.put("keys", List.of("entity-1", "other-1", "plain-1", "mid-1"));

      final String plan = profilePlan(database, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params);

      assertThat(plan)
          .as("a value the row decides is answered by an index seek too, made again for every row\n%s", plan)
          .contains("Entity[uuid]");
      assertThat(uuidsOf(database, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params))
          .containsExactlyInAnyOrder("entity-1", "other-1", "plain-1", "mid-1");
    }
  }

  @Test
  void aValueTheRowDecidesWithATypeWithoutAnIndexIsAnsweredByTheWalk() {
    // the walk of the types without an index would be repeated for every row, where it used to be walked once and
    // cached (issues #6602, #8695): the walk of the whole database answers the statement instead
    try (final Database database = openMixed()) {
      final Map<String, Object> params = new HashMap<>();
      params.put("keys", List.of("entity-1", "plain-1"));

      final String plan = profilePlan(database, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params);

      assertThat(plan)
          .as("a type without an index keeps the walk of the whole database\n%s", plan)
          .doesNotContain("[index:");
      assertThat(uuidsOf(database, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params))
          .containsExactlyInAnyOrder("entity-1", "plain-1");
    }
  }

  @Test
  void theIndexPathAnswersWhatTheWalkAnswers() {
    // the same statement over a database with the index and over one without it: the rows have to be the same
    try (final Database withIndex = open(DB_PATH, true); final Database withoutIndex = open(DB_PATH + "-noindex", false)) {
      final Object[] values = { 1, 1L, 1.0d, 1.5d, "1", Boolean.TRUE };
      for (final Object value : values) {
        assertThat(uuidsOf(withIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(value)))
            .as("the index path and the walk have to answer the same rows for %s", value)
            .isEqualTo(uuidsOf(withoutIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(value)));
      }
    }
  }

  @Test
  void theTextOfARIDIsAnsweredByTheWalkAndNotByTheIndex() {
    try (final Database withIndex = open(DB_PATH, true); final Database withoutIndex = open(DB_PATH + "-noindex", false)) {
      // a numeric property and the text of a RID: the walk answers it by the id(), the index cannot key it
      // (issue #11933)
      final RID rid = ridOf(withIndex, "numeric-1");
      final RID otherRid = ridOf(withoutIndex, "numeric-1");

      assertThat(uuidsOf(withIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(rid.toString())))
          .isEqualTo(uuidsOf(withoutIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(otherRid.toString())));
    }
  }

  @Test
  void aTextValueOnANumberKeyIsAnsweredByTheWalk() {
    // the walk converts the text to the declared LONG of the property and finds the vertex: the index, keyed by
    // numbers, must not be asked with the text and answer nothing instead
    try (final Database database = open()) {
      final String plan = profilePlan(database, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params("1"));

      assertThat(plan)
          .as("a text value on a number key is answered by the walk\n%s", plan)
          .doesNotContain("[index:");
      assertThat(uuidsOf(database, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params("1"))).containsExactly("numeric-1");
    }
  }

  @Test
  void aFloatingPointValueOnAnIntegralKeyIsAnsweredByTheWalk() {
    // the walk compares a double with a LONG property by its double value, so 9007199254740992d matches the stored
    // 9007199254740993, which the key lookup of 9007199254740992 would not find
    try (final Database withIndex = open(DB_PATH, true); final Database withoutIndex = open(DB_PATH + "-noindex", false)) {
      for (final Database database : List.of(withIndex, withoutIndex))
        database.transaction(() -> database.command("opencypher", "CREATE (n:Numeric {id: $id, uuid: 'big-1'})",
            Map.of("id", 9007199254740993L)));

      final String plan = profilePlan(withIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(9007199254740992d));
      assertThat(plan)
          .as("a double on a LONG key is answered by the walk\n%s", plan)
          .doesNotContain("Numeric[id]");

      final List<String> walked = uuidsOf(withoutIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(9007199254740992d));
      assertThat(uuidsOf(withIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(9007199254740992d)))
          .isEqualTo(walked);

      // a long keeps the exact seek
      final String longPlan = profilePlan(withIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(9007199254740993L));
      assertThat(longPlan)
          .as("a long on a LONG key is answered by the index\n%s", longPlan)
          .contains("Numeric[id]");
      assertThat(uuidsOf(withIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(9007199254740993L)))
          .containsExactly("big-1");
    }
  }

  @Test
  void aLimitSatisfiedByTheFirstTypeDoesNotSeekTheOthers() {
    // the types are opened when the caller asks for their first row: a LIMIT a type answers with rows to spare does
    // not seek the index of the types after it
    try (final Database database = open()) {
      database.transaction(() -> {
        database.command("opencypher", "CREATE (n:Numeric {uuid: 'dup'})");
        database.command("opencypher", "CREATE (n:Numeric {uuid: 'dup'})");
        database.command("opencypher", "CREATE (o:Other {uuid: 'dup'})");
      });

      final String plan = profilePlan(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid LIMIT 1", params("dup"));

      assertThat(plan)
          .as("the first type with rows answers the LIMIT\n%s", plan)
          .contains("Numeric[uuid]")
          .doesNotContain("Other[uuid]");
      assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid", params("dup")))
          .containsExactly("dup", "dup", "dup");
    }
  }

  private RID ridOf(final Database database, final String uuid) {
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final ResultSet rs = database.command("opencypher", "MATCH (n {uuid: $u}) RETURN n AS n", params(uuid));
      while (rs.hasNext()) {
        final Object n = rs.next().getProperty("n");
        if (n instanceof Result r)
          rid[0] = r.getElement().map(Document::getIdentity).orElse(null);
        else if (n instanceof Document d)
          rid[0] = d.getIdentity();
      }
      rs.close();
    });
    return rid[0];
  }
}
