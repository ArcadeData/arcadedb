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
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.VertexType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The regression test of issue #12522: a node pattern with no label names no type, so the index of the property it
 * is pinned on could not be used and the statement walked every vertex of every type. Every vertex bucket is now
 * answered by exactly one source: the index that covers it, or a walk of the bucket when no index does. The type
 * hierarchy does not matter to that: a type under two types (a diamond, which is what a node with two labels
 * creates) is answered once, and by an index.
 */
class CypherUnlabelledNodeIndexSeekIssue12522Test extends TestHelper {
  private static final String PATH_PREFIX = "target/databases/Issue12522-";

  private final List<Database> extraDatabases = new ArrayList<>();

  @Override
  protected void endTest() {
    for (final Database extra : extraDatabases)
      if (extra.isOpen()) {
        while (extra.isTransactionActive())
          extra.rollback();
        extra.drop();
      }
    extraDatabases.clear();
  }

  private Database newDatabase(final String name) {
    final Database extra = createDatabase(PATH_PREFIX + name);
    extraDatabases.add(extra);
    return extra;
  }

  /**
   * The schema of the main tests: every vertex type has an index on {@code uuid}, or inherits the one of its parent.
   * The types of the hierarchy are there so that the answers of a type under another one are checked as well.
   */
  private void buildMain(final Database db, final boolean withIndexes) {
    db.transaction(() -> {
      final VertexType entity = db.getSchema().createVertexType("Entity");
      entity.createProperty("uuid", String.class);
      entity.createProperty("name", String.class);

      final VertexType other = db.getSchema().createVertexType("Other");
      other.createProperty("uuid", String.class);

      final VertexType plain = db.getSchema().createVertexType("Plain");
      plain.createProperty("uuid", String.class);

      // a numeric property, to tell the key of an index from the comparison of a walk
      final VertexType numeric = db.getSchema().createVertexType("Numeric");
      numeric.createProperty("id", Long.class);
      numeric.createProperty("uuid", String.class);

      // a hierarchy: the index of the parent holds the vertices of the child too
      final VertexType base = db.getSchema().createVertexType("Base");
      base.createProperty("uuid", String.class);
      final VertexType mid = db.getSchema().createVertexType("Mid");
      mid.addSuperType(base);
      final VertexType leaf = db.getSchema().createVertexType("Leaf");
      leaf.addSuperType(mid);

      // a type under a type that has an index of its own
      final VertexType own = db.getSchema().createVertexType("Own");
      own.createProperty("uuid", String.class);
      final VertexType underOwn = db.getSchema().createVertexType("UnderOwn");
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
    db.transaction(() -> {
      db.command("opencypher", "CREATE (e:Entity {uuid: 'entity-1', name: 'one'})");
      db.command("opencypher", "CREATE (e:Entity {uuid: 'entity-2', name: 'two'})");
      db.command("opencypher", "CREATE (o:Other {uuid: 'other-1'})");
      db.command("opencypher", "CREATE (p:Plain {uuid: 'plain-1'})");
      db.command("opencypher", "CREATE (n:Numeric {id: 1, uuid: 'numeric-1'})");
      db.command("opencypher", "CREATE (b:Base {uuid: 'base-1'})");
      db.command("opencypher", "CREATE (m:Mid {uuid: 'mid-1'})");
      db.command("opencypher", "CREATE (l:Leaf {uuid: 'leaf-1'})");
      db.command("opencypher", "CREATE (o:Own {uuid: 'own-1'})");
      db.command("opencypher", "CREATE (u:UnderOwn {uuid: 'under-own-1'})");
    });
  }

  /**
   * A type under two types: the two branches can each hold an index over the property, and each of them holds the
   * vertices of their common sub type.
   */
  private Database newDiamond(final boolean leftIndexed, final boolean rightIndexed) {
    final Database db = newDatabase("diamond-" + leftIndexed + "-" + rightIndexed);
    db.transaction(() -> {
      final VertexType left = db.getSchema().createVertexType("Left");
      left.createProperty("uuid", String.class);
      if (leftIndexed)
        left.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      final VertexType right = db.getSchema().createVertexType("Right");
      right.createProperty("uuid", String.class);
      if (rightIndexed)
        right.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      final VertexType diamond = db.getSchema().createVertexType("Diamond");
      diamond.addSuperType(left);
      diamond.addSuperType(right);
    });
    db.transaction(() -> {
      db.command("opencypher", "CREATE (l:Left {uuid: 'left-1'})");
      db.command("opencypher", "CREATE (r:Right {uuid: 'right-1'})");
      db.command("opencypher", "CREATE (d:Diamond {uuid: 'diamond-1'})");
    });
    return db;
  }

  /**
   * A schema whose index stands on a property the type does not declare, the way the graphiti benchmark builds it:
   * the type of the key is then the one of the index, not the one of the schema.
   */
  private Database newUndeclaredProperty() {
    final Database db = newDatabase("undeclared");
    // the index is created on a type that does not exist yet, the way the graphiti benchmark creates it: the type
    // comes with the index, and the property is not declared
    db.transaction(() -> db.command("opencypher", "CREATE INDEX entity_uuid IF NOT EXISTS FOR (n:Entity) ON (n.uuid)"));
    assertThat(db.getSchema().getType("Entity").getPropertyIfExists("uuid")).as("the property of the index is not declared").isNull();
    db.transaction(() -> db.command("opencypher", "CREATE (e:Entity {uuid: 'entity-1'})"));
    return db;
  }

  /**
   * Two labels on one node: openCypher creates a composite type for it, which has the two types as super types, and
   * both of them have an index on the property. This is the shape the entities of a Graphiti graph take.
   */
  private Database newMultiLabel() {
    final Database db = newDatabase("multilabel");
    db.transaction(() -> {
      final VertexType a = db.getSchema().createVertexType("A");
      a.createProperty("uuid", String.class);
      final VertexType b = db.getSchema().createVertexType("B");
      b.createProperty("uuid", String.class);
      a.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      b.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
    });
    db.transaction(() -> {
      db.command("opencypher", "CREATE (n:A:B {uuid: 'multi-1'})");
      db.command("opencypher", "CREATE (n:A {uuid: 'a-1'})");
      db.command("opencypher", "CREATE (n:B {uuid: 'b-1'})");
    });
    return db;
  }

  /**
   * One type that has an index, one that has not, and one that has none and holds nothing.
   */
  private Database newMixed() {
    final Database db = newDatabase("mixed");
    db.transaction(() -> {
      final VertexType entity = db.getSchema().createVertexType("Entity");
      entity.createProperty("uuid", String.class);
      entity.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uuid");
      final VertexType plain = db.getSchema().createVertexType("Plain");
      plain.createProperty("uuid", String.class);
      final VertexType unused = db.getSchema().createVertexType("Unused");
      unused.createProperty("uuid", String.class);
    });
    db.transaction(() -> {
      db.command("opencypher", "CREATE (e:Entity {uuid: 'entity-1'})");
      db.command("opencypher", "CREATE (p:Plain {uuid: 'plain-1'})");
    });
    return db;
  }

  /**
   * The plan of the statement, as the PROFILE run printed it: the name of the index the step used, when it used one.
   */
  private String profilePlan(final Database db, final String cypher, final Map<String, Object> params) {
    final StringBuilder plan = new StringBuilder();
    db.transaction(() -> {
      final ResultSet rs = params != null ? db.command("opencypher", "PROFILE " + cypher, params) : db.command("opencypher", "PROFILE " + cypher);
      while (rs.hasNext())
        rs.next();
      plan.append(rs.getExecutionPlan().orElseThrow().prettyPrint(0, 2));
      rs.close();
    });
    return plan.toString();
  }

  private List<String> uuidsOf(final Database db, final String cypher, final Map<String, Object> params) {
    final List<String> out = new ArrayList<>();
    db.transaction(() -> {
      final ResultSet rs = params != null ? db.command("opencypher", cypher, params) : db.command("opencypher", cypher);
      while (rs.hasNext()) {
        final Result row = rs.next();
        out.add(row.getProperty("uuid"));
      }
      rs.close();
    });
    return out;
  }

  private static Map<String, Object> params(final Object value) {
    final Map<String, Object> params = new HashMap<>();
    params.put("u", value);
    return params;
  }

  private static final String BY_UUID = "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid";

  @Test
  void unlabelledNodePinnedByAnIndexedPropertyUsesTheIndex() {
    buildMain(database, true);

    final String plan = profilePlan(database, BY_UUID, params("entity-1"));

    assertThat(plan).as("an unlabelled node pinned by an indexed property must use the index of the type that owns it\n%s", plan)
        .contains("Entity[uuid]");
  }

  @Test
  void unlabelledNodeStillAnswersTheRightVertex() {
    buildMain(database, true);

    assertThat(uuidsOf(database, BY_UUID, params("entity-2"))).containsExactly("entity-2");
  }

  @Test
  void everyTypeWithAnIndexIsAnsweredByItsOwnIndex() {
    buildMain(database, true);

    final String plan = profilePlan(database, BY_UUID, params("other-1"));

    assertThat(plan).as("the index of the second type has to be used as well\n%s", plan).contains("Other[uuid]");
    assertThat(uuidsOf(database, BY_UUID, params("other-1"))).containsExactly("other-1");
  }

  @Test
  void aTypeWithoutAnIndexIsStillWalked() {
    buildMain(database, false);

    final String plan = profilePlan(database, BY_UUID, params("entity-1"));

    assertThat(plan).as("without an index there is nothing to seek\n%s", plan).doesNotContain("[index:");
    assertThat(uuidsOf(database, BY_UUID, params("entity-1"))).containsExactly("entity-1");
  }

  @Test
  void aTypeWithoutAnIndexIsWalkedBesideTheTypesThatHaveOne() {
    final Database db = newMixed();

    final String plan = profilePlan(db, BY_UUID, params("entity-1"));

    assertThat(plan).as("the type that has the index is answered by it\n%s", plan).contains("Entity[uuid]");
    assertThat(uuidsOf(db, BY_UUID, params("entity-1"))).containsExactly("entity-1");
    assertThat(uuidsOf(db, BY_UUID, params("plain-1"))).as("the type with no index is walked").containsExactly("plain-1");
  }

  @Test
  void aSubTypeIsAnsweredByTheIndexOfItsParentAndOnlyOnce() {
    buildMain(database, true);

    // the index of the parent holds the vertices of the child too: the child must not be answered twice
    assertThat(uuidsOf(database, BY_UUID, params("mid-1"))).containsExactly("mid-1");
    assertThat(uuidsOf(database, BY_UUID, params("base-1"))).containsExactly("base-1");
  }

  @Test
  void aGrandChildIsAnsweredOnce() {
    buildMain(database, true);

    assertThat(uuidsOf(database, BY_UUID, params("leaf-1"))).containsExactly("leaf-1");
  }

  @Test
  void aTypeWithItsOwnIndexUnderATypeWithOneIsAnsweredOnce() {
    buildMain(database, true);

    assertThat(uuidsOf(database, BY_UUID, params("own-1"))).containsExactly("own-1");
    assertThat(uuidsOf(database, BY_UUID, params("under-own-1"))).containsExactly("under-own-1");
  }

  @Test
  void aTypeUnderTwoTypesIsAnsweredOnceAndByAnIndex() {
    final Database db = newDiamond(true, true);

    // the two branches of the diamond each hold an index over the property, and each of them holds the vertices of
    // their common sub type: the sub type must be answered once, and no walk of the database is needed for it
    assertThat(uuidsOf(db, BY_UUID, params("diamond-1"))).containsExactly("diamond-1");
    assertThat(uuidsOf(db, BY_UUID, params("left-1"))).containsExactly("left-1");
    assertThat(uuidsOf(db, BY_UUID, params("right-1"))).containsExactly("right-1");
    final String plan = profilePlan(db, BY_UUID, params("diamond-1"));
    assertThat(plan).as("a database with a type under two types is answered by the indexes\n%s", plan).contains("[index:");
  }

  @Test
  void aTypeUnderTwoTypesWithAnIndexOnOneBranchOnlyIsAnsweredOnce() {
    for (final boolean leftIndexed : new boolean[] { true, false }) {
      final Database db = newDiamond(leftIndexed, !leftIndexed);

      // the common sub type is answered by the index of the one branch that has it, and the other branch is walked
      assertThat(uuidsOf(db, BY_UUID, params("diamond-1"))).as("left indexed: %s", leftIndexed).containsExactly("diamond-1");
      assertThat(uuidsOf(db, BY_UUID, params("left-1"))).as("left indexed: %s", leftIndexed).containsExactly("left-1");
      assertThat(uuidsOf(db, BY_UUID, params("right-1"))).as("left indexed: %s", leftIndexed).containsExactly("right-1");
      final String plan = profilePlan(db, BY_UUID, params("diamond-1"));
      assertThat(plan).as("the index of the branch that has one answers\n%s", plan).contains(leftIndexed ? "Left[uuid]" : "Right[uuid]");
      extraDatabases.remove(db);
      db.drop();
    }
  }

  @Test
  void aNodeWithTwoLabelsIsAnsweredOnceAndByAnIndex() {
    // openCypher creates a composite type for a node with two labels, a type under both: that is the shape of every
    // entity of a Graphiti graph, and the walk of the whole database must not be the price of having one
    final Database db = newMultiLabel();

    assertThat(uuidsOf(db, BY_UUID, params("multi-1"))).containsExactly("multi-1");
    assertThat(uuidsOf(db, BY_UUID, params("a-1"))).containsExactly("a-1");
    assertThat(uuidsOf(db, BY_UUID, params("b-1"))).containsExactly("b-1");
    final String plan = profilePlan(db, BY_UUID, params("multi-1"));
    assertThat(plan).as("a database with a multi-label node is answered by the indexes\n%s", plan).contains("[index:");
  }

  @Test
  void aPatternWithSeveralLabelsIsAnsweredByTheIndexesOfTheTypesTheyName() {
    // (n:A:B) is the composite type under both, which holds no index of its own: the indexes of A and of B answer for its bucket
    final Database db = newMultiLabel();

    final String both = "MATCH (n:A:B {uuid: $u}) RETURN n.uuid AS uuid";
    assertThat(profilePlan(db, both, params("multi-1"))).contains("[index:");
    assertThat(uuidsOf(db, both, params("multi-1"))).containsExactly("multi-1");
    assertThat(uuidsOf(db, both, params("a-1"))).as("a vertex that only carries A is not an A and B").isEmpty();

    // (n:A|B) is A, B and the composite, and is planned by the cost-based optimizer: the vertex of the composite type is one row
    final String either = "MATCH (n:A|B {uuid: $u}) RETURN n.uuid AS uuid";
    assertThat(uuidsOf(db, either, params("multi-1"))).containsExactly("multi-1");
    assertThat(uuidsOf(db, either, params("a-1"))).containsExactly("a-1");
    assertThat(uuidsOf(db, either, params("b-1"))).containsExactly("b-1");
    assertThat(uuidsOf(db, either, params("nobody"))).isEmpty();

    // a label narrows the types: a vertex of another type with the same value is not an answer
    assertThat(uuidsOf(db, "MATCH (n:A {uuid: $u}) RETURN n.uuid AS uuid", params("b-1"))).isEmpty();
  }

  @Test
  void aValueNoVertexHasAnswersNoRow() {
    buildMain(database, true);

    assertThat(uuidsOf(database, BY_UUID, params("nobody"))).isEmpty();
  }

  @Test
  void aValueTheRowDecidesIsSeekedAgainForEveryRow() {
    buildMain(database, true);
    // the Graphiti shape: an UNWIND of the keys, each matched by an unlabelled node
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", List.of("entity-1", "other-1", "plain-1", "mid-1", "multi-nobody"));

    final String plan = profilePlan(database, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params);

    assertThat(plan).as("a value the row decides is answered by an index seek too, made again for every row\n%s", plan)
        .contains("Entity[uuid]");
    assertThat(uuidsOf(database, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params))
        .containsExactlyInAnyOrder("entity-1", "other-1", "plain-1", "mid-1");
  }

  @Test
  void aRowThatCannotBeSoughtDoesNotLockTheRowsAfterItOutOfTheIndex() {
    buildMain(database, true);

    // the first row is a null, the second a value an index keyed by strings cannot be asked with, the third a key: the walk
    // that answers the first rows is not cached for the rows after them, which are answered by the index
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", Arrays.asList(null, 1.5d, "entity-1", "other-1"));
    final String inline = "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid";
    final String where = "UNWIND $keys AS k MATCH (n) WHERE n.uuid = k RETURN n.uuid AS uuid";

    for (final String statement : List.of(inline, where)) {
      assertThat(profilePlan(database, statement, params)).as(statement).contains("Entity[uuid]");
      assertThat(uuidsOf(database, statement, params)).as(statement).containsExactlyInAnyOrder("entity-1", "other-1");
    }
  }

  @Test
  void aNullValueMatchesNothing() {
    buildMain(database, true);

    assertThat(uuidsOf(database, BY_UUID, params(null))).isEmpty();
    assertThat(uuidsOf(database, "MATCH (n) WHERE n.uuid = $u RETURN n.uuid AS uuid", params(null))).isEmpty();
  }

  @Test
  void aValueTheRowDecidesWithATypeWithoutAnIndexIsAnsweredByTheWalk() {
    // the walk of a type without an index would be repeated for every row, where it used to be walked once and cached
    // (issues #6602, #8695): the walk of the whole database answers the statement instead
    final Database db = newMixed();
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", List.of("entity-1", "plain-1"));

    final String plan = profilePlan(db, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params);

    assertThat(plan).as("a type without an index keeps the walk of the whole database\n%s", plan).doesNotContain("[index:");
    assertThat(uuidsOf(db, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params))
        .containsExactlyInAnyOrder("entity-1", "plain-1");
  }

  @Test
  void aValueTheRowDecidesIsSeekedWhenTheTypesWithoutAnIndexAreEmpty() {
    final Database db = newDatabase("empty-unindexed");
    db.transaction(() -> {
      final VertexType entity = db.getSchema().createVertexType("Entity");
      entity.createProperty("uuid", String.class);
      entity.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uuid");
      db.getSchema().createVertexType("Unused").createProperty("uuid", String.class);
    });
    db.transaction(() -> db.command("opencypher", "CREATE (e:Entity {uuid: 'entity-1'})"));
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", List.of("entity-1", "nobody"));

    final String plan = profilePlan(db, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params);

    assertThat(plan).as("a type that holds nothing is not a reason to walk the database\n%s", plan).contains("Entity[uuid]");
    assertThat(uuidsOf(db, "UNWIND $keys AS k MATCH (n {uuid: k}) RETURN n.uuid AS uuid", params)).containsExactly("entity-1");
  }

  @Test
  void aStatementThatStopsAtTheFirstRowDoesNotSeekTheIndexesAfterIt() {
    // two indexes answer the same value, the first holds more entries than a batch of rows (100), and the first batch is
    // all a LIMIT asks for: the seek of the second one is not made
    final Database db = newDatabase("lazy");
    db.transaction(() -> {
      for (final String name : List.of("First", "Second")) {
        final VertexType type = db.getSchema().createVertexType(name);
        type.createProperty("uuid", String.class);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      }
    });
    db.transaction(() -> {
      db.command("opencypher", "UNWIND range(1, 150) AS i CREATE (a:First {uuid: 'dup'})");
      db.command("opencypher", "CREATE (a:Second {uuid: 'dup'})");
    });

    final String plan = profilePlan(db, "MATCH (n {uuid: $u}) RETURN n.uuid AS uuid LIMIT 1", params("dup"));

    final boolean first = plan.contains("First[uuid]");
    final boolean second = plan.contains("Second[uuid]");
    assertThat(first ^ second).as("exactly one of the two indexes is sought when one batch is enough\n%s", plan).isTrue();
    assertThat(uuidsOf(db, BY_UUID, params("dup"))).as("without a LIMIT every index is sought").hasSize(151);
  }

  @Test
  void anIndexOnAnUndeclaredPropertyIsUsed() {
    // the index stands on a property the type does not declare: the type of the key is the one of the index
    final Database db = newUndeclaredProperty();

    final String plan = profilePlan(db, BY_UUID, params("entity-1"));

    assertThat(plan).as("an index on an undeclared property answers the pattern too\n%s", plan).contains("Entity[uuid]");
    assertThat(uuidsOf(db, BY_UUID, params("entity-1"))).containsExactly("entity-1");
  }

  @Test
  void aCompositeIndexAnswersAPatternThatPinsEveryProperty() {
    final Database db = newDatabase("composite");
    db.transaction(() -> {
      final VertexType pair = db.getSchema().createVertexType("Pair");
      pair.createProperty("a", Long.class);
      pair.createProperty("b", String.class);
      pair.createProperty("uuid", String.class);
      pair.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "a", "b");
    });
    db.transaction(() -> {
      db.command("opencypher", "CREATE (p:Pair {a: 1, b: 'x', uuid: 'p-1x'})");
      db.command("opencypher", "CREATE (p:Pair {a: 1, b: 'y', uuid: 'p-1y'})");
      db.command("opencypher", "CREATE (p:Pair {a: 2, b: 'x', uuid: 'p-2x'})");
    });

    final String both = "MATCH (n {a: 1, b: 'y'}) RETURN n.uuid AS uuid";
    assertThat(profilePlan(db, both, null)).contains("Pair[a, b]");
    assertThat(uuidsOf(db, both, null)).containsExactly("p-1y");
    // a prefix of the key is not an equality on the whole key: the answer is the same, from the walk
    assertThat(uuidsOf(db, "MATCH (n {a: 1}) RETURN n.uuid AS uuid", null)).containsExactlyInAnyOrder("p-1x", "p-1y");
  }

  @Test
  void anEqualityOfTheWhereFilterPinsTheNodeAsTheInlineMapDoes() {
    buildMain(database, true);

    for (final String statement : List.of("MATCH (n) WHERE n.uuid = $u RETURN n.uuid AS uuid",
        "MATCH (n) WHERE $u = n.uuid RETURN n.uuid AS uuid", "MATCH (n) WHERE n.uuid = $u AND n.name = 'two' RETURN n.uuid AS uuid")) {
      final String plan = profilePlan(database, statement, params("entity-2"));
      assertThat(plan).as("%s\n%s", statement, plan).contains("Entity[uuid]");
      assertThat(uuidsOf(database, statement, params("entity-2"))).as(statement).containsExactly("entity-2");
      assertThat(uuidsOf(database, statement, params("nobody"))).as(statement).isEmpty();
    }
    // what the filter keeps still decides the rows the seek returns
    assertThat(uuidsOf(database, "MATCH (n) WHERE n.uuid = $u AND n.name = 'two' RETURN n.uuid AS uuid", params("entity-1"))).isEmpty();
    assertThat(uuidsOf(database, "MATCH (n {uuid: $u}) WHERE n.name = 'one' RETURN n.uuid AS uuid", params("entity-1")))
        .containsExactly("entity-1");
  }

  @Test
  void aWhereFilterThatIsNotAConjunctionOfEqualitiesIsAnsweredByTheWalk() {
    buildMain(database, true);

    final String statement = "MATCH (n) WHERE n.uuid = $u OR n.uuid = 'other-1' RETURN n.uuid AS uuid";
    assertThat(profilePlan(database, statement, params("entity-2"))).doesNotContain("[index:");
    assertThat(uuidsOf(database, statement, params("entity-2"))).containsExactlyInAnyOrder("entity-2", "other-1");
  }

  @Test
  void aWhereEqualityTheRowDecidesIsSeekedAgainForEveryRow() {
    buildMain(database, true);
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", List.of("entity-1", "other-1", "plain-1", "leaf-1", "nobody"));
    final String statement = "UNWIND $keys AS k MATCH (n) WHERE n.uuid = k RETURN n.uuid AS uuid";

    assertThat(profilePlan(database, statement, params)).contains("Entity[uuid]");
    assertThat(uuidsOf(database, statement, params)).containsExactlyInAnyOrder("entity-1", "other-1", "plain-1", "leaf-1");

    // and with a type that has no index and holds vertices the walk of the whole database answers, once
    final Database mixed = newMixed();
    final Map<String, Object> mixedParams = new HashMap<>();
    mixedParams.put("keys", List.of("entity-1", "plain-1"));
    assertThat(profilePlan(mixed, statement, mixedParams)).doesNotContain("[index:");
    assertThat(uuidsOf(mixed, statement, mixedParams)).containsExactlyInAnyOrder("entity-1", "plain-1");
  }

  @Test
  void aWriteOfAnEarlierRowDoesNotFeedTheMatchOfALaterRow() {
    // the same key on more rows than a batch holds (100): the CREATE of the first rows writes vertices the match of the later
    // rows could read if it were seeked again, and a serial execution of the statement reads the database as it was when the
    // statement started: one vertex per row
    buildMain(database, true);
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", Collections.nCopies(250, "entity-1"));

    database.transaction(() -> database.command("opencypher", "UNWIND $keys AS k MATCH (n {uuid: k}) CREATE (m:Plain {uuid: k})", params));

    assertThat(uuidsOf(database, "MATCH (n:Plain) WHERE n.uuid = 'entity-1' RETURN n.uuid AS uuid", null)).hasSize(250);
  }

  @Test
  void aWriteOfAnEarlierRowDoesNotFeedTheMatchOfALaterRowOfALabelledNode() {
    // the reference: the labelled form of the same statement, which has always been seeked for every row
    buildMain(database, true);
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", Collections.nCopies(250, "plain-1"));

    database.transaction(() -> database.command("opencypher", "UNWIND $keys AS k MATCH (n:Plain {uuid: k}) CREATE (m:Plain {uuid: k})", params));

    assertThat(uuidsOf(database, "MATCH (n:Plain) WHERE n.uuid = 'plain-1' RETURN n.uuid AS uuid", null)).hasSize(251);
  }

  @Test
  void theIndexPathAnswersWhatTheWalkAnswers() {
    // the same statement over a database with the index and over one without it: the rows have to be the same
    buildMain(database, true);
    final Database withoutIndex = newDatabase("noindex");
    buildMain(withoutIndex, false);

    final Object[] values = { 1, 1L, 1.0d, 1.5d, "1", Boolean.TRUE };
    for (final Object value : values)
      assertThat(uuidsOf(database, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(value)))
          .as("the index path and the walk have to answer the same rows for %s", value)
          .isEqualTo(uuidsOf(withoutIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(value)));
  }

  @Test
  void aTextAgainstANumberPropertyIsAnsweredByTheWalkAndNotByTheIndex() {
    buildMain(database, true);

    // a numeric property declared LONG reads the text '1' as 1: the walk finds the vertex, an index keyed by a number
    // cannot be asked with a string, so the walk answers
    final String plan = profilePlan(database, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params("1"));

    assertThat(uuidsOf(database, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params("1"))).containsExactly("numeric-1");
    assertThat(plan).as("the text of a number is not sought in an index keyed by numbers\n%s", plan).doesNotContain("Numeric[id]");
  }

  @Test
  void theTextOfARIDIsAnsweredByTheWalkAndNotByTheIndex() {
    buildMain(database, true);
    final Database withoutIndex = newDatabase("noindex");
    buildMain(withoutIndex, false);

    // a numeric property and the text of a RID: the walk answers it by the id(), the index cannot key it (issue #11933)
    final RID rid = ridOf(database, "numeric-1");
    final RID otherRid = ridOf(withoutIndex, "numeric-1");

    assertThat(uuidsOf(database, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(rid.toString())))
        .isEqualTo(uuidsOf(withoutIndex, "MATCH (n {id: $u}) RETURN n.uuid AS uuid", params(otherRid.toString())));
  }

  @Test
  void aNumberTheIndexCannotKeyExactlyIsAnsweredByTheWalk() {
    // a long past 2^53 and a double: the walk compares them as doubles, the key of the index is the long
    final String statement = "MATCH (n {n: $u}) RETURN n.uuid AS uuid";
    final List<List<String>> answers = new ArrayList<>();
    for (final boolean indexed : new boolean[] { true, false }) {
      final Database db = newDatabase("big-" + indexed);
      db.transaction(() -> {
        final VertexType big = db.getSchema().createVertexType("Big");
        big.createProperty("n", Long.class);
        big.createProperty("uuid", String.class);
        if (indexed)
          big.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "n");
      });
      db.transaction(() -> db.command("opencypher", "CREATE (b:Big {n: 9007199254740993, uuid: 'big-1'})"));
      answers.add(uuidsOf(db, statement, params(9007199254740992d)));
    }

    assertThat(answers.get(0)).as("the index path answers what the walk answers").isEqualTo(answers.get(1));
  }

  @Test
  void aVertexCreatedInTheSameTransactionIsFound() {
    buildMain(database, true);
    final List<String> found = new ArrayList<>();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (e:Entity {uuid: 'fresh-1'})");
      database.command("opencypher", "CREATE (p:Plain {uuid: 'fresh-2'})");
      found.addAll(drain(database.command("opencypher", "MATCH (n {uuid: 'fresh-1'}) RETURN n.uuid AS uuid")));
      found.addAll(drain(database.command("opencypher", "MATCH (n {uuid: 'fresh-2'}) RETURN n.uuid AS uuid")));
    });

    assertThat(found).containsExactlyInAnyOrder("fresh-1", "fresh-2");
  }

  @Test
  void aVertexDeletedOrChangedInTheSameTransactionIsAnsweredAsItIsNow() {
    buildMain(database, true);
    final List<String> deleted = new ArrayList<>();
    final List<String> renamedOld = new ArrayList<>();
    final List<String> renamedNew = new ArrayList<>();

    database.transaction(() -> {
      database.command("opencypher", "MATCH (n:Entity {uuid: 'entity-1'}) DELETE n");
      database.command("opencypher", "MATCH (n:Other {uuid: 'other-1'}) SET n.uuid = 'other-2'");
      deleted.addAll(drain(database.command("opencypher", "MATCH (n {uuid: 'entity-1'}) RETURN n.uuid AS uuid")));
      renamedOld.addAll(drain(database.command("opencypher", "MATCH (n {uuid: 'other-1'}) RETURN n.uuid AS uuid")));
      renamedNew.addAll(drain(database.command("opencypher", "MATCH (n {uuid: 'other-2'}) RETURN n.uuid AS uuid")));
    });

    assertThat(deleted).as("a deleted vertex is not found").isEmpty();
    assertThat(renamedOld).as("the old value of a changed property is not found").isEmpty();
    assertThat(renamedNew).as("the new value of a changed property is found").containsExactly("other-2");
  }

  private static List<String> drain(final ResultSet rs) {
    final List<String> uuids = new ArrayList<>();
    while (rs.hasNext())
      uuids.add(rs.next().getProperty("uuid"));
    rs.close();
    return uuids;
  }

  private RID ridOf(final Database db, final String uuid) {
    final RID[] rid = new RID[1];
    db.transaction(() -> {
      final ResultSet rs = db.command("opencypher", "MATCH (n {uuid: $u}) RETURN n AS n", params(uuid));
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
