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
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * The regression test of issue #12546: a node pattern with no label (or with several) that is pinned to a LIST of values by an
 * indexed property ({@code MATCH (n) WHERE n.uuid IN $keys}) walked every vertex, while the same pattern pinned to one value, or the
 * labelled form of the list, is answered by the indexes. The list is answered by the same per-bucket plan as the equality
 * (issue #12522): every vertex bucket is answered by exactly one source, an index seeked once per key or a walk of the bucket, and
 * the rows are the ones the walk answers.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherUnlabelledNodeInListIndexSeekIssue12546Test extends TestHelper {
  private static final String PATH_PREFIX = "target/databases/Issue12546-";
  private static final String IN_KEYS     = "MATCH (n) WHERE n.uuid IN $keys RETURN n.uuid AS uuid";

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
   * The schema of the main tests: every vertex type has an index on {@code uuid}, or inherits the one of its parent. The types of
   * the hierarchy are there so that the answers of a type under another one are checked as well.
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
      db.command("opencypher", "CREATE (n:Numeric {id: 2, uuid: 'numeric-2'})");
      db.command("opencypher", "CREATE (b:Base {uuid: 'base-1'})");
      db.command("opencypher", "CREATE (m:Mid {uuid: 'mid-1'})");
      db.command("opencypher", "CREATE (l:Leaf {uuid: 'leaf-1'})");
      db.command("opencypher", "CREATE (o:Own {uuid: 'own-1'})");
      db.command("opencypher", "CREATE (u:UnderOwn {uuid: 'under-own-1'})");
    });
  }

  /**
   * A type under two types: the two branches can each hold an index over the property, and each of them holds the vertices of
   * their common sub type.
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

  /** Two labels on one node: openCypher creates a composite type for it, a type under both, which have an index each. */
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

  /** One type that has an index, one that has not and holds a vertex, and one that has none and holds nothing. */
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

  /** The plan of the statement, as the PROFILE run printed it: the name of the index the step used, when it used one. */
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

  private static Map<String, Object> keys(final Object... values) {
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", Arrays.asList(values));
    return params;
  }

  @Test
  void anUnlabelledNodePinnedByAListUsesTheIndexes() {
    buildMain(database, true);

    final String plan = profilePlan(database, IN_KEYS, keys("entity-1", "other-1", "plain-1"));

    assertThat(plan).as("the IN list is answered by the index of every type that has one\n%s", plan)
        .contains("Entity[uuid]").contains("Other[uuid]").contains("Plain[uuid]");
    assertThat(uuidsOf(database, IN_KEYS, keys("entity-1", "other-1", "plain-1"))).containsExactlyInAnyOrder("entity-1", "other-1", "plain-1");
  }

  @Test
  void aTypeWithoutAnIndexIsStillWalkedBesideTheTypesThatHaveOne() {
    final Database db = newMixed();

    final String plan = profilePlan(db, IN_KEYS, keys("entity-1", "plain-1", "nobody"));

    assertThat(plan).as("the type that has the index is answered by it\n%s", plan).contains("Entity[uuid]");
    assertThat(uuidsOf(db, IN_KEYS, keys("entity-1", "plain-1", "nobody"))).as("the type with no index is walked")
        .containsExactlyInAnyOrder("entity-1", "plain-1");
  }

  @Test
  void withoutAnyIndexTheListIsAnsweredByTheWalk() {
    buildMain(database, false);

    final String plan = profilePlan(database, IN_KEYS, keys("entity-1", "other-1"));

    assertThat(plan).as("without an index there is nothing to seek\n%s", plan).doesNotContain("[index:");
    assertThat(uuidsOf(database, IN_KEYS, keys("entity-1", "other-1"))).containsExactlyInAnyOrder("entity-1", "other-1");
  }

  @Test
  void theRowsAreTheOnesTheWalkAnswers() {
    // the same statement over a database with the indexes and over one without them
    buildMain(database, true);
    final Database withoutIndex = newDatabase("noindex");
    buildMain(withoutIndex, false);

    final List<List<Object>> lists = List.of(List.of(), List.of("entity-1"), List.of("entity-1", "entity-2", "other-1"),
        List.of("mid-1", "base-1", "leaf-1", "own-1", "under-own-1"), List.of("nobody"), List.of("nobody", "plain-1", "numeric-2"),
        Arrays.asList("entity-2", null, "other-1"), Arrays.asList(null, null), List.of("entity-1", "entity-1", "entity-1"));
    for (final List<Object> list : lists) {
      final Map<String, Object> params = new HashMap<>();
      params.put("keys", list);
      assertThat(uuidsOf(database, IN_KEYS, params)).as("the indexes and the walk have to answer the same rows for %s", list)
          .containsExactlyInAnyOrderElementsOf(uuidsOf(withoutIndex, IN_KEYS, params));
    }
  }

  @Test
  void aSubTypeIsAnsweredOnceByTheIndexOfItsParent() {
    buildMain(database, true);

    // the index of the parent holds the vertices of the child too: the child must not be answered twice
    assertThat(uuidsOf(database, IN_KEYS, keys("base-1", "mid-1", "leaf-1"))).containsExactlyInAnyOrder("base-1", "mid-1", "leaf-1");
    assertThat(uuidsOf(database, IN_KEYS, keys("own-1", "under-own-1"))).containsExactlyInAnyOrder("own-1", "under-own-1");
  }

  @Test
  void aTypeUnderTwoTypesIsAnsweredOnceWhateverBranchHasTheIndex() {
    for (final boolean[] indexed : new boolean[][] { { true, true }, { true, false }, { false, true } }) {
      final Database db = newDiamond(indexed[0], indexed[1]);
      final String label = "left indexed: " + indexed[0] + ", right indexed: " + indexed[1];

      assertThat(uuidsOf(db, IN_KEYS, keys("diamond-1", "left-1", "right-1", "nobody"))).as(label)
          .containsExactlyInAnyOrder("diamond-1", "left-1", "right-1");
      assertThat(uuidsOf(db, IN_KEYS, keys("diamond-1"))).as(label).containsExactly("diamond-1");
      assertThat(profilePlan(db, IN_KEYS, keys("diamond-1", "left-1"))).as(label).contains("[index:");
      extraDatabases.remove(db);
      db.drop();
    }
  }

  @Test
  void aPatternWithSeveralLabelsIsAnsweredByTheIndexesOfTheTypesTheyName() {
    // (n:A:B) is the composite type under both, which holds no index of its own: the indexes of A and of B answer for its bucket
    final Database db = newMultiLabel();

    final String both = "MATCH (n:A:B) WHERE n.uuid IN $keys RETURN n.uuid AS uuid";
    assertThat(profilePlan(db, both, keys("multi-1", "a-1"))).contains("[index:");
    assertThat(uuidsOf(db, both, keys("multi-1", "a-1", "b-1"))).as("a vertex that only carries A is not an A and B").containsExactly("multi-1");

    // (n:A|B) is A, B and the composite
    final String either = "MATCH (n:A|B) WHERE n.uuid IN $keys RETURN n.uuid AS uuid";
    assertThat(uuidsOf(db, either, keys("multi-1", "a-1", "b-1", "nobody"))).containsExactlyInAnyOrder("multi-1", "a-1", "b-1");

    // a node with two labels is the composite, answered once
    assertThat(uuidsOf(db, IN_KEYS, keys("multi-1", "a-1", "b-1"))).containsExactlyInAnyOrder("multi-1", "a-1", "b-1");
    assertThat(profilePlan(db, IN_KEYS, keys("multi-1"))).contains("[index:");

    // a label narrows the types: a vertex of another type with a key of the list is not an answer
    assertThat(uuidsOf(db, "MATCH (n:A) WHERE n.uuid IN $keys RETURN n.uuid AS uuid", keys("b-1", "a-1"))).containsExactly("a-1");
  }

  @Test
  void aKeyTheIndexCannotKeyExactlyIsAnsweredByTheWalk() {
    // a numeric property declared LONG: a double, the text of a number and the text of a RID are compared by the walk the way the
    // WHERE filter compares them, which an index keyed by numbers cannot reproduce
    buildMain(database, true);
    final Database withoutIndex = newDatabase("noindex");
    buildMain(withoutIndex, false);

    final String statement = "MATCH (n) WHERE n.id IN $keys RETURN n.uuid AS uuid";
    final List<List<Object>> lists = List.of(List.of(1L), List.of(1, 2), List.of(1L, 1.0d), List.of(1.5d), List.of("1"), List.of(1L, "2"),
        List.of(true), Arrays.asList(1L, null), List.of(9007199254740993L));
    for (final List<Object> list : lists) {
      final Map<String, Object> params = new HashMap<>();
      params.put("keys", list);
      assertThat(uuidsOf(database, statement, params)).as("the indexes and the walk have to answer the same rows for %s", list)
          .containsExactlyInAnyOrderElementsOf(uuidsOf(withoutIndex, statement, params));
    }

    assertThat(profilePlan(database, statement, keys(1L, 2L))).as("whole numbers are keys of the index").contains("Numeric[id]");
    assertThat(profilePlan(database, statement, keys(1L, 1.5d))).as("one value the index cannot key leaves the walk in charge")
        .doesNotContain("Numeric[id]");
    assertThat(profilePlan(database, statement, keys("1"))).as("the text of a number").doesNotContain("Numeric[id]");
  }

  @Test
  void aNumberTheIndexCannotKeyExactlyIsAnsweredByTheWalk() {
    // a long past 2^53 and a double: the walk compares them as doubles, the key of the index is the long
    final String statement = "MATCH (n) WHERE n.n IN $keys RETURN n.uuid AS uuid";
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
      answers.add(uuidsOf(db, statement, keys(9007199254740992d)));
      if (indexed)
        assertThat(profilePlan(db, statement, keys(9007199254740992d))).as("a double for an integer key is left to the walk")
            .doesNotContain("Big[n]");
    }

    assertThat(answers.get(0)).as("the indexes answer what the walk answers").isEqualTo(answers.get(1));
  }

  @Test
  void theTextOfARIDIsAnsweredByTheWalkAndNotByTheIndex() {
    buildMain(database, true);
    final Database withoutIndex = newDatabase("noindex");
    buildMain(withoutIndex, false);

    final String statement = "MATCH (n) WHERE n.id IN $keys RETURN n.uuid AS uuid";
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", List.of(ridOf(database, "numeric-1").toString()));
    final Map<String, Object> otherParams = new HashMap<>();
    otherParams.put("keys", List.of(ridOf(withoutIndex, "numeric-1").toString()));

    assertThat(uuidsOf(database, statement, params)).isEqualTo(uuidsOf(withoutIndex, statement, otherParams));
    assertThat(profilePlan(database, statement, params)).doesNotContain("Numeric[id]");
  }

  @Test
  void anEmptyListAndAListOfNullsAnswerNoRow() {
    buildMain(database, true);

    assertThat(uuidsOf(database, IN_KEYS, keys())).isEmpty();
    assertThat(uuidsOf(database, IN_KEYS, keys((Object) null))).isEmpty();
    assertThat(uuidsOf(database, IN_KEYS, keys(null, null))).isEmpty();
    // a null among the keys equals nothing, and the others are answered
    assertThat(uuidsOf(database, IN_KEYS, keys(null, "entity-1"))).containsExactly("entity-1");
  }

  @Test
  void aKeyThatIsRepeatedAnswersItsVertexOnce() {
    buildMain(database, true);

    assertThat(uuidsOf(database, IN_KEYS, keys("entity-1", "entity-1", "entity-2", "entity-1"))).containsExactlyInAnyOrder("entity-1", "entity-2");
    assertThat(uuidsOf(database, "MATCH (n) WHERE n.uuid IN ['plain-1', 'plain-1', $a] RETURN n.uuid AS uuid", Map.of("a", "plain-1")))
        .containsExactly("plain-1");
  }

  @Test
  void aListOfLiteralsAndParametersIsAnsweredByTheIndexes() {
    buildMain(database, true);

    final String statement = "MATCH (n) WHERE n.uuid IN ['entity-1', $b, 'other-1'] RETURN n.uuid AS uuid";
    final Map<String, Object> params = Map.of("b", "plain-1");

    assertThat(profilePlan(database, statement, params)).contains("Entity[uuid]").contains("Plain[uuid]");
    assertThat(uuidsOf(database, statement, params)).containsExactlyInAnyOrder("entity-1", "plain-1", "other-1");
  }

  @Test
  void aStatementThatStopsAtTheFirstRowDoesNotSeekTheIndexesAfterIt() {
    // two indexes answer the keys, the first holds more entries than the lookup holds whole (256): it is read key by key as the
    // caller pulls, and a LIMIT that one key satisfies seeks no other key and no other index
    final Database db = newDatabase("lazy");
    db.transaction(() -> {
      for (final String name : List.of("First", "Second")) {
        final VertexType type = db.getSchema().createVertexType(name);
        type.createProperty("uuid", String.class);
        type.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, false, "uuid");
      }
    });
    db.transaction(() -> {
      db.command("opencypher", "UNWIND range(1, 300) AS i CREATE (a:First {uuid: 'dup'})");
      db.command("opencypher", "UNWIND range(1, 300) AS i CREATE (a:First {uuid: 'other'})");
      db.command("opencypher", "CREATE (a:Second {uuid: 'dup'})");
    });
    final Map<String, Object> params = keys("dup", "other");

    final String plan = profilePlan(db, IN_KEYS + " LIMIT 1", params);

    final boolean first = plan.contains("First[uuid]");
    final boolean second = plan.contains("Second[uuid]");
    assertThat(first ^ second).as("exactly one of the two indexes is sought when one batch is enough\n%s", plan).isTrue();
    assertThat(uuidsOf(db, IN_KEYS, params)).as("without a LIMIT every index is sought").hasSize(601);
    assertThat(uuidsOf(db, IN_KEYS + " LIMIT 5", params)).hasSize(5);
  }

  @Test
  void aListTheRowDecidesIsSeekedAgainForEveryRow() {
    buildMain(database, true);
    final Map<String, Object> params = new HashMap<>();
    params.put("lists", List.of(List.of("entity-1", "other-1"), List.of("plain-1", "nobody"), List.of(), List.of("mid-1", "leaf-1", "entity-2")));
    final String statement = "UNWIND $lists AS ks MATCH (n) WHERE n.uuid IN ks RETURN n.uuid AS uuid";

    assertThat(profilePlan(database, statement, params)).as("a list the row decides is answered by the indexes too").contains("Entity[uuid]");
    assertThat(uuidsOf(database, statement, params)).containsExactlyInAnyOrder("entity-1", "other-1", "plain-1", "mid-1", "leaf-1", "entity-2");

    // every row has its own keys: the candidates of the first row are not the ones of the second
    final Map<String, Object> sameKeyParams = new HashMap<>();
    sameKeyParams.put("lists", List.of(List.of("entity-1"), List.of("entity-2"), List.of("entity-1")));
    assertThat(uuidsOf(database, statement, sameKeyParams)).containsExactly("entity-1", "entity-2", "entity-1");
  }

  @Test
  void aRowThatCannotBeSoughtDoesNotLockTheRowsAfterItOutOfTheIndexes() {
    buildMain(database, true);
    final Map<String, Object> params = new HashMap<>();
    // the first row is a null (no list), the second a list with a key an index keyed by strings cannot be asked with, the third one
    // that contains a list, the fourth a good one: the walks that answer the first rows are not kept for the rows after them
    params.put("lists", Arrays.asList(null, List.of(1.5d), List.of(List.of("x")), List.of("entity-1", "other-1")));
    final String statement = "UNWIND $lists AS ks MATCH (n) WHERE n.uuid IN ks RETURN n.uuid AS uuid";

    assertThat(profilePlan(database, statement, params)).contains("Entity[uuid]");
    assertThat(uuidsOf(database, statement, params)).containsExactlyInAnyOrder("entity-1", "other-1");
  }

  @Test
  void aListTheRowDecidesWithATypeWithoutAnIndexIsAnsweredByTheWalk() {
    // the walk of a type without an index would be repeated for every row, where it used to be walked once and kept
    // (issues #6602, #8695): the walk of the whole database answers the statement instead
    final Database db = newMixed();
    final Map<String, Object> params = new HashMap<>();
    params.put("lists", List.of(List.of("entity-1"), List.of("plain-1")));
    final String statement = "UNWIND $lists AS ks MATCH (n) WHERE n.uuid IN ks RETURN n.uuid AS uuid";

    assertThat(profilePlan(db, statement, params)).as("a type without an index keeps the walk of the whole database").doesNotContain("[index:");
    assertThat(uuidsOf(db, statement, params)).containsExactlyInAnyOrder("entity-1", "plain-1");
  }

  @Test
  void aListTheRowDecidesIsSeekedWhenTheTypesWithoutAnIndexAreEmpty() {
    final Database db = newDatabase("empty-unindexed");
    db.transaction(() -> {
      final VertexType entity = db.getSchema().createVertexType("Entity");
      entity.createProperty("uuid", String.class);
      entity.createTypeIndex(Schema.INDEX_TYPE.LSM_TREE, true, "uuid");
      db.getSchema().createVertexType("Unused").createProperty("uuid", String.class);
    });
    db.transaction(() -> db.command("opencypher", "CREATE (e:Entity {uuid: 'entity-1'})"));
    final Map<String, Object> params = new HashMap<>();
    params.put("lists", List.of(List.of("entity-1", "nobody"), List.of("nobody")));
    final String statement = "UNWIND $lists AS ks MATCH (n) WHERE n.uuid IN ks RETURN n.uuid AS uuid";

    assertThat(profilePlan(db, statement, params)).as("a type that holds nothing is not a reason to walk the database").contains("Entity[uuid]");
    assertThat(uuidsOf(db, statement, params)).containsExactly("entity-1");
  }

  @Test
  void theOtherPredicatesOfTheWhereFilterStillDecideTheRows() {
    buildMain(database, true);

    assertThat(uuidsOf(database, "MATCH (n) WHERE n.uuid IN $keys AND n.name = 'two' RETURN n.uuid AS uuid", keys("entity-1", "entity-2", "other-1")))
        .containsExactly("entity-2");
    assertThat(uuidsOf(database, "MATCH (n) WHERE n.uuid IN $keys AND n.name <> 'two' RETURN n.uuid AS uuid", keys("entity-1", "entity-2")))
        .containsExactly("entity-1");
    // a list that is one alternative of a disjunction does not narrow the node: the walk answers
    final String disjunction = "MATCH (n) WHERE n.uuid IN $keys OR n.name = 'two' RETURN n.uuid AS uuid";
    assertThat(profilePlan(database, disjunction, keys("other-1"))).doesNotContain("[index:");
    assertThat(uuidsOf(database, disjunction, keys("other-1"))).containsExactlyInAnyOrder("other-1", "entity-2");
    // and the negation of a list keeps every other vertex
    assertThat(uuidsOf(database, "MATCH (n:Entity) WHERE NOT n.uuid IN $keys RETURN n.uuid AS uuid", keys("entity-1"))).containsExactly("entity-2");
  }

  @Test
  void anEqualityTheIndexesAnswerIsPreferredToTheList() {
    buildMain(database, true);
    final Map<String, Object> params = new HashMap<>();
    params.put("u", "entity-2");
    params.put("keys", List.of("entity-1", "entity-2", "other-1"));

    final String statement = "MATCH (n) WHERE n.uuid = $u AND n.uuid IN $keys RETURN n.uuid AS uuid";

    assertThat(profilePlan(database, statement, params)).contains("Entity[uuid]");
    assertThat(uuidsOf(database, statement, params)).containsExactly("entity-2");
  }

  @Test
  void aListIsSeekedWhenTheEqualityBesideItHasNoIndex() {
    buildMain(database, true);

    // name has no index, uuid has one: the list narrows the node
    final String statement = "MATCH (n) WHERE n.name = 'two' AND n.uuid IN $keys RETURN n.uuid AS uuid";

    assertThat(profilePlan(database, statement, keys("entity-1", "entity-2", "other-1"))).contains("Entity[uuid]");
    assertThat(uuidsOf(database, statement, keys("entity-1", "entity-2", "other-1"))).containsExactly("entity-2");

    // an equality an index cannot key (a double against the LONG id) leaves the list to narrow the node
    final String unkeyable = "MATCH (n) WHERE n.id = 1.0 AND n.uuid IN $keys RETURN n.uuid AS uuid";
    assertThat(profilePlan(database, unkeyable, keys("numeric-1", "entity-1"))).contains("Numeric[uuid]");
    assertThat(uuidsOf(database, unkeyable, keys("numeric-1", "numeric-2", "entity-1"))).containsExactly("numeric-1");
  }

  @Test
  void theListWithTheFewestKeysNarrowsTheNode() {
    buildMain(database, true);
    final Map<String, Object> params = new HashMap<>();
    params.put("many", List.of("entity-1", "entity-2", "other-1", "plain-1"));
    params.put("few", List.of("entity-2", "nobody"));

    final String statement = "MATCH (n) WHERE n.uuid IN $many AND n.uuid IN $few RETURN n.uuid AS uuid";

    assertThat(uuidsOf(database, statement, params)).containsExactly("entity-2");
    assertThat(profilePlan(database, statement, params)).contains("Entity[uuid]");
  }

  @Test
  void aValueThatIsNotAListLeavesTheWalkToRaiseWhatItRaises() {
    buildMain(database, true);
    final Database withoutIndex = newDatabase("noindex");
    buildMain(withoutIndex, false);
    final Map<String, Object> params = new HashMap<>();
    params.put("keys", "entity-1");

    final Throwable indexed = catchThrowable(() -> uuidsOf(database, IN_KEYS, params));
    final Throwable walked = catchThrowable(() -> uuidsOf(withoutIndex, IN_KEYS, params));

    assertThat(indexed == null).as("the indexes and the walk answer a value that is not a list alike: %s / %s", indexed, walked)
        .isEqualTo(walked == null);
    if (indexed != null)
      assertThat(indexed.getClass()).isEqualTo(walked.getClass());
  }

  @Test
  void aVertexCreatedInTheSameTransactionIsFound() {
    buildMain(database, true);
    final List<String> found = new ArrayList<>();

    database.transaction(() -> {
      database.command("opencypher", "CREATE (e:Entity {uuid: 'fresh-1'})");
      database.command("opencypher", "CREATE (p:Plain {uuid: 'fresh-2'})");
      found.addAll(drain(database.command("opencypher", IN_KEYS, keys("fresh-1", "fresh-2", "entity-1"))));
    });

    assertThat(found).containsExactlyInAnyOrder("fresh-1", "fresh-2", "entity-1");
  }

  @Test
  void aVertexDeletedOrChangedInTheSameTransactionIsAnsweredAsItIsNow() {
    buildMain(database, true);
    final List<String> found = new ArrayList<>();

    database.transaction(() -> {
      database.command("opencypher", "MATCH (n:Entity {uuid: 'entity-1'}) DELETE n");
      database.command("opencypher", "MATCH (n:Other {uuid: 'other-1'}) SET n.uuid = 'other-2'");
      found.addAll(drain(database.command("opencypher", IN_KEYS, keys("entity-1", "other-1", "other-2", "entity-2"))));
    });

    assertThat(found).as("a deleted vertex and the old value of a changed property are not found").containsExactlyInAnyOrder("other-2", "entity-2");
  }

  @Test
  void aWriteOfAnEarlierRowDoesNotFeedTheMatchOfALaterRow() {
    // the same list on more rows than a batch holds (100): the CREATE of the first rows writes vertices the match of the later
    // rows could read if it were seeked again, and a serial execution of the statement reads the database as it was when the
    // statement started: one vertex per row
    buildMain(database, true);
    final Map<String, Object> params = new HashMap<>();
    params.put("lists", Collections.nCopies(250, List.of("entity-1")));

    database.transaction(() -> database.command("opencypher",
        "UNWIND $lists AS ks MATCH (n) WHERE n.uuid IN ks CREATE (m:Plain {uuid: 'entity-1'})", params));

    assertThat(uuidsOf(database, "MATCH (n:Plain) WHERE n.uuid = 'entity-1' RETURN n.uuid AS uuid", null)).hasSize(250);
  }

  @Test
  void aWriteOfAnEarlierRowDoesNotFeedTheMatchOfALaterRowOfAListThatNoRowDecides() {
    // the candidates of a list every row shares are read once and replayed, as the walk they replace was
    buildMain(database, true);

    database.transaction(() -> database.command("opencypher",
        "UNWIND range(1, 250) AS i MATCH (n) WHERE n.uuid IN ['entity-1', 'nobody'] CREATE (m:Plain {uuid: 'entity-1'})"));

    assertThat(uuidsOf(database, "MATCH (n:Plain) WHERE n.uuid = 'entity-1' RETURN n.uuid AS uuid", null)).hasSize(250);
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
      final ResultSet rs = db.command("opencypher", "MATCH (n) WHERE n.uuid = $u RETURN n AS n", Map.of("u", uuid));
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
