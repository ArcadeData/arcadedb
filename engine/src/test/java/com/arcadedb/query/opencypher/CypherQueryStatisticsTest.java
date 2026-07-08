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
import com.arcadedb.index.TypeIndex;
import com.arcadedb.query.sql.executor.QueryStatistics;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.Schema;
import com.arcadedb.schema.Type;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that write commands populate QueryStatistics with Neo4j-compatible CRUD counts,
 * and that read queries carry no statistics.
 */
class CypherQueryStatisticsTest extends TestHelper {

  private QueryStatistics statsOf(final Database db, final String cypher) {
    final ResultSet rs = db.command("opencypher", cypher);
    while (rs.hasNext())
      rs.next();
    final Optional<QueryStatistics> s = rs.getStatistics();
    assertThat(s).isPresent();
    return s.get();
  }

  @Test
  void createNodesRelationshipAndProperties() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database,
          "CREATE (:Beer {name:'IPA'})-[:BREWED_BY {since:1990}]->(:Brewery {name:'Acme'})");
      assertThat(s.getNodesCreated()).isEqualTo(2);
      assertThat(s.getRelationshipsCreated()).isEqualTo(1);
      assertThat(s.getPropertiesSet()).isEqualTo(3); // name, since, name
      assertThat(s.getLabelsAdded()).isEqualTo(2);   // Beer, Brewery
      assertThat(s.containsUpdates()).isTrue();
    });
  }

  @Test
  void createWithInlineNullPropertyDoesNotCountIt() {
    database.transaction(() -> {
      // Neo4j does not store a null-valued property, so it is not counted: only 'a' is set.
      final QueryStatistics s = statsOf(database, "CREATE (:Foo {a: 1, b: null})");
      assertThat(s.getNodesCreated()).isEqualTo(1);
      assertThat(s.getPropertiesSet()).isEqualTo(1);
    });
  }

  @Test
  void setPropertyAndLabel() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Person {name:'a'})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Person {name:'a'}) SET n.age = 30, n:Employee");
      assertThat(s.getPropertiesSet()).isEqualTo(1);
      assertThat(s.getLabelsAdded()).isEqualTo(1);
    });
  }

  @Test
  void removePropertyAndLabel() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Tag:Old {name:'x'})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Tag {name:'x'}) REMOVE n.name, n:Old");
      assertThat(s.getPropertiesSet()).isEqualTo(1);
      assertThat(s.getLabelsRemoved()).isEqualTo(1);
    });
  }

  @Test
  void removeAbsentPropertyCountsZero() {
    // Neo4j reports properties-set: 0 when REMOVE targets a property that isn't present.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Widget3 {name:'x'})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Widget3 {name:'x'}) REMOVE n.doesNotExist");
      assertThat(s.getPropertiesSet()).isZero();
      assertThat(s.containsUpdates()).isFalse();
    });
  }

  @Test
  void setAbsentPropertyToNullCountsZero() {
    // SET n.absent = null on a property that was never set is a no-op removal: Neo4j counts 0.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Widget4 {name:'x'})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Widget4 {name:'x'}) SET n.absent = null");
      assertThat(s.getPropertiesSet()).isZero();
    });
  }

  @Test
  void removeExistingPropertyStillCountsOne() {
    // Control: removing a property that genuinely exists must still count 1 (guards against
    // regressing the existing-property removal path while fixing the absent-property case above).
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Widget5 {name:'x'})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Widget5 {name:'x'}) REMOVE n.name");
      assertThat(s.getPropertiesSet()).isEqualTo(1);
    });
  }

  @Test
  void deleteNode() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Doomed {id:1})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Doomed {id:1}) DELETE n");
      assertThat(s.getNodesDeleted()).isEqualTo(1);
    });
  }

  @Test
  void mergeCreatesOnlyOnce() {
    database.transaction(() -> {
      final QueryStatistics first = statsOf(database, "MERGE (:City {name:'Rome'})");
      assertThat(first.getNodesCreated()).isEqualTo(1);
      final ResultSet rs = database.command("opencypher", "MERGE (:City {name:'Rome'})");
      while (rs.hasNext()) rs.next();
      assertThat(rs.getStatistics().get().getNodesCreated()).isZero();
    });
  }

  @Test
  void readQueryHasNoStatistics() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:R {n:1})");
      final ResultSet rs = database.query("opencypher", "MATCH (n:R) RETURN n");
      while (rs.hasNext()) rs.next();
      assertThat(rs.getStatistics()).isEmpty();
    });
  }

  @Test
  void createVertexWithDuplicateLabelCountsDistinctLabels() {
    // ArcadeDB dedups labels (Labels.ensureCompositeType), so CREATE (n:Dup:Dup) creates a
    // single-label vertex. Neo4j-compatible statistics must report 1 label added, not 2.
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database, "CREATE (n:Dup:Dup) RETURN n");
      assertThat(s.getNodesCreated()).isEqualTo(1);
      assertThat(s.getLabelsAdded()).isEqualTo(1);
    });
  }

  @Test
  void mergeCreateWithDuplicateLabelCountsDistinctLabels() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database, "MERGE (n:Twin:Twin {id:1}) RETURN n");
      assertThat(s.getNodesCreated()).isEqualTo(1);
      assertThat(s.getLabelsAdded()).isEqualTo(1);
    });
  }

  @Test
  void setReplaceMapCountsRemovedPreexistingProperties() {
    // n originally has 3 properties (a, b, c). SET n = {a:10, d:4} keeps/updates "a", adds "d",
    // and removes "b" and "c" since they are absent from the new map. Neo4j-compatible
    // "properties set" counts both the 2 written entries (a, d) and the 2 removed ones (b, c) = 4.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Item {a:1, b:2, c:3})");
      final QueryStatistics s = statsOf(database, "MATCH (n:Item) SET n = {a:10, d:4}");
      assertThat(s.getPropertiesSet()).isEqualTo(4);
    });
  }

  @Test
  void mergeOnMatchSetReplaceMapCountsRemovedPreexistingProperties() {
    // Same replace-map semantics as above (a kept/updated, d added, b and c removed = 4), but
    // exercised through MERGE ... ON MATCH SET, which duplicates the REPLACE_MAP branch in
    // MergeStep.applySetClause.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Widget {a:1, b:2, c:3})");
      final QueryStatistics s = statsOf(database,
          "MERGE (n:Widget {a:1}) ON MATCH SET n = {a:10, d:4}");
      assertThat(s.getPropertiesSet()).isEqualTo(4);
    });
  }

  @Test
  void mergeMapPlusWithNullValuesCountsOnlyRealChanges() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Widget {a:1, b:2})");
      // += : c set (new, +1), b removed (existing null, +1), z removed (absent null, +0) = 2
      final QueryStatistics s = statsOf(database, "MATCH (n:Widget) SET n += {c: 3, b: null, z: null}");
      assertThat(s.getPropertiesSet()).isEqualTo(2);
    });
  }

  @Test
  void mergeOnMatchSetPlusMapWithNullCountsOnlyRealChanges() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:Gadget {k:'x', a:1})");
      // ON MATCH SET += : a removed (existing null, +1), absent removed (+0), b set (+1) = 2
      final QueryStatistics s = statsOf(database, "MERGE (n:Gadget {k:'x'}) ON MATCH SET n += {a: null, absent: null, b: 2}");
      assertThat(s.getPropertiesSet()).isEqualTo(2);
    });
  }

  @Test
  void createIndexCounts() {
    database.command("opencypher", "CREATE (:Product {sku:'a'})"); // ensure type exists
    final ResultSet rs = database.command("opencypher", "CREATE INDEX FOR (p:Product) ON (p.sku)");
    while (rs.hasNext())
      rs.next();
    assertThat(rs.getStatistics()).isPresent();
    assertThat(rs.getStatistics().get().getIndexesAdded()).isEqualTo(1);
    assertThat(rs.getStatistics().get().containsUpdates()).isTrue();
  }

  @Test
  void createIndexIfNotExistsAlreadyPresentCountsZero() {
    database.command("opencypher", "CREATE (:Widget2 {code:'a'})");
    database.command("opencypher", "CREATE INDEX IF NOT EXISTS FOR (w:Widget2) ON (w.code)");
    // Second run is a genuine no-op: the index already exists, so nothing changed.
    final QueryStatistics s = statsOf(database, "CREATE INDEX IF NOT EXISTS FOR (w:Widget2) ON (w.code)");
    assertThat(s.getIndexesAdded()).isZero();
  }

  @Test
  void dropIndexCounts() {
    database.command("opencypher", "CREATE (:Gadget {code:'a'})");
    database.command("opencypher", "CREATE INDEX FOR (g:Gadget) ON (g.code)");

    final Collection<TypeIndex> indexes = database.getSchema().getType("Gadget").getAllIndexes(false);
    assertThat(indexes).isNotEmpty();
    final String indexName = indexes.iterator().next().getName();

    final QueryStatistics s = statsOf(database, "DROP INDEX `" + indexName + "`");
    assertThat(s.getIndexesRemoved()).isEqualTo(1);
    assertThat(s.containsUpdates()).isTrue();
  }

  @Test
  void createConstraintCounts() {
    final QueryStatistics s = statsOf(database, "CREATE CONSTRAINT FOR (p:Employee) REQUIRE p.id IS UNIQUE");
    assertThat(s.getConstraintsAdded()).isEqualTo(1);
    assertThat(s.containsUpdates()).isTrue();
  }

  @Test
  void createConstraintIfNotExistsAlreadyPresentCountsZero() {
    database.command("opencypher", "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Employee2) REQUIRE p.id IS UNIQUE");
    // Second run is a genuine no-op: the constraint already exists, so nothing changed.
    final QueryStatistics s = statsOf(database, "CREATE CONSTRAINT IF NOT EXISTS FOR (p:Employee2) REQUIRE p.id IS UNIQUE");
    assertThat(s.getConstraintsAdded()).isZero();
  }

  @Test
  void dropConstraintCounts() {
    database.getSchema().createVertexType("Manager");
    database.getSchema().getType("Manager").createProperty("id", Type.STRING);
    database.getSchema().buildTypeIndex("Manager", new String[] { "id" })
        .withType(Schema.INDEX_TYPE.LSM_TREE)
        .withUnique(true)
        .create();

    final Collection<TypeIndex> indexes = database.getSchema().getType("Manager").getAllIndexes(false);
    assertThat(indexes).isNotEmpty();
    final String constraintName = indexes.iterator().next().getName();

    final QueryStatistics s = statsOf(database, "DROP CONSTRAINT `" + constraintName + "`");
    assertThat(s.getConstraintsRemoved()).isEqualTo(1);
    assertThat(s.containsUpdates()).isTrue();
  }

  @Test
  void detachDeleteCountsNodeAndRelationship() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:DA {id:1})-[:REL]->(:DB {id:2})");
      final QueryStatistics s = statsOf(database, "MATCH (a:DA {id:1}) DETACH DELETE a");
      assertThat(s.getNodesDeleted()).isEqualTo(1);
      assertThat(s.getRelationshipsDeleted()).isEqualTo(1);
    });
  }

  @Test
  void foreachDetachDeleteCountsRelationship() {
    // Deferred path: FOREACH queues deletes via DeleteStep.DEFERRED_DELETE_BATCH_VAR and flushes
    // them through DeleteStep.flushDeferredDeletes, which must count cascaded relationships too.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:FA {id:1})-[:REL]->(:FB {id:2})");
      final QueryStatistics s = statsOf(database,
          "MATCH (a:FA {id:1}) WITH collect(a) AS xs FOREACH (x IN xs | DETACH DELETE x)");
      assertThat(s.getNodesDeleted()).isEqualTo(1);
      assertThat(s.getRelationshipsDeleted()).isEqualTo(1);
    });
  }

  @Test
  void mergeOnMatchSetUnchangedValueStillCountsPropertySet() {
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:MM {k:'x', p:1})");
      final QueryStatistics s = statsOf(database, "MERGE (n:MM {k:'x'}) ON MATCH SET n.p = 1");
      assertThat(s.getPropertiesSet()).isEqualTo(1);
    });
  }

  @Test
  void detachDeleteSelfLoopCountsRelationshipOnce() {
    // A self-loop relationship is returned by both vertex.getEdges(OUT) and vertex.getEdges(IN),
    // so the immediate (non-FOREACH) DETACH DELETE path must de-dup it: exactly 1 relationship
    // deleted, not 2, and no RecordNotFoundException from deleting the same edge twice.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (a:SL {id:1})");
      database.command("opencypher", "MATCH (a:SL {id:1}) CREATE (a)-[:LOOP]->(a)");
      final QueryStatistics s = statsOf(database, "MATCH (a:SL {id:1}) DETACH DELETE a");
      assertThat(s.getNodesDeleted()).isEqualTo(1);
      assertThat(s.getRelationshipsDeleted()).isEqualTo(1);
    });
  }

  @Test
  void detachDeleteWithExplicitEdgeCountsRelationshipOnce() {
    database.transaction(() -> {
      database.command("opencypher",
          "CREATE (a:Dd {n:'a'})-[:LINK]->(b:Dd {n:'b'})");
      final QueryStatistics s = statsOf(database,
          "MATCH (a:Dd {n:'a'})-[r:LINK]->(b:Dd {n:'b'}) DETACH DELETE r, a, b");
      assertThat(s.getRelationshipsDeleted()).isEqualTo(1);
      assertThat(s.getNodesDeleted()).isEqualTo(2);
    });
  }

  @Test
  void callSubqueryWriteCountsPropagateToOuter() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database,
          "UNWIND [1,2] AS x CALL { WITH x CREATE (:CallNode {v:x}) } RETURN x");
      assertThat(s.getNodesCreated()).isEqualTo(2);
      assertThat(s.getPropertiesSet()).isEqualTo(2);
      assertThat(s.getLabelsAdded()).isEqualTo(2);
      assertThat(s.containsUpdates()).isTrue();
    });
  }

  @Test
  void topLevelUnionWriteCountsAreSummed() {
    database.transaction(() -> {
      final QueryStatistics s = statsOf(database,
          "CREATE (:UnionA {n:1}) RETURN 1 AS r UNION ALL CREATE (:UnionB {n:2}) RETURN 2 AS r");
      assertThat(s.getNodesCreated()).isEqualTo(2);
      assertThat(s.getPropertiesSet()).isEqualTo(2);
      assertThat(s.getLabelsAdded()).isEqualTo(2);
      assertThat(s.containsUpdates()).isTrue();
    });
  }

  @Test
  void zeroNetEffectWriteUnionStillReportsStatisticsPresent() {
    // Presence of statistics on the result set signals "this was a write statement", independent
    // of whether the write actually mutated anything. Both UNION branches are MERGE clauses that
    // match the pre-existing node, so containsUpdates() is false, but getStatistics() must still
    // be present.
    database.transaction(() -> {
      database.command("opencypher", "CREATE (:UnionMergeTarget {id:1})");
      final ResultSet rs = database.command("opencypher",
          "MERGE (n:UnionMergeTarget {id:1}) RETURN n.id AS r UNION ALL "
              + "MERGE (n:UnionMergeTarget {id:1}) RETURN n.id AS r");
      while (rs.hasNext())
        rs.next();
      assertThat(rs.getStatistics()).isPresent();
      assertThat(rs.getStatistics().get().containsUpdates()).isFalse();
    });
  }

  @Test
  void notNullConstraintCountsAsConstraintAdded() {
    database.command("opencypher", "CREATE (:NnLabel {p:1})");
    final QueryStatistics s = statsOf(database, "CREATE CONSTRAINT FOR (n:NnLabel) REQUIRE n.p IS NOT NULL");
    assertThat(s.getConstraintsAdded()).isEqualTo(1);
  }

  @Test
  void nodeKeyConstraintCountsAsConstraintAdded() {
    final QueryStatistics s = statsOf(database, "CREATE CONSTRAINT FOR (n:KeyLabel) REQUIRE n.k IS NODE KEY");
    assertThat(s.getConstraintsAdded()).isEqualTo(1);
  }

  @Test
  void typedConstraintCountsAsConstraintAdded() {
    final QueryStatistics s = statsOf(database, "CREATE CONSTRAINT FOR (n:TypedLabel) REQUIRE n.age IS TYPED INTEGER");
    assertThat(s.getConstraintsAdded()).isEqualTo(1);
  }

  @Test
  void uniqueConstraintOverPlainIndexStillCountsAsConstraintAdded() {
    // A non-unique index already covers the property. TypeIndexBuilder.create() only tolerates a
    // pre-existing index with a different uniqueness when IF NOT EXISTS is used - it then drops the
    // plain index and creates the unique one. That is a genuine schema change and must be counted
    // even though indexExistsOnProperties(...) sees an index there both before and after.
    database.command("sql", "CREATE VERTEX TYPE CoveredLabel");
    database.command("sql", "CREATE PROPERTY CoveredLabel.email STRING");
    database.command("sql", "CREATE INDEX ON CoveredLabel (email) NOTUNIQUE");
    final QueryStatistics s = statsOf(database, "CREATE CONSTRAINT IF NOT EXISTS FOR (n:CoveredLabel) REQUIRE n.email IS UNIQUE");
    assertThat(s.getConstraintsAdded()).isEqualTo(1);
  }
}
