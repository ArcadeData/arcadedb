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
import com.arcadedb.database.MutableDocument;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.schema.Type;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.utility.StallAwareStopwatch;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression for issue #7945: an existence constraint ({@code REQUIRE n.prop IS NOT NULL}, which ArcadeDB models as
 * a MANDATORY property) was enforced the instant {@code MERGE}/{@code CREATE} wrote the node, before the {@code SET}
 * of the very same statement had supplied the property. That made the constraint unusable with
 * {@code MERGE (n:L {id: $id}) SET n.prop = $v}, the standard openCypher upsert - which is exactly how Neo4j, the
 * reference implementation, is used, and Neo4j accepts it because existence constraints are enforced once the write
 * has fully applied.
 * <p>
 * The tests below pin both halves: the deferral itself, and the fact that nothing else about the constraint moved -
 * a statement that never supplies the property still fails and still leaves no record behind, and a write that is
 * not part of the statement that created the record is validated as eagerly as it always was.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CypherExistenceConstraintWithSetIssue7945Test {
  private Database database;

  @BeforeEach
  void setUp() {
    final DatabaseFactory factory = new DatabaseFactory("./target/databases/issue-7945");
    if (factory.exists())
      factory.open().drop();
    database = factory.create();
    database.command("cypher", "CREATE CONSTRAINT rec_org_exists IF NOT EXISTS FOR (n:Record) REQUIRE n.orgId IS NOT NULL");
  }

  @AfterEach
  void tearDown() {
    if (database != null) {
      database.drop();
      database = null;
    }
  }

  @Test
  void mergeThenSetPopulatesTheRequiredProperty() {
    try (final ResultSet rs = database.command("cypher", "MERGE (n:Record {id: $id}) SET n.orgId = $orgId RETURN n.orgId AS o",
        Map.of("id", "r1", "orgId", "org-1"))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("o")).isEqualTo("org-1");
    }
    assertThat(countRecords()).isEqualTo(1);
  }

  @Test
  void createThenSetPopulatesTheRequiredProperty() {
    try (final ResultSet rs = database.command("cypher", "CREATE (n:Record {id: $id}) SET n.orgId = $orgId RETURN n.orgId AS o",
        Map.of("id", "r2", "orgId", "org-2"))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("o")).isEqualTo("org-2");
    }
    assertThat(countRecords()).isEqualTo(1);
  }

  /**
   * The upsert run twice: the second run takes the MATCH branch, which never creates and so never defers.
   */
  @Test
  void theUpsertIsIdempotent() {
    for (int i = 0; i < 2; i++)
      database.command("cypher", "MERGE (n:Record {id: $id}) SET n.orgId = $orgId",
          Map.of("id", "r3", "orgId", "org-" + i)).close();

    assertThat(countRecords()).isEqualTo(1);
    try (final ResultSet rs = database.query("cypher", "MATCH (n:Record {id: 'r3'}) RETURN n.orgId AS o")) {
      assertThat(rs.next().<String>getProperty("o")).isEqualTo("org-1");
    }
  }

  /**
   * A property the constraint requires may be supplied by any clause of the statement, not only by one that
   * immediately follows the creation.
   */
  @Test
  void aLaterClauseMaySupplyTheProperty() {
    database.command("cypher", "MERGE (n:Record {id: $id}) SET n.name = 'first' SET n.orgId = $orgId",
        Map.of("id", "r4", "orgId", "org-4")).close();

    try (final ResultSet rs = database.query("cypher", "MATCH (n:Record {id: 'r4'}) RETURN n.orgId AS o, n.name AS n")) {
      final Result row = rs.next();
      assertThat(row.<String>getProperty("o")).isEqualTo("org-4");
      assertThat(row.<String>getProperty("n")).isEqualTo("first");
    }
  }

  /**
   * The constraint is deferred, not dropped: a statement that ends without supplying the property still fails, and
   * the provisional record it created is taken back rather than left behind for good - a record that violates its
   * own type's constraints could not be updated afterwards either.
   */
  @Test
  void aStatementThatNeverSuppliesThePropertyStillFailsAndLeavesNothingBehind() {
    assertThatThrownBy(() -> database.command("cypher", "CREATE (n:Record {id: $id})", Map.of("id", "r5")))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class)
        .hasMessageContaining("orgId");

    assertThat(countRecords()).isZero();
  }

  @Test
  void aMergeThatNeverSuppliesThePropertyStillFailsAndLeavesNothingBehind() {
    assertThatThrownBy(() -> database.command("cypher", "MERGE (n:Record {id: $id})", Map.of("id", "r6")))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class)
        .hasMessageContaining("orgId");

    assertThat(countRecords()).isZero();
  }

  /**
   * Once a clause of the statement has supplied the property, the record is no longer provisional: a later clause
   * clearing it again is an ordinary write that leaves a complete record incomplete, and fails where any such write
   * does - at the write. What survives is the record as the completing clause left it, which satisfies the
   * constraint. (That the earlier clauses of a failed statement survive at all is the openCypher pipeline's
   * long-standing auto-commit behaviour, unchanged here: a write step commits its own transaction.)
   */
  @Test
  void clearingThePropertyAgainLaterInTheStatementFailsAtTheWrite() {
    assertThatThrownBy(() -> database.command("cypher", "CREATE (n:Record {id: $id}) SET n.orgId = 'x' SET n.orgId = null",
        Map.of("id", "r7")))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class);

    try (final ResultSet rs = database.query("cypher", "MATCH (n:Record {id: 'r7'}) RETURN n.orgId AS o")) {
      assertThat(rs.next().<String>getProperty("o")).isEqualTo("x");
    }
  }

  /**
   * A statement that fails before ever completing the record it created takes that record back: the creation was
   * provisional, and nothing that never satisfied its existence constraints outlives the statement that wrote it.
   */
  @Test
  void aStatementThatFailsWhileTheRecordIsStillProvisionalTakesItBack() {
    assertThatThrownBy(() -> database.command("cypher", "CREATE (n:Record {id: $id}) SET n.orgId = 1 / 0",
        Map.of("id", "r11")))
        .isInstanceOf(CommandExecutionException.class);

    assertThat(countRecords()).isZero();
  }

  /**
   * A record this statement did not create is a complete write of its own: clearing a required property on it fails
   * where it always did, at the write, and the earlier value survives.
   */
  @Test
  void clearingTheRequiredPropertyOfAnExistingRecordStillFailsEagerly() {
    database.command("cypher", "CREATE (n:Record {id: 'r8', orgId: 'org-8'})").close();

    assertThatThrownBy(() -> database.command("cypher", "MATCH (n:Record {id: 'r8'}) SET n.orgId = null"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class);

    try (final ResultSet rs = database.query("cypher", "MATCH (n:Record {id: 'r8'}) RETURN n.orgId AS o")) {
      assertThat(rs.next().<String>getProperty("o")).isEqualTo("org-8");
    }
  }

  /**
   * The deferral belongs to the openCypher statement that opened it, and to nothing else: the SQL engine and the
   * record API validate at the write, as they always have.
   */
  @Test
  void theRecordApiStillValidatesEagerly() {
    assertThatThrownBy(() -> database.transaction(() -> database.newVertex("Record").set("id", "r9").save()))
        .isInstanceOf(ValidationException.class);

    assertThat(countRecords()).isZero();
  }

  @Test
  void theSqlEngineStillValidatesEagerly() {
    assertThatThrownBy(() -> database.command("sql", "INSERT INTO Record SET id = 'r10'"))
        .isInstanceOf(ValidationException.class);

    assertThat(countRecords()).isZero();
  }

  /**
   * A whole path is one provisional region - the vertices and the relationship between them - so a statement that
   * completes only one side must not leave the other half of a freshly created path behind. Deleting the vertex
   * goes through the ordinary delete, which detaches its edges, so what survives is a consistent graph rather than
   * a dangling relationship.
   */
  @Test
  void aPathWhoseVertexIsNeverCompletedLeavesNoHalfOfItBehind() {
    database.command("cypher", "CREATE CONSTRAINT rel_since_exists IF NOT EXISTS FOR ()-[r:KNOWS]-() REQUIRE r.since IS NOT NULL")
        .close();

    // The relationship is completed by the SET, the vertices never are.
    assertThatThrownBy(() -> database.command("cypher",
        "CREATE (a:Record {id: 'p1'})-[r:KNOWS]->(b:Record {id: 'p2'}) SET r.since = 2020"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class)
        .hasMessageContaining("orgId");

    assertThat(countRecords()).isZero();
    try (final ResultSet rs = database.query("cypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  /**
   * The same partial completion through MERGE rather than CREATE. It is worth its own test because the two are
   * structured differently: CreateStep opens one pattern-create region around the whole path, while MergeStep
   * opens one per vertex and one per relationship. The depth is only ever asked "greater than zero?" at the
   * moment an element is written, so both shapes must answer the same way.
   */
  @Test
  void aMergedPathWhoseElementsAreNeverCompletedLeavesNothingBehind() {
    database.command("cypher", "CREATE CONSTRAINT rel_since_exists IF NOT EXISTS FOR ()-[r:KNOWS]-() REQUIRE r.since IS NOT NULL")
        .close();

    assertThatThrownBy(() -> database.command("cypher",
        "MERGE (a:Record {id: 'm1'})-[r:KNOWS]->(b:Record {id: 'm2'})"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class);

    assertThat(countRecords()).isZero();
    try (final ResultSet rs = database.query("cypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  /**
   * A MERGE path whose vertices are completed but whose relationship is not: only the relationship is taken back.
   */
  @Test
  void aMergedPathKeepsTheCompletedVerticesWhenTheRelationshipIsNot() {
    database.command("cypher", "CREATE CONSTRAINT rel_since_exists IF NOT EXISTS FOR ()-[r:KNOWS]-() REQUIRE r.since IS NOT NULL")
        .close();

    assertThatThrownBy(() -> database.command("cypher",
        "MERGE (a:Record {id: 'm3'})-[r:KNOWS]->(b:Record {id: 'm4'}) SET a.orgId = 'o', b.orgId = 'o'"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class)
        .hasMessageContaining("since");

    assertThat(countRecords()).isEqualTo(2);
    try (final ResultSet rs = database.query("cypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  /**
   * The mirror image: the vertices are completed and the relationship is not, so only the relationship is taken
   * back and the two nodes stay.
   */
  @Test
  void aPathWhoseRelationshipIsNeverCompletedKeepsTheCompletedVertices() {
    database.command("cypher", "CREATE CONSTRAINT rel_since_exists IF NOT EXISTS FOR ()-[r:KNOWS]-() REQUIRE r.since IS NOT NULL")
        .close();

    assertThatThrownBy(() -> database.command("cypher",
        "CREATE (a:Record {id: 'p3'})-[r:KNOWS]->(b:Record {id: 'p4'}) SET a.orgId = 'o', b.orgId = 'o'"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class)
        .hasMessageContaining("since");

    assertThat(countRecords()).isEqualTo(2);
    try (final ResultSet rs = database.query("cypher", "MATCH ()-[r:KNOWS]->() RETURN count(r) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  /**
   * A bulk upsert: every row creates a provisional record that the SET a few steps later completes. Pinned because
   * the pending set is swept rather than grown, so this shape must not depend on how many rows it carries.
   */
  @Test
  void aBulkUpsertCompletesEveryRow() {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 2_500; i++)
      rows.add(Map.of("id", "bulk-" + i, "org", "org-" + i));

    database.command("cypher", "UNWIND $rows AS r MERGE (n:Record {id: r.id}) SET n.orgId = r.org",
        Map.of("rows", rows)).close();

    assertThat(countRecords()).isEqualTo(rows.size());
    try (final ResultSet rs = database.query("cypher", "MATCH (n:Record) WHERE n.orgId IS NULL RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  /**
   * The same bulk shape on {@code CREATE} rather than {@code MERGE}, which is the harder one: {@code CreateStep}
   * batches (OPENCYPHER_BULK_CREATE_BATCH_SIZE defaults to 20,000) and runs every pattern creation of a batch
   * before yielding a single row downstream, so all of these records are provisional at once, before any of them
   * reaches the {@code SET} that completes it. The pending set therefore grows to the batch rather than to the
   * pull window, which is exactly the case the sweep has to stay linear for.
   */
  @Test
  void aBulkCreateThenSetCompletesEveryRow() {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 3_000; i++)
      rows.add(Map.of("id", "batch-" + i, "org", "org-" + i));

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    database.command("cypher", "UNWIND $rows AS r CREATE (n:Record {id: r.id}) SET n.orgId = r.org",
        Map.of("rows", rows)).close();

    stopwatch.assertGaveUpWithin(60_000, "a linear end-of-statement sweep from a quadratic one");

    assertThat(countRecords()).isEqualTo(rows.size());
    try (final ResultSet rs = database.query("cypher", "MATCH (n:Record) WHERE n.orgId IS NULL RETURN count(n) AS c")) {
      assertThat(rs.next().<Number>getProperty("c").longValue()).isZero();
    }
  }

  /**
   * The mistake the deferral makes possible: a bulk write that never supplies the required property at all. Every
   * row stays provisional to the end, which is the one shape where the pending set grows instead of draining.
   * <p>
   * The bound is a tripwire between a bounded operation and an unbounded one, not a latency budget: the sweep that
   * keeps the pending set honest used to run on every registration past the first thousand, re-reading the whole
   * (growing) set each time, which is quadratic in the number of rows. It is generous on purpose - what must not
   * happen here is minutes, not milliseconds.
   */
  @Test
  void aBulkWriteThatCompletesNothingStillFailsQuickly() {
    final List<Map<String, Object>> rows = new ArrayList<>();
    for (int i = 0; i < 5_000; i++)
      rows.add(Map.of("id", "never-" + i));

    final StallAwareStopwatch stopwatch = StallAwareStopwatch.start();

    assertThatThrownBy(() -> database.command("cypher", "UNWIND $rows AS r CREATE (n:Record {id: r.id})",
        Map.of("rows", rows)))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class);

    stopwatch.assertGaveUpWithin(60_000, "a linear end-of-statement sweep from a quadratic one");

    // Nothing this statement created satisfied its constraints, so nothing it created survives.
    assertThat(countRecords()).isZero();
  }

  /**
   * Inside an explicit transaction the provisional records are the caller's to keep or discard: the statement
   * fails, its own creations are taken back, and a COMMIT the client issues anyway commits what is left rather
   * than the incomplete records.
   */
  @Test
  void insideAnExplicitTransactionTheFailedStatementLeavesNothingToCommit() {
    database.command("cypher", "START TRANSACTION").close();
    database.command("cypher", "CREATE (n:Record {id: 'tx-ok', orgId: 'org'})").close();

    assertThatThrownBy(() -> database.command("cypher", "CREATE (n:Record {id: 'tx-bad'})"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class);

    database.command("cypher", "COMMIT").close();

    assertThat(countRecords()).isEqualTo(1);
    try (final ResultSet rs = database.query("cypher", "MATCH (n:Record) RETURN n.id AS id")) {
      assertThat(rs.next().<String>getProperty("id")).isEqualTo("tx-ok");
    }
  }

  /**
   * The invariant the end-of-statement check rests on: a write statement is drained to completion before
   * {@code command()} returns, so the moment after it is the end of the statement. Pinned behaviourally - the
   * record is there before the caller has pulled a single row - because if writes ever became lazy again the
   * deferred check would run before the writes it is meant to check (the profile path was converted from draining
   * to streaming once already, in #7330).
   */
  @Test
  void aWriteStatementIsDrainedBeforeItsResultIsReturned() {
    try (final ResultSet unconsumed = database.command("cypher",
        "CREATE (n:Record {id: 'drained', orgId: 'org'}) RETURN n")) {
      // Deliberately not pulled: the write must already have happened.
      assertThat(countRecords()).isEqualTo(1);
      assertThat(unconsumed.hasNext()).isTrue();
    }
  }

  /**
   * A record can be provisional on one property and break a rule that is not deferrable at all on another. The
   * value-shaped constraints (here a max) still fail at the write, and the provisional record the statement had
   * already created is taken back on the way out - the discard path rather than the end-of-statement check.
   */
  @Test
  void aProvisionalRecordIsTakenBackWhenANonDeferrableConstraintFails() {
    database.getSchema().getType("Record").createProperty("score", Type.INTEGER).setMax("10");

    assertThatThrownBy(() -> database.command("cypher", "CREATE (n:Record {id: 'r12'}) SET n.score = 99"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class)
        .hasMessageContaining("score");

    assertThat(countRecords()).isZero();
  }

  /**
   * An embedded document has no identity and never will, so it can never be completed by a later clause and must
   * not be deferred by one. Its owner being a pattern element is what would otherwise put its validation inside
   * the pattern-create region.
   */
  @Test
  void anEmbeddedDocumentStillValidatesEagerly() {
    database.getSchema().createDocumentType("Tag").createProperty("label", Type.STRING).setMandatory(true);
    database.getSchema().getType("Record").createProperty("tag", Type.EMBEDDED).setOfType("Tag");

    assertThatThrownBy(() -> database.transaction(() -> {
      final MutableDocument record = database.newVertex("Record").set("id", "r13").set("orgId", "org");
      record.set("tag", record.newEmbeddedDocument("Tag", "tag"));
      record.save();
    })).isInstanceOf(ValidationException.class).hasMessageContaining("Tag.label");

    assertThat(countRecords()).isZero();
  }

  /**
   * The same upsert under PROFILE, which takes a different branch of the engine
   * ({@code plan.profile()} rather than {@code plan.execute()}). Both drain a write statement before returning,
   * which is what the end-of-statement check rests on - and the profile path is the one that was converted from
   * draining to streaming once before (#7330), so the combination is worth pinning rather than assuming.
   */
  @Test
  void theDeferredCheckStillAppliesUnderProfile() {
    try (final ResultSet rs = database.command("cypher",
        "PROFILE MERGE (n:Record {id: $id}) SET n.orgId = $orgId RETURN n.orgId AS o",
        Map.of("id", "prof-1", "orgId", "org-p"))) {
      assertThat(rs.hasNext()).isTrue();
      assertThat(rs.next().<String>getProperty("o")).isEqualTo("org-p");
    }
    assertThat(countRecords()).isEqualTo(1);

    // And a profiled statement that never completes its record still fails, and still leaves nothing behind.
    assertThatThrownBy(() -> database.command("cypher", "PROFILE CREATE (n:Record {id: 'prof-2'})"))
        .isInstanceOf(CommandExecutionException.class)
        .rootCause().isInstanceOf(ValidationException.class);

    assertThat(countRecords()).isEqualTo(1);
  }

  private long countRecords() {
    try (final ResultSet rs = database.query("cypher", "MATCH (n:Record) RETURN count(n) AS c")) {
      return rs.next().<Number>getProperty("c").longValue();
    }
  }
}
