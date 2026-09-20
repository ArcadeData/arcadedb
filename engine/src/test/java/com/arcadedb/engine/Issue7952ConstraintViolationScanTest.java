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
package com.arcadedb.engine;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.DeferredExistenceChecks;
import com.arcadedb.database.MutableDocument;
import com.arcadedb.database.RID;
import com.arcadedb.event.AfterRecordReadListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Regression tests for issue #7952: a record that does not satisfy its own type's existence constraints
 * ({@code MANDATORY}, {@code NOTNULL}) has to be findable.
 * <p>
 * Since #7945 an openCypher {@code CREATE}/{@code MERGE} pattern may commit a record before the {@code SET} that
 * completes it has run, with {@link DeferredExistenceChecks} holding the constraint until the statement ends. That
 * bookkeeping is an in-memory, thread-bound scope, so a process that dies - or a connection that is killed -
 * between the provisional commit and the end-of-statement check leaves the incomplete record behind for good. It is
 * not the only way to reach that state: {@code ALTER PROPERTY ... MANDATORY TRUE} on a populated type reaches it
 * too, and #6127 showed {@code RESTORE} could before it started validating.
 * <p>
 * {@code CHECK DATABASE} now answers the question for all of them, and {@code FIX DELETE INVALID RECORDS} removes
 * what it finds - a clause of its own rather than part of {@code FIX}, because the schema-change route produces
 * records that hold real data.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7952ConstraintViolationScanTest extends TestHelper {

  /**
   * Most of these tests deliberately LEAVE a record violating its type's existence constraints in the database -
   * that is the state under test - and the shared end-of-test integrity assertion requires a run with no warnings
   * at all. Asserting the check's findings is what each test does itself.
   */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * The #7952 window itself: a scope is opened, a pattern creates a record missing the property a later clause was
   * going to supply, and the scope is abandoned without {@link DeferredExistenceChecks#check()} ever running - which
   * is what a process death or a torn-down thread leaves. The record survives, and before this change nothing in
   * the database could find it.
   */
  @Test
  void checkDatabaseFindsTheRecordAnAbandonedStatementLeftBehind() {
    createConstrainedDocumentType();

    final RID provisional = leaveProvisionalRecord("Record", 1);

    assertThat(db().lookupByRID(provisional, true)).as("the incomplete record is still in the database").isNotNull();

    final Map<String, Object> result = new DatabaseChecker(db()).setVerboseLevel(0).check();

    assertThat((Collection<RID>) result.get("constraintViolatingRecords")).containsExactly(provisional);
    assertThat((Long) result.get("totalConstraintViolations")).isEqualTo(1L);
    assertThat((Collection<String>) result.get("warnings"))
        .anyMatch(w -> w.contains(provisional.toString()) && w.contains("Record.orgId") && w.contains("mandatory"));
  }

  /** A NOTNULL violation is the other existence constraint and reads as its own finding. */
  @Test
  void checkDatabaseFindsANotNullViolation() {
    final DocumentType type = database.getSchema().createDocumentType("Record");
    type.createProperty("id", Type.INTEGER);
    type.createProperty("orgId", Type.STRING);

    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final MutableDocument doc = database.newDocument("Record").set("id", 1).set("orgId", null);
      doc.save();
      rid[0] = doc.getIdentity();
    });

    // The constraint arrives after the record does - the schema-change route to the same state.
    type.getProperty("orgId").setNotNull(true);

    final Map<String, Object> result = new DatabaseChecker(db()).setVerboseLevel(0).check();

    assertThat((Collection<RID>) result.get("constraintViolatingRecords")).containsExactly(rid[0]);
    assertThat((Collection<String>) result.get("warnings"))
        .anyMatch(w -> w.contains(rid[0].toString()) && w.contains("Record.orgId") && w.contains("null"));
  }

  /**
   * The other route to the same state, and the reason the repair is opt-in: making a property mandatory on a type
   * that already holds records does not validate them, so every one of them becomes a finding - while holding data
   * nobody would want a plain {@code FIX} to delete.
   */
  @Test
  void aSchemaChangeThatInvalidatesExistingRecordsIsReportedButNeverRemovedByPlainFix() {
    final DocumentType type = database.getSchema().createDocumentType("Record");
    type.createProperty("id", Type.INTEGER);

    database.transaction(() -> {
      for (int i = 0; i < 3; i++)
        database.newDocument("Record").set("id", i).save();
    });

    type.createProperty("orgId", Type.STRING).setMandatory(true);

    final Map<String, Object> reportOnly = new DatabaseChecker(db()).setVerboseLevel(0).check();
    assertThat((Long) reportOnly.get("totalConstraintViolations")).isEqualTo(3L);

    final Map<String, Object> fixed = new DatabaseChecker(db()).setFix(true).setVerboseLevel(0).check();
    assertThat((Long) fixed.get("totalConstraintViolations")).as("still found").isEqualTo(3L);
    assertThat((Collection<RID>) fixed.get("deletedConstraintViolatingRecords")).as("and still there").isEmpty();
    assertThat(database.countType("Record", false)).isEqualTo(3);
  }

  /** The opt-in repair: {@code FIX DELETE INVALID RECORDS} takes the records out. */
  @Test
  void deleteInvalidRecordsRemovesThem() {
    createConstrainedDocumentType();

    final RID provisional = leaveProvisionalRecord("Record", 1);

    database.transaction(() -> database.newDocument("Record").set("id", 2).set("orgId", "acme").save());

    final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setDeleteInvalidRecords(true)
        .setVerboseLevel(0).check();

    assertThat((Collection<RID>) result.get("deletedConstraintViolatingRecords")).containsExactly(provisional);
    assertThat((Long) result.get("totalDeletedConstraintViolatingRecords")).isEqualTo(1L);
    assertThat((Long) result.get("removedRecords")).isEqualTo(1L);
    assertThat(database.countType("Record", false)).as("the complete record is untouched").isEqualTo(1);
    assertThatThrownBy(() -> db().lookupByRID(provisional, true)).isInstanceOf(RecordNotFoundException.class);

    final Map<String, Object> after = new DatabaseChecker(db()).setVerboseLevel(0).check();
    assertThat((Long) after.get("totalConstraintViolations")).isZero();
  }

  /**
   * A vertex that violates a constraint is removed through the graph-aware delete, so its edges go with it rather
   * than being left dangling. Reached through the schema-change route, because it is the only one that can produce
   * an invalid vertex that already HAS edges: attaching an edge rewrites the vertex record, so the write path would
   * refuse it while the constraint is in force.
   */
  @Test
  void deleteInvalidRecordsRemovesAnInvalidVertexAndItsEdges() {
    final VertexType person = database.getSchema().createVertexType("Person");
    person.createProperty("id", Type.INTEGER);
    database.getSchema().createEdgeType("Knows");

    final RID[] vertices = new RID[2];
    database.transaction(() -> {
      vertices[0] = database.newVertex("Person").set("id", 1).set("orgId", "acme").save().getIdentity();
      vertices[1] = database.newVertex("Person").set("id", 2).save().getIdentity();
      db().lookupByRID(vertices[0], true).asVertex(true)
          .newEdge("Knows", db().lookupByRID(vertices[1], true).asVertex(true)).save();
    });

    assertThat(database.countType("Knows", false)).isEqualTo(1);

    person.createProperty("orgId", Type.STRING).setMandatory(true);
    final RID incomplete = vertices[1];

    final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setDeleteInvalidRecords(true)
        .setVerboseLevel(0).check();

    assertThat((Collection<RID>) result.get("deletedConstraintViolatingRecords")).containsExactly(incomplete);
    assertThat(database.countType("Person", false)).isEqualTo(1);
    assertThat(database.countType("Knows", false)).as("the edge went with the vertex").isZero();
  }

  /**
   * The race the repair itself creates, and the reason every record is re-validated inside the delete transaction
   * (PR review): the repair this check RECOMMENDS is an ordinary {@code UPDATE} supplying the missing property, so
   * a completed record meeting a {@code FIX DELETE INVALID RECORDS} that found it incomplete a moment earlier is
   * the other half of the feature rather than an exotic race. It must be left alone, with its data.
   * <p>
   * The window is entered where it actually is - the scan has committed, and the delete transaction is reading the
   * record back - by hooking the read that re-validation makes. An {@code AfterRecordReadListener} fires inside
   * {@code LocalDatabase.lookupByRID}, which is what re-validation calls and what the bucket scan deliberately does
   * NOT (it builds its records straight off the page view), so the scan still sees the incomplete record and only
   * the re-read sees the completed one. That is precisely the state a concurrent {@code UPDATE} produces, without a
   * second thread to race against.
   * <p>
   * NOT a vacuous test: the {@code beforeDelete} tripwire below fails it if the repair ever reaches the delete, and
   * removing the re-validation from {@code deleteConstraintViolatingRecord} makes it fire.
   */
  @Test
  void aRecordCompletedBetweenTheScanAndTheDeleteIsLeftAlone() {
    createConstrainedDocumentType();

    final RID provisional = leaveProvisionalRecord("Record", 1);

    final AtomicBoolean completed = new AtomicBoolean();
    final AfterRecordReadListener completeOnReadBack = record -> {
      if (!provisional.equals(record.getIdentity()) || !completed.compareAndSet(false, true))
        return record;
      // A mutable record returned here becomes the record's content, which is exactly what the UPDATE an operator
      // was told to run would have done a moment earlier.
      return record.asDocument(true).modify().set("orgId", "acme").save();
    };
    final BeforeRecordDeleteListener tripwire = record -> {
      if (provisional.equals(record.getIdentity()))
        fail("the repair must not delete a record that satisfies its constraints when it is re-read");
      return true;
    };

    database.getSchema().getType("Record").getEvents().registerListener(completeOnReadBack)
        .registerListener(tripwire);
    try {
      final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setDeleteInvalidRecords(true)
          .setVerboseLevel(0).check();

      assertThat(completed.get()).as("the re-read really did happen - otherwise this test proves nothing").isTrue();
      assertThat((Collection<RID>) result.get("deletedConstraintViolatingRecords")).isEmpty();
      assertThat((Long) result.get("totalDeletedConstraintViolatingRecords")).isZero();
      assertThat((Long) result.get("removedRecords")).isZero();
      assertThat(database.countType("Record", false)).as("the completed record keeps its data").isEqualTo(1);
      assertThat((Collection<String>) result.get("warnings"))
          .anyMatch(w -> w.contains(provisional.toString()) && w.contains("left in place"));
    } finally {
      database.getSchema().getType("Record").getEvents().unregisterListener(completeOnReadBack)
          .unregisterListener(tripwire);
    }

    // The completing write was persisted, so a fresh run finds nothing left to report either.
    final Map<String, Object> after = new DatabaseChecker(db()).setVerboseLevel(0).check();
    assertThat((Long) after.get("totalConstraintViolations")).isZero();
    assertThat(db().lookupByRID(provisional, true).asDocument(true).getString("orgId")).isEqualTo("acme");
  }

  /**
   * The repair walks a bucket again when one pass could not hold every record it had to take out (PR review): the
   * pending-removal list is bounded so it cannot exhaust the heap, and the bound must not cost completeness - an
   * operator who asked for these records to go must not get some of them removed and no word about the rest.
   * <p>
   * Driven through the package-private bound rather than by writing a million records, which is the only way to
   * reach the loop at a size a test can afford.
   */
  @Test
  void aBucketWithMoreViolationsThanOnePassCanHoldIsWalkedAgain() {
    final DocumentType type = database.getSchema().createDocumentType("Record");
    type.createProperty("id", Type.INTEGER);

    database.transaction(() -> {
      for (int i = 0; i < 7; i++)
        database.newDocument("Record").set("id", i).save();
    });
    type.createProperty("orgId", Type.STRING).setMandatory(true);

    final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setDeleteInvalidRecords(true)
        .setInvalidRecordsPerRepairPass(2).setVerboseLevel(0).check();

    assertThat((Long) result.get("totalConstraintViolations")).as("every record is reported ONCE").isEqualTo(7L);
    assertThat((Long) result.get("totalDeletedConstraintViolatingRecords")).as("and every one removed").isEqualTo(7L);
    assertThat((Collection<RID>) result.get("constraintViolatingRecords")).hasSize(7);
    assertThat(database.countType("Record", false)).isZero();
    assertThat((Collection<String>) result.get("warnings"))
        .as("nothing is left, so the repair advice is not printed")
        .noneMatch(w -> w.contains("CHECK DATABASE FIX DELETE INVALID RECORDS"));
  }

  /**
   * The same loop must terminate when the records it collects cannot be removed at all - a {@code beforeDelete}
   * listener refusing every delete would otherwise have it re-collect the same full batch for ever.
   */
  @Test
  @Timeout(60)
  void theRepairPassLoopStopsWhenNothingCanBeRemoved() {
    final DocumentType type = database.getSchema().createDocumentType("Record");
    type.createProperty("id", Type.INTEGER);

    database.transaction(() -> {
      for (int i = 0; i < 5; i++)
        database.newDocument("Record").set("id", i).save();
    });
    type.createProperty("orgId", Type.STRING).setMandatory(true);

    final BeforeRecordDeleteListener veto = record -> false;
    database.getSchema().getType("Record").getEvents().registerListener(veto);
    try {
      final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setDeleteInvalidRecords(true)
          .setInvalidRecordsPerRepairPass(1).setVerboseLevel(0).check();

      assertThat((Long) result.get("totalDeletedConstraintViolatingRecords")).isZero();
      assertThat(database.countType("Record", false)).isEqualTo(5);
      assertThat((Collection<String>) result.get("warnings"))
          .anyMatch(w -> w.contains("CHECK DATABASE FIX DELETE INVALID RECORDS"));
    } finally {
      database.getSchema().getType("Record").getEvents().unregisterListener(veto);
    }
  }

  /**
   * The other half of re-validating against the LIVE type (PR review): the record is unchanged, but the constraint
   * it violated is gone by the time the repair reaches it, so there is nothing left to condemn it for and it must
   * survive. Re-reading the constrained properties from the array the scan was planned with would delete it.
   * <p>
   * Same hook as the concurrent-completion test and for the same reason - an {@code AfterRecordReadListener} fires
   * on the re-validation's {@code lookupByRID} and not on the bucket scan - except that here the listener changes
   * the SCHEMA and hands the record back untouched.
   */
  @Test
  void aRecordWhoseConstraintIsDroppedBeforeTheRepairReachesItIsLeftAlone() {
    createConstrainedDocumentType();

    final RID provisional = leaveProvisionalRecord("Record", 1);

    final AtomicBoolean dropped = new AtomicBoolean();
    final AfterRecordReadListener dropTheConstraint = record -> {
      if (provisional.equals(record.getIdentity()) && dropped.compareAndSet(false, true))
        database.getSchema().getType("Record").getProperty("orgId").setMandatory(false);
      return record;
    };
    final BeforeRecordDeleteListener tripwire = record -> {
      if (provisional.equals(record.getIdentity()))
        fail("the repair must not delete a record whose type no longer constrains the missing property");
      return true;
    };

    database.getSchema().getType("Record").getEvents().registerListener(dropTheConstraint)
        .registerListener(tripwire);
    try {
      final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setDeleteInvalidRecords(true)
          .setVerboseLevel(0).check();

      assertThat(dropped.get()).as("the re-read really did happen - otherwise this test proves nothing").isTrue();
      assertThat((Long) result.get("totalConstraintViolations")).as("the scan still found it").isEqualTo(1L);
      assertThat((Long) result.get("totalDeletedConstraintViolatingRecords")).isZero();
      assertThat(database.countType("Record", false)).as("but nothing removed it").isEqualTo(1);
      assertThat((Collection<String>) result.get("warnings"))
          .anyMatch(w -> w.contains(provisional.toString()) && w.contains("left in place"));
    } finally {
      database.getSchema().getType("Record").getEvents().unregisterListener(dropTheConstraint)
          .unregisterListener(tripwire);
    }
  }

  /**
   * A {@code beforeDelete} listener can refuse a delete, and {@code LocalDatabase.deleteRecord} reports that by
   * returning without deleting rather than by throwing. The report must not claim a record is gone that a trigger
   * deliberately kept.
   */
  @Test
  void aRemovalRefusedByABeforeDeleteListenerIsNotCountedAsRemoved() {
    createConstrainedDocumentType();

    final RID provisional = leaveProvisionalRecord("Record", 1);

    final BeforeRecordDeleteListener veto = record -> false;
    database.getSchema().getType("Record").getEvents().registerListener(veto);
    try {
      final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setDeleteInvalidRecords(true)
          .setVerboseLevel(0).check();

      assertThat((Long) result.get("totalDeletedConstraintViolatingRecords")).isZero();
      assertThat((Collection<RID>) result.get("deletedConstraintViolatingRecords")).isEmpty();
      assertThat((Long) result.get("removedRecords")).isZero();
      assertThat(database.countType("Record", false)).as("the listener kept it").isEqualTo(1);
      assertThat((Collection<String>) result.get("warnings"))
          .anyMatch(w -> w.contains(provisional.toString()) && w.contains("beforeDelete"));
      // Still in that state, so the summary still tells the operator what to do about it.
      assertThat((Collection<String>) result.get("warnings"))
          .anyMatch(w -> w.contains("CHECK DATABASE FIX DELETE INVALID RECORDS"));
    } finally {
      database.getSchema().getType("Record").getEvents().unregisterListener(veto);
    }
  }

  /** A constraint violation is not corruption: the record loads fine and its index entries are correct. */
  @Test
  void aConstraintViolationIsNotReportedAsCorruption() {
    createConstrainedDocumentType();

    final RID provisional = leaveProvisionalRecord("Record", 1);

    final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setVerboseLevel(0).check();

    assertThat((Collection<RID>) result.get("corruptedRecords")).doesNotContain(provisional);
    assertThat((Long) result.get("totalCorruptedRecords")).isZero();
    assertThat((Collection<String>) result.get("rebuiltIndexes")).isEmpty();
  }

  /** A database whose types declare no existence constraint publishes the keys and pays for no pass at all. */
  @Test
  void aDatabaseWithNoExistenceConstraintReportsZero() {
    final DocumentType type = database.getSchema().createDocumentType("Record");
    type.createProperty("id", Type.INTEGER);

    database.transaction(() -> database.newDocument("Record").set("id", 1).save());

    final Map<String, Object> result = new DatabaseChecker(db()).setVerboseLevel(0).check();

    assertThat(result).containsKey("constraintViolatingRecords").containsKey("deletedConstraintViolatingRecords");
    assertThat((Long) result.get("totalConstraintViolations")).isZero();
    assertThat((Collection<String>) result.get("warnings"))
        .as("the pass never ran, so it says nothing").noneMatch(w -> w.contains("existence constraint"));
  }

  /** The {@code RECORD} scope answers the same question about the records it names. */
  @Test
  void theRecordScopeReportsAConstraintViolation() {
    createConstrainedDocumentType();

    final RID provisional = leaveProvisionalRecord("Record", 1);

    final Map<String, Object> result = new DatabaseChecker(db()).setRecords(Set.of(provisional))
        .setVerboseLevel(0).check();

    assertThat((Collection<RID>) result.get("constraintViolatingRecords")).containsExactly(provisional);
    assertThat((Long) result.get("totalConstraintViolations")).isEqualTo(1L);
  }

  /**
   * The {@code RECORD}-scoped arm has its own delete path - its own grouping of the named records by bucket, its own
   * {@link com.arcadedb.engine.DatabaseChecker} pending-removal lists and its own per-bucket delete loop - so it
   * needs its own proof that it removes what it names (PR review). Two types, so the grouping really is exercised
   * across more than one bucket, and a third record left unnamed so the scope is shown to bound the repair too.
   */
  @Test
  void theRecordScopeRemovesTheRecordsItNamesAndOnlyThose() {
    createConstrainedDocumentType();
    final DocumentType other = database.getSchema().createDocumentType("Other");
    other.createProperty("id", Type.INTEGER);
    other.createProperty("orgId", Type.STRING).setMandatory(true);

    final RID inRecord = leaveProvisionalRecord("Record", 1);
    final RID inOther = leaveProvisionalRecord("Other", 2);
    final RID notNamed = leaveProvisionalRecord("Record", 3);

    assertThat(inRecord.getBucketId()).as("the two named records live in different buckets")
        .isNotEqualTo(inOther.getBucketId());

    final Map<String, Object> result = new DatabaseChecker(db()).setRecords(Set.of(inRecord, inOther))
        .setFix(true).setDeleteInvalidRecords(true).setVerboseLevel(0).check();

    assertThat((Collection<RID>) result.get("deletedConstraintViolatingRecords"))
        .containsExactlyInAnyOrder(inRecord, inOther);
    assertThat((Long) result.get("totalDeletedConstraintViolatingRecords")).isEqualTo(2L);
    assertThat((Long) result.get("removedRecords")).isEqualTo(2L);
    assertThatThrownBy(() -> db().lookupByRID(inRecord, true)).isInstanceOf(RecordNotFoundException.class);
    assertThatThrownBy(() -> db().lookupByRID(inOther, true)).isInstanceOf(RecordNotFoundException.class);

    assertThat(db().lookupByRID(notNamed, true)).as("a record the scope did not name is untouched").isNotNull();
    assertThat(database.countType("Record", false)).isEqualTo(1);
    assertThat(database.countType("Other", false)).isZero();
  }

  /** The {@code TYPE} scope narrows the pass the same way it narrows every other one. */
  @Test
  void theTypeScopeNarrowsThePass() {
    createConstrainedDocumentType();
    final DocumentType other = database.getSchema().createDocumentType("Other");
    other.createProperty("id", Type.INTEGER);
    other.createProperty("orgId", Type.STRING).setMandatory(true);

    final RID inRecord = leaveProvisionalRecord("Record", 1);
    leaveProvisionalRecord("Other", 2);

    final Map<String, Object> result = new DatabaseChecker(db()).setTypes(Set.of("Record"))
        .setVerboseLevel(0).check();

    assertThat((Collection<RID>) result.get("constraintViolatingRecords")).containsExactly(inRecord);
  }

  /** The other repair, and the one an operator reaches for when the record holds data: complete it in one update. */
  @Test
  void anIncompleteRecordCanBeCompletedByASingleUpdate() {
    createConstrainedDocumentType();

    final RID provisional = leaveProvisionalRecord("Record", 1);

    database.transaction(() -> database.command("sql", "UPDATE `Record` SET orgId = 'acme' WHERE @rid = ?", provisional));

    final Map<String, Object> result = new DatabaseChecker(db()).setVerboseLevel(0).check();
    assertThat((Long) result.get("totalConstraintViolations")).isZero();
  }

  /** {@code DELETE INVALID RECORDS} removes records, so - like {@code DELETE ORPHANS} - it refuses to imply FIX. */
  @Test
  void deleteInvalidRecordsWithoutFixIsRefused() {
    createConstrainedDocumentType();

    assertThatThrownBy(() -> database.command("sql", "CHECK DATABASE DELETE INVALID RECORDS"))
        .hasMessageContaining("requires FIX");
  }

  /** End to end through SQL, which is how an operator actually reaches it. */
  @Test
  void checkDatabaseSqlReportsAndRepairs() {
    createConstrainedDocumentType();

    final RID provisional = leaveProvisionalRecord("Record", 1);

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result row = rs.next();
      assertThat((Long) row.getProperty("totalConstraintViolations")).isEqualTo(1L);
      assertThat((Collection<RID>) row.getProperty("constraintViolatingRecords")).containsExactly(provisional);
    }

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX DELETE INVALID RECORDS")) {
      final Result row = rs.next();
      assertThat((Collection<RID>) row.getProperty("deletedConstraintViolatingRecords")).containsExactly(provisional);
    }

    assertThat(database.countType("Record", false)).isZero();
  }

  private void createConstrainedDocumentType() {
    final DocumentType type = database.getSchema().createDocumentType("Record");
    type.createProperty("id", Type.INTEGER);
    type.createProperty("orgId", Type.STRING).setMandatory(true);
  }

  /**
   * Reproduces the #7952 window exactly: a {@link DeferredExistenceChecks} scope is opened, a pattern-create region
   * writes a record that does not satisfy its type's existence constraints, and the scope is then abandoned without
   * {@code check()} - which is what the process dying, or the connection being killed, between the provisional
   * commit and the end of the statement leaves behind.
   */
  private RID leaveProvisionalRecord(final String typeName, final int id) {
    // begin() first, so the thread has a DatabaseContext for the scope to attach to, exactly as it does when an
    // openCypher plan opens one.
    database.begin();
    final DeferredExistenceChecks scope = DeferredExistenceChecks.open(db());
    final RID rid;
    try (final DeferredExistenceChecks.PatternCreate ignored = DeferredExistenceChecks.patternCreate(db())) {
      final MutableDocument doc = database.newDocument(typeName);
      doc.set("id", id);
      doc.save();
      rid = doc.getIdentity();
    }
    // The write step commits - the openCypher pipeline auto-commits per step - and then the statement never
    // reaches its end, so check() never runs.
    database.commit();
    scope.close();
    return rid;
  }

  private DatabaseInternal db() {
    return (DatabaseInternal) database;
  }
}
