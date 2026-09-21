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
package com.arcadedb.query.sql.executor;

import com.arcadedb.TestHelper;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.database.Record;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.AfterRecordCreateListener;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.DatabaseOperationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.ValidationException;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.EdgeType;
import com.arcadedb.schema.Type;
import com.arcadedb.schema.VertexType;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Issue #6127 items 1 and 2: {@code RESTORE DOCUMENT/VERTEX/EDGE} used to skip {@code setDefaultValues()} /
 * {@code validate()} and to fire no create event, so an ordinary successful statement could persist a record its
 * own type forbids - one that could then never be UPDATEd (the update path validates too) and that
 * {@code CHECK DATABASE}, being a structural check, never flagged. The restore path now applies the same schema
 * contract and the same create events a plain INSERT does, and since #7952 the check reports such a record wherever
 * it came from.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class RestoreConstraintsAndEventsTest extends TestHelper {

  private static final String CONSTRAINED = "ConstrainedDoc";
  private static final String PLAIN       = "PlainDoc";
  private static final String VERTEX      = "ConstrainedVertex";
  private static final String EDGE        = "ConstrainedEdge";

  @Override
  protected void beginTest() {
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType(CONSTRAINED);
      type.createProperty("name", Type.STRING).setMandatory(true);
      // Quoted: a default value is evaluated as an SQL expression, so a bare `active` would resolve to a field name.
      type.createProperty("status", Type.STRING).setDefaultValue("'active'");
      type.createProperty("note", Type.STRING);

      database.getSchema().createDocumentType(PLAIN).createProperty("v", Type.STRING);

      final VertexType vertexType = database.getSchema().createVertexType(VERTEX);
      vertexType.createProperty("label", Type.STRING).setMandatory(true);

      final EdgeType edgeType = database.getSchema().createEdgeType(EDGE);
      edgeType.createProperty("since", Type.STRING).setMandatory(true);
      edgeType.createProperty("weight", Type.STRING).setDefaultValue("'1'");
    });
  }

  /**
   * The edge arm builds its shell with {@code new MutableEdge(...)} rather than {@code database.newDocument} /
   * {@code newVertex}, so it reaches {@code restoreRecord} down a different construction path and needs its own
   * proof that validation, default values and the create events all apply there too (PR #6130 review).
   */
  @Test
  void restoreEdgeAppliesTheSchemaAndFiresTheCreateEvents() {
    final RID[] endpoints = new RID[2];
    final RID[] edge = new RID[1];
    database.transaction(() -> {
      final MutableVertex from = database.newVertex(VERTEX).set("label", "from").save();
      final MutableVertex to = database.newVertex(VERTEX).set("label", "to").save();
      endpoints[0] = from.getIdentity();
      endpoints[1] = to.getIdentity();
      // The properties go through newEdge: it saves the edge itself, so a later set() would be validated too late.
      edge[0] = from.newEdge(EDGE, to, "since", "yesterday").getIdentity();
    });

    final LocalBucket bucket = (LocalBucket) ((DatabaseInternal) database).getSchema().getBucketById(edge[0].getBucketId());
    database.transaction(() -> bucket.deleteRecord(edge[0]));

    final String restore =
        "RESTORE EDGE " + EDGE + " RID " + edge[0] + " FROM " + endpoints[0] + " TO " + endpoints[1];

    // The mandatory property is enforced on the edge shell too.
    database.transaction(() -> assertThatThrownBy(() -> database.command("sql", restore))//
        .isInstanceOf(ValidationException.class).hasMessageContaining("since"));

    final AtomicInteger after = new AtomicInteger();
    final AfterRecordCreateListener afterListener = (final Record record) -> after.incrementAndGet();
    database.getSchema().getType(EDGE).getEvents().registerListener(afterListener);
    try {
      database.transaction(() -> database.command("sql", restore + " SET since = 'today'"));
      assertThat(after.get()).isEqualTo(1);
    } finally {
      database.getSchema().getType(EDGE).getEvents().unregisterListener(afterListener);
    }

    database.transaction(() -> {
      final var restored = database.lookupByRID(edge[0], true).asEdge();
      assertThat(restored.getString("since")).isEqualTo("today");
      // The declared default reached the edge shell as well.
      assertThat(restored.getString("weight")).isEqualTo("1");
      assertThat(restored.getOut()).isEqualTo(endpoints[0]);
      assertThat(restored.getIn()).isEqualTo(endpoints[1]);
    });
  }

  /**
   * Baseline: a plain INSERT of the very same shape is refused, so the schema really is enforced on the create path
   * and the RESTORE assertions below are comparing against a live rule.
   */
  @Test
  void insertWithoutTheMandatoryPropertyIsRefused() {
    database.transaction(() -> assertThatThrownBy(
        () -> database.command("sql", "INSERT INTO " + CONSTRAINED + " SET note = 'x'")).isInstanceOf(ValidationException.class));
  }

  @Test
  void restoreRefusesToRecreateARecordThatViolatesItsType() {
    final RID rid = deletedRecord(CONSTRAINED);

    database.transaction(() -> assertThatThrownBy(
        () -> database.command("sql", "RESTORE DOCUMENT " + CONSTRAINED + " RID " + rid + " SET note = 'x'")).isInstanceOf(
        ValidationException.class).hasMessageContaining("name").hasMessageContaining("mandatory"));

    // Nothing was written: the slot is still free, so a compliant retry can take it.
    database.transaction(
        () -> assertThatThrownBy(() -> database.lookupByRID(rid, true)).isInstanceOf(RecordNotFoundException.class));
    database.transaction(
        () -> database.command("sql", "RESTORE DOCUMENT " + CONSTRAINED + " RID " + rid + " SET name = 'supplied'"));
    database.transaction(() -> assertThat(database.lookupByRID(rid, true).asDocument().getString("name")).isEqualTo("supplied"));
  }

  /** RESTORE VERTEX builds the shell itself, so the same rule has to reach the vertex arm of the statement family. */
  @Test
  void restoreVertexRefusesAShellThatViolatesItsType() {
    final RID rid = deletedVertex();

    database.transaction(() -> assertThatThrownBy(() -> database.command("sql", "RESTORE VERTEX " + VERTEX + " RID " + rid))//
        .isInstanceOf(ValidationException.class).hasMessageContaining("label"));

    database.transaction(() -> database.command("sql", "RESTORE VERTEX " + VERTEX + " RID " + rid + " SET label = 'recovered'"));
    database.transaction(() -> assertThat(database.lookupByRID(rid, true).asVertex().getString("label")).isEqualTo("recovered"));
  }

  /**
   * Default values are not a constraint - they can never block a repair - and leaving them out produced a record the
   * schema says cannot exist even when nothing was mandatory.
   */
  @Test
  void restoreAppliesTheDeclaredDefaultValues() {
    final RID rid = deletedRecord(CONSTRAINED);

    database.transaction(() -> database.command("sql", "RESTORE DOCUMENT " + CONSTRAINED + " RID " + rid + " SET name = 'back'"));

    database.transaction(() -> assertThat(database.lookupByRID(rid, true).asDocument().getString("status")).isEqualTo("active"));
  }

  @Test
  void restoreFiresTheCreateEvents() {
    final RID rid = deletedRecord(PLAIN);
    final AtomicInteger before = new AtomicInteger();
    final AtomicInteger after = new AtomicInteger();

    final BeforeRecordCreateListener beforeListener = record -> {
      before.incrementAndGet();
      return true;
    };
    final AfterRecordCreateListener afterListener = (final Record record) -> after.incrementAndGet();

    database.getSchema().getType(PLAIN).getEvents().registerListener(beforeListener).registerListener(afterListener);
    try {
      database.transaction(() -> database.command("sql", "RESTORE DOCUMENT " + PLAIN + " RID " + rid + " SET v = 'r'"));

      assertThat(before.get()).as("a restored record must be visible to the triggers that maintain derived state").isEqualTo(1);
      assertThat(after.get()).isEqualTo(1);
    } finally {
      database.getSchema().getType(PLAIN).getEvents().unregisterListener(beforeListener).unregisterListener(afterListener);
    }
  }

  /**
   * The database-level registry is a separate call from the per-type one above and vetoes with its own message, so it
   * gets its own case rather than riding on the type-level test (PR #6130 review).
   */
  @Test
  void restoreFiresTheDatabaseLevelCreateEvents() {
    final RID rid = deletedRecord(PLAIN);
    final AtomicInteger before = new AtomicInteger();
    final AtomicInteger after = new AtomicInteger();

    final BeforeRecordCreateListener beforeListener = record -> {
      before.incrementAndGet();
      return true;
    };
    final AfterRecordCreateListener afterListener = (final Record record) -> after.incrementAndGet();

    database.getEvents().registerListener(beforeListener).registerListener(afterListener);
    try {
      database.transaction(() -> database.command("sql", "RESTORE DOCUMENT " + PLAIN + " RID " + rid + " SET v = 'r'"));

      assertThat(before.get()).isEqualTo(1);
      assertThat(after.get()).isEqualTo(1);
    } finally {
      database.getEvents().unregisterListener(beforeListener).unregisterListener(afterListener);
    }
  }

  @Test
  void aDatabaseLevelVetoAlsoRaises() {
    final RID rid = deletedRecord(PLAIN);
    final BeforeRecordCreateListener veto = record -> false;

    database.getEvents().registerListener(veto);
    try {
      database.transaction(() -> assertThatThrownBy(
          () -> database.command("sql", "RESTORE DOCUMENT " + PLAIN + " RID " + rid + " SET v = 'r'")).isInstanceOf(
          DatabaseOperationException.class).hasMessageContaining("database-level beforeCreate listener vetoed"));
    } finally {
      database.getEvents().unregisterListener(veto);
    }

    database.transaction(
        () -> assertThatThrownBy(() -> database.lookupByRID(rid, true)).isInstanceOf(RecordNotFoundException.class));
  }

  /**
   * The one intentional divergence from {@code createRecordNoLock}, which returns quietly on a veto: a vetoed RESTORE
   * must raise, because reporting success while writing nothing is exactly the silent outcome #6127 is about.
   */
  @Test
  void aVetoedRestoreRaisesInsteadOfSilentlyDoingNothing() {
    final RID rid = deletedRecord(PLAIN);
    final BeforeRecordCreateListener veto = record -> false;

    database.getSchema().getType(PLAIN).getEvents().registerListener(veto);
    try {
      database.transaction(() -> assertThatThrownBy(
          () -> database.command("sql", "RESTORE DOCUMENT " + PLAIN + " RID " + rid + " SET v = 'r'")).isInstanceOf(
          DatabaseOperationException.class).hasMessageContaining("vetoed"));
    } finally {
      database.getSchema().getType(PLAIN).getEvents().unregisterListener(veto);
    }

    database.transaction(
        () -> assertThatThrownBy(() -> database.lookupByRID(rid, true)).isInstanceOf(RecordNotFoundException.class));
  }

  /**
   * PR #6130 review: validation now runs before anything is written, so it could have started masking the error that
   * used to be the only one RESTORE returned. Aiming at a RID that is still live is the likeliest mistake with this
   * statement, so when a restore is BOTH invalid and pointed at an occupied slot, the occupied slot is what the
   * caller must be told about - fixing the record would not have helped.
   */
  @Test
  void anOccupiedSlotIsReportedAheadOfTheRecordsOwnConstraints() {
    final RID[] live = new RID[1];
    database.transaction(() -> live[0] = database.newDocument(CONSTRAINED).set("name", "still here").save().getIdentity());

    database.transaction(() -> assertThatThrownBy(
        () -> database.command("sql", "RESTORE DOCUMENT " + CONSTRAINED + " RID " + live[0] + " SET note = 'x'"))//
        .isInstanceOf(DatabaseOperationException.class).hasMessageContaining("occupied by a live record"));

    // And the live record is untouched.
    database.transaction(() -> assertThat(database.lookupByRID(live[0], true).asDocument().getString("name")).isEqualTo("still here"));
  }

  /**
   * The Java-API arm: {@code GraphEngine.restoreVertexAt} restores a property-less shell by design, so it is the one
   * caller the mandatory-property rule can turn away - and it must, for the same reason the SQL arm does.
   */
  @Test
  void restoreVertexAtAppliesTheSameSchemaContract() {
    final DatabaseInternal db = (DatabaseInternal) database;
    final RID rid = deletedVertex();

    database.transaction(() -> assertThatThrownBy(() -> db.getGraphEngine().restoreVertexAt(rid, VERTEX))//
        .isInstanceOf(ValidationException.class).hasMessageContaining("label"));
  }

  /**
   * The issue asked to confirm what {@code CHECK DATABASE} could see before choosing where the refusal belongs, and
   * the answer at the time was "nothing": it was a purely STRUCTURAL check (page layout, record markers, graph
   * adjacency, index entries) and evaluated no schema constraint, so it would never have caught the record a
   * permissive {@code RESTORE} wrote. That is why the refusal happens in the statement itself, and it still does.
   * <p>
   * It is no longer the whole story, which is what this test now pins: since #7952 the check also asks every record
   * it scans whether it satisfies its own type's EXISTENCE constraints, so a record in this state is reported -
   * under {@code constraintViolatingRecords}, never as corruption - and can be removed on request. The two are
   * complementary rather than redundant: refusing the write keeps {@code RESTORE} from creating the state, and the
   * scan finds one that got in some other way (an openCypher statement that died mid-write, #7952; a database
   * restored by a version predating the refusal; a property made mandatory after the fact, as below).
   */
  @Test
  void checkDatabaseReportsARecordThatViolatesALaterConstraint() {
    // The record is written while the property is still optional and the constraint is added afterwards: that
    // reproduces the record a permissive RESTORE used to leave behind, without going through a validating write
    // path (there is no longer one that would let it in).
    final RID[] rid = new RID[1];
    database.transaction(() -> {
      final DocumentType type = database.getSchema().createDocumentType("LateConstraint");
      type.createProperty("name", Type.STRING);
      rid[0] = database.newDocument("LateConstraint").set("other", "x").save().getIdentity();
      type.getProperty("name").setMandatory(true);
    });

    try (final ResultSet rs = database.command("sql", "CHECK DATABASE")) {
      final Result row = rs.next();
      assertThat((Long) row.getProperty("totalConstraintViolations")).isEqualTo(1L);
      assertThat((Collection<RID>) row.getProperty("constraintViolatingRecords")).containsExactly(rid[0]);
      // Not corruption: the record reads, deserialises and indexes correctly. Only the schema disagrees with it.
      assertThat((Collection<?>) row.getProperty("corruptedRecords")).isEmpty();
      final Object errors = row.getProperty("totalErrors");
      assertThat(errors == null ? 0L : ((Number) errors).longValue()).as("check database: " + row.toJSON()).isZero();
    }

    // Removed here so the shared end-of-test integrity assertion sees a clean database - and, in passing, so this
    // test pins that the opt-in repair reaches this route to the state as well as the crashed-statement one.
    try (final ResultSet rs = database.command("sql", "CHECK DATABASE FIX DELETE INVALID RECORDS")) {
      assertThat((Collection<RID>) rs.next().getProperty("deletedConstraintViolatingRecords")).containsExactly(rid[0]);
    }
  }

  /** Deletes a freshly created document and returns its now-free RID. */
  private RID deletedRecord(final String typeName) {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = CONSTRAINED.equals(typeName) ?
        database.newDocument(typeName).set("name", "original").save().getIdentity() :
        database.newDocument(typeName).set("v", "original").save().getIdentity());

    final LocalBucket bucket = (LocalBucket) ((DatabaseInternal) database).getSchema().getBucketById(rid[0].getBucketId());
    database.transaction(() -> bucket.deleteRecord(rid[0]));
    return rid[0];
  }

  /** Deletes a freshly created vertex and returns its now-free RID. */
  private RID deletedVertex() {
    final RID[] rid = new RID[1];
    database.transaction(() -> rid[0] = database.newVertex(VERTEX).set("label", "original").save().getIdentity());

    final LocalBucket bucket = (LocalBucket) ((DatabaseInternal) database).getSchema().getBucketById(rid[0].getBucketId());
    database.transaction(() -> bucket.deleteRecord(rid[0]));
    return rid[0];
  }
}
