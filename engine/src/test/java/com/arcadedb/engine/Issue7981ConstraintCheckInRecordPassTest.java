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
import com.arcadedb.database.RID;
import com.arcadedb.graph.MutableEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Type;
import com.arcadedb.utility.ProgressCallback;
import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Regression test for issue #7981: the existence-constraint pass of {@code CHECK DATABASE} (#7952) read and deserialised
 * every record of every constrained type a second time in the same run. A report-only run now asks the question during the
 * record-content read every full run makes anyway, so it has no constraint step of its own; only {@code FIX DELETE INVALID
 * RECORDS}, whose bounded delete passes need a scan they own, still walks those types again. The findings are the same,
 * for documents, vertices and edges alike, and are still kept apart from corruption.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue7981ConstraintCheckInRecordPassTest extends TestHelper {
  private static final String CONSTRAINT_STEP = "Checking existence constraints";

  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  private DatabaseInternal db() {
    return (DatabaseInternal) database;
  }

  /** Steps the run announced, and the number of distinct step indexes against the total it declared. */
  private static final class StepRecorder implements ProgressCallback {
    final Set<String>   names           = new HashSet<>();
    final Set<Integer>  indexes         = new HashSet<>();
    final AtomicInteger totalSteps      = new AtomicInteger(-1);
    final AtomicInteger constraintTicks = new AtomicInteger();

    @Override
    public void onProgress(final String stepName, final int stepIndex, final int total, final long done, final long stepTotal) {
      names.add(stepName);
      indexes.add(stepIndex);
      totalSteps.set(total);
      if (CONSTRAINT_STEP.equals(stepName) && done > 0)
        constraintTicks.incrementAndGet();
    }
  }

  @Test
  void aReportOnlyRunFindsEveryViolationWithoutASecondPass() {
    final RID[] rids = createViolatingDocumentVertexAndEdge();

    final StepRecorder steps = new StepRecorder();
    final Map<String, Object> result = new DatabaseChecker(db()).setVerboseLevel(0).setProgressCallback(steps).check();

    assertThat((Collection<RID>) result.get("constraintViolatingRecords")).containsExactlyInAnyOrder(rids);
    assertThat((Long) result.get("totalConstraintViolations")).isEqualTo(3L);
    assertThat((Collection<RID>) result.get("corruptedRecords")).as("a violation is not corruption").isEmpty();
    assertThat((Collection<String>) result.get("warnings"))
        .anyMatch(w -> w.contains(rids[0].toString()) && w.contains("Doc.orgId"))
        .anyMatch(w -> w.contains(rids[1].toString()) && w.contains("Person.orgId"))
        .anyMatch(w -> w.contains(rids[2].toString()) && w.contains("Knows.since"))
        .anyMatch(w -> w.contains("CHECK DATABASE FIX DELETE INVALID RECORDS"));

    assertThat(steps.names).as("no second walk of the constrained types").doesNotContain(CONSTRAINT_STEP);
    assertThat(steps.names).anyMatch(n -> n.startsWith("Decoding record content"));
    assertThat(steps.indexes).as("the step plan announced is the one that ran").hasSize(steps.totalSteps.get());

    // NOTHING WAS REMOVED: A REPORT-ONLY RUN
    for (final RID rid : rids)
      assertThat(database.existsRecord(rid)).as("a report-only run keeps %s", rid).isTrue();
  }

  @Test
  void aPlainFixStillOnlyReports() {
    final RID[] rids = createViolatingDocumentVertexAndEdge();

    final StepRecorder steps = new StepRecorder();
    final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setVerboseLevel(0).setProgressCallback(steps)
        .check();

    assertThat((Collection<RID>) result.get("constraintViolatingRecords")).containsExactlyInAnyOrder(rids);
    assertThat((Long) result.get("totalDeletedConstraintViolatingRecords")).isZero();
    assertThat(steps.names).doesNotContain(CONSTRAINT_STEP);
    assertThat(steps.indexes).hasSize(steps.totalSteps.get());
    for (final RID rid : rids)
      assertThat(database.existsRecord(rid)).as("a plain FIX keeps %s", rid).isTrue();
  }

  @Test
  void theRepairStillHasItsOwnPassAndReportsEachRecordOnce() {
    final RID[] rids = createViolatingDocumentVertexAndEdge();

    final StepRecorder steps = new StepRecorder();
    final Map<String, Object> result = new DatabaseChecker(db()).setFix(true).setDeleteInvalidRecords(true).setVerboseLevel(0)
        .setProgressCallback(steps).check();

    assertThat((Long) result.get("totalConstraintViolations")).as("reported once, by the repair pass").isEqualTo(3L);
    assertThat((Long) result.get("totalDeletedConstraintViolatingRecords")).isEqualTo(3L);
    assertThat((Collection<RID>) result.get("deletedConstraintViolatingRecords")).containsExactlyInAnyOrder(rids);
    assertThat(steps.names).contains(CONSTRAINT_STEP);
    assertThat(steps.constraintTicks.get()).isPositive();
    assertThat(steps.indexes).hasSize(steps.totalSteps.get());
    for (final RID rid : rids)
      assertThat(database.existsRecord(rid)).as("the repair removed %s", rid).isFalse();
    assertThat(database.query("sql", "SELECT count() AS c FROM Doc").next().<Long>getProperty("c")).as("the complete document stays")
        .isEqualTo(1L);
    assertThat((Collection<String>) result.get("warnings")).noneMatch(w -> w.contains("CHECK DATABASE FIX DELETE INVALID RECORDS"));
  }

  @Test
  void aTypeScopedRunAsksOnlyTheTypesInScope() {
    final RID[] rids = createViolatingDocumentVertexAndEdge();

    final Map<String, Object> result = new DatabaseChecker(db()).setTypes(Set.of("Person")).setVerboseLevel(0).check();

    assertThat((Collection<RID>) result.get("constraintViolatingRecords")).containsExactly(rids[1]);
  }

  @Test
  void aBucketScopedRunStillAsksTheQuestion() {
    final RID[] rids = createViolatingDocumentVertexAndEdge();

    // BUCKET narrows the bucket-level pass, not the record passes, so a report-only run under it still finds every record
    final Map<String, Object> result = new DatabaseChecker(db())
        .setBuckets(Set.<Object>of(database.getSchema().getType("Doc").getBuckets(false).getFirst().getName())).setVerboseLevel(0)
        .check();

    assertThat((Collection<RID>) result.get("constraintViolatingRecords")).containsExactlyInAnyOrder(rids);
  }

  @Test
  void aDatabaseWithoutExistenceConstraintsReportsNothing() {
    database.getSchema().createDocumentType("Free").createProperty("id", Type.INTEGER);
    database.transaction(() -> database.newDocument("Free").save());

    final Map<String, Object> result = new DatabaseChecker(db()).setVerboseLevel(0).check();

    assertThat((Long) result.get("totalConstraintViolations")).isZero();
    assertThat((Collection<String>) result.get("warnings")).noneMatch(w -> w.contains("existence constraint"));
  }

  /**
   * One violating record per record family - a document, a vertex and an edge - next to a complete document and vertex,
   * each made invalid the schema-change way: the constraint arrives after the record does.
   */
  private RID[] createViolatingDocumentVertexAndEdge() {
    final DocumentType doc = database.getSchema().createDocumentType("Doc");
    doc.createProperty("id", Type.INTEGER);
    doc.createProperty("orgId", Type.STRING);
    database.getSchema().createVertexType("Person").createProperty("orgId", Type.STRING);
    database.getSchema().createEdgeType("Knows").createProperty("since", Type.INTEGER);

    final RID[] rids = new RID[3];
    database.transaction(() -> {
      rids[0] = database.newDocument("Doc").set("id", 1).save().getIdentity();
      database.newDocument("Doc").set("id", 2).set("orgId", "o").save();
      final MutableVertex incomplete = database.newVertex("Person").save();
      final MutableVertex complete = database.newVertex("Person").set("orgId", "o").save();
      rids[1] = incomplete.getIdentity();
      final MutableEdge edge = complete.newEdge("Knows", incomplete);
      rids[2] = edge.getIdentity();
    });

    doc.getProperty("orgId").setMandatory(true);
    database.getSchema().getType("Person").getProperty("orgId").setMandatory(true);
    database.getSchema().getType("Knows").getProperty("since").setMandatory(true);
    return rids;
  }
}
