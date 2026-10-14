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
package com.arcadedb.database;

import com.arcadedb.TestHelper;
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.event.BeforeRecordUpdateListener;
import com.arcadedb.graph.MutableLightEdge;
import com.arcadedb.graph.MutableVertex;
import com.arcadedb.query.sql.executor.DeleteStep;
import com.arcadedb.query.sql.executor.ExecutionStep;
import com.arcadedb.query.sql.executor.LimitExecutionStep;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.executor.SaveElementStep;
import com.arcadedb.schema.EdgeType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #9974: a write a before-event listener vetoed must not be counted as applied. The engine decides the answer, and the void
 * {@code Database.createRecord()} / {@code updateRecord()} and {@code MutableDocument.save()} threw it away, so every caller that
 * counts the records it wrote counted the refused ones too. {@link DatabaseInternal#createRecordReportingVeto},
 * {@link DatabaseInternal#updateRecordReportingVeto} and {@link MutableDocument#saveReportingVeto()} keep it, the SQL
 * {@code DELETE}/{@code UPDATE}/{@code INSERT} steps pass on only the rows the engine wrote, and the asynchronous create does not
 * report a success for a vetoed record.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue9974WriteVetoReportingTest extends TestHelper {

  // --- the accessors ---

  @Test
  void createReportsTheVeto() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("name")));

    final boolean[] answers = new boolean[2];
    final MutableDocument refused = database.newDocument("T").set("name", "refused");
    database.transaction(() -> {
      answers[0] = ((DatabaseInternal) database).createRecordReportingVeto(database.newDocument("T").set("name", "kept"), null);
      answers[1] = refused.saveReportingVeto();
    });

    assertThat(answers[0]).isTrue();
    assertThat(answers[1]).isFalse();
    assertThat(refused.getIdentity()).isNull();
    assertThat(count("T")).isEqualTo(1L);
  }

  @Test
  void updateReportsTheVeto() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordUpdateListener) record -> !"refused".equals(record.asDocument().getString("name")));
    final RID rid = createDocuments("T", "a")[0];

    final boolean[] answers = new boolean[2];
    database.transaction(() -> answers[0] = rid.asDocument().modify().set("name", "b").saveReportingVeto());
    database.transaction(
        () -> answers[1] = ((DatabaseInternal) database).updateRecordReportingVeto(rid.asDocument().modify().set("name", "refused")));

    assertThat(answers[0]).isTrue();
    assertThat(answers[1]).isFalse();
    assertThat(rid.asDocument().getString("name")).isEqualTo("b");
  }

  @Test
  void anUpdateOfARecordThisTransactionDeletedIsNotApplied() {
    database.getSchema().createDocumentType("T");
    final RID rid = createDocuments("T", "a")[0];

    final boolean[] applied = { true };
    database.transaction(() -> {
      final MutableDocument doc = rid.asDocument().modify();
      database.deleteRecord(doc);
      applied[0] = doc.set("name", "b").saveReportingVeto();
    });

    assertThat(applied[0]).isFalse();
    assertThat(count("T")).isZero();
  }

  /** A lightweight edge has no record to write, so there is nothing a listener could refuse: its save is always applied. */
  @Test
  void aLightweightEdgeSaveIsAlwaysApplied() {
    database.getSchema().createVertexType("V");
    final EdgeType light = database.getSchema().createEdgeType("Light");
    light.getEvents().registerListener((BeforeRecordUpdateListener) record -> false);

    final boolean[] applied = new boolean[1];
    database.transaction(() -> {
      final MutableVertex v1 = database.newVertex("V").save();
      final MutableVertex v2 = database.newVertex("V").save();
      applied[0] = new MutableLightEdge(database, light, v1.getIdentity(), v2.getIdentity()).saveReportingVeto();
    });

    assertThat(applied[0]).isTrue();
  }

  @Test
  void anAsynchronousCreateVetoedDoesNotReportASuccess() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("name")));

    final AtomicInteger created = new AtomicInteger();
    database.async().createRecord(database.newDocument("T").set("name", "kept"), record -> created.incrementAndGet());
    database.async().createRecord(database.newDocument("T").set("name", "refused"), record -> created.incrementAndGet());
    database.async().waitCompletion();

    assertThat(created.get()).isEqualTo(1);
    assertThat(count("T")).isEqualTo(1L);
  }

  // --- SQL ---

  @Test
  void sqlDeleteCountsOnlyTheRecordsItDeleted() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordDeleteListener) record -> !"keep".equals(record.asDocument().getString("name")));
    createDocuments("T", "a", "keep", "b");

    assertThat(commandCount("DELETE FROM T")).isEqualTo(2L);
    assertThat(count("T")).isEqualTo(1L);
  }

  @Test
  void sqlDeleteReturnBeforeReturnsOnlyTheRecordsItDeleted() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordDeleteListener) record -> !"keep".equals(record.asDocument().getString("name")));
    createDocuments("T", "a", "keep", "b");

    final List<String> returned = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM T RETURN BEFORE")) {
        while (rs.hasNext())
          returned.add(rs.next().getProperty("name"));
      }
    });

    assertThat(returned).containsExactlyInAnyOrder("a", "b");
  }

  /**
   * A whole fetch batch of vetoed rows must not end the statement: the steps after the delete stop at the first empty pull, so a
   * step that answered one would leave every later record undeleted.
   */
  @Test
  void sqlDeleteGoesOnPastAWholeBatchOfVetoedRows() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordDeleteListener) record -> record.asDocument().getInteger("i") >= 250);
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newDocument("T").set("i", i).save();
    });

    assertThat(commandCount("DELETE FROM T")).isEqualTo(250L);
    assertThat(count("T")).isEqualTo(250L);
  }

  /**
   * The delete step acts one row ahead of what it returns: a LIMIT must still bound the records deleted, and every record deleted
   * must be one the statement returns.
   */
  @Test
  void sqlDeleteWithALimitDeletesExactlyTheRowsItReturns() {
    database.getSchema().createDocumentType("T");
    createDocuments("T", "a", "b", "c", "d");

    final List<String> returned = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "DELETE FROM T RETURN BEFORE LIMIT 2")) {
        while (rs.hasNext())
          returned.add(rs.next().getProperty("name"));
      }
    });

    assertThat(returned).hasSize(2);
    assertThat(count("T")).isEqualTo(2L);
    assertThat(commandCount("DELETE FROM T LIMIT 1")).isEqualTo(1L);
    assertThat(count("T")).isEqualTo(1L);
  }

  /**
   * The write steps act one row ahead of what they return ({@code SkippingResultSet}), which is only safe because no step after
   * them can stop pulling early. Pinned on the plans themselves: in every DML plan a LIMIT comes before the write step.
   */
  @Test
  void noDmlPlanLimitsAfterItsWriteStep() {
    database.getSchema().createDocumentType("T");
    createDocuments("T", "a", "b", "c");

    for (final String sql : new String[] { "DELETE FROM T LIMIT 1", "DELETE FROM T RETURN BEFORE LIMIT 1",
        "UPDATE T SET x = 1 LIMIT 1", "UPDATE T SET x = 2 RETURN AFTER LIMIT 1", "UPDATE T SET x = 3 RETURN BEFORE LIMIT 1",
        "INSERT INTO T SET name = 'n'", "INSERT INTO T FROM SELECT name FROM T LIMIT 1" })
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", sql)) {
          final List<ExecutionStep> steps = rs.getExecutionPlan().orElseThrow().getSteps();
          int write = -1;
          for (int i = 0; i < steps.size(); i++)
            if (steps.get(i) instanceof DeleteStep || steps.get(i) instanceof SaveElementStep)
              write = i;
          assertThat(write).as("write step of: %s", sql).isGreaterThanOrEqualTo(0);
          for (int i = write + 1; i < steps.size(); i++)
            assertThat(steps.get(i)).as("step after the write of: %s", sql).isNotInstanceOf(LimitExecutionStep.class);
        }
      });
  }

  @Test
  void sqlDeleteOfOnlyVetoedRowsCountsZero() {
    database.getSchema().createDocumentType("T").getEvents().registerListener((BeforeRecordDeleteListener) record -> false);
    createDocuments("T", "a", "b", "c");

    assertThat(commandCount("DELETE FROM T")).isZero();
    assertThat(count("T")).isEqualTo(3L);
  }

  @Test
  void sqlUpdateCountsOnlyTheRecordsItChanged() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordUpdateListener) record -> !"keep".equals(record.asDocument().getString("name")));
    createDocuments("T", "a", "keep", "b");

    assertThat(commandCount("UPDATE T SET touched = true")).isEqualTo(2L);
    assertThat(commandCount("SELECT count() AS count FROM T WHERE touched = true")).isEqualTo(2L);
  }

  @Test
  void sqlUpdateGoesOnPastAWholeBatchOfVetoedRows() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordUpdateListener) record -> record.asDocument().getInteger("i") >= 250);
    database.transaction(() -> {
      for (int i = 0; i < 500; i++)
        database.newDocument("T").set("i", i).save();
    });

    assertThat(commandCount("UPDATE T SET touched = true")).isEqualTo(250L);
    assertThat(commandCount("SELECT count() AS count FROM T WHERE touched = true")).isEqualTo(250L);
  }

  @Test
  void sqlInsertDoesNotReturnARecordItDidNotCreate() {
    database.getSchema().createDocumentType("T").getEvents()
        .registerListener((BeforeRecordCreateListener) record -> !"refused".equals(record.asDocument().getString("name")));

    final List<Result> returned = new ArrayList<>();
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "INSERT INTO T CONTENT [{\"name\":\"a\"},{\"name\":\"refused\"},{\"name\":\"b\"}]")) {
        while (rs.hasNext())
          returned.add(rs.next());
      }
    });

    assertThat(returned).extracting(r -> r.<String>getProperty("name")).containsExactlyInAnyOrder("a", "b");
    assertThat(returned).allMatch(r -> r.getIdentity().isPresent());
    assertThat(count("T")).isEqualTo(2L);
  }

  // --- helpers ---

  private RID[] createDocuments(final String type, final String... names) {
    final RID[] rids = new RID[names.length];
    database.transaction(() -> {
      for (int i = 0; i < names.length; i++)
        rids[i] = database.newDocument(type).set("name", names[i]).save().getIdentity();
    });
    return rids;
  }

  private long commandCount(final String sql) {
    final long[] count = new long[1];
    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", sql)) {
        count[0] = ((Number) rs.next().getProperty("count")).longValue();
      }
    });
    return count[0];
  }

  private long count(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }
}
