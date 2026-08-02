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
package com.arcadedb.graph;

import com.arcadedb.TestHelper;
import com.arcadedb.database.Binary;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.Document;
import com.arcadedb.database.RID;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.engine.MutablePage;
import com.arcadedb.engine.PageId;
import com.arcadedb.engine.PaginatedComponentFile;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BiConsumer;
import java.util.logging.Level;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * #5773: the type-wide arms of {@code CHECK DATABASE} materialise each vertex/edge record ONCE, not twice.
 * <p>
 * {@code GraphDatabaseChecker.checkVertices}/{@code checkEdges} used to open with a raw bucket scan that built
 * every record and decoded it ({@code newImmutableRecord(...)} then {@code asVertex(true)}), and then run the
 * connectivity/endpoint walk through {@code scanType} - which performs the identical construction from the
 * identical raw page view and opens with the identical decode. The first pass therefore detected nothing the
 * second misses, and the progress budget said so out loud ({@code 2 * countType}).
 * <p>
 * These tests pin both halves of dropping it: the budget is now one pass per record, and every corruption shape
 * the removed pass could see is still reported by the surviving one - once, with the wording that does not
 * promise a removal a non-fixing run never performs.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class CheckDatabaseSinglePassTest extends TestHelper {

  private record Emission(String stepName, long done, long total) {
  }

  /** These tests deliberately corrupt records, so the blanket end-of-test check would always fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  /**
   * The measurable half: the per-type step budgets ONE unit of work per record. The budget is what the two passes
   * were literally written as ({@code 2 * countType}), so a regression that reinstates a second pass either has to
   * under-report progress or fails here.
   */
  @Test
  void theTypeWideGraphStepsBudgetOnePassPerRecord() {
    createGraph(50);

    final List<Emission> emissions = new ArrayList<>();
    new DatabaseChecker(database).setVerboseLevel(0)
        .setProgressCallback((stepName, stepIndex, totalSteps, done, total) ->
            emissions.add(new Emission(stepName, done, total)))
        .check();

    assertBudgetIsOnePassPerRecord(emissions, "Checking vertices 'Person'", database.countType("Person", false));
    assertBudgetIsOnePassPerRecord(emissions, "Checking edges 'WorksAt'", database.countType("WorksAt", false));
  }

  /**
   * The correctness half for a record that cannot be MATERIALISED at all (an unknown record-type byte): the
   * removed pass caught it in the bucket scan, and {@code scanType}'s error callback catches it at the same point,
   * because the construction it fails in sits inside the same {@code LocalBucket.scan} try block.
   * <p>
   * Also pins the wording (#5773 item 2): the surviving message does not say "removing it", which a check without
   * {@code FIX} never does.
   */
  @Test
  void anUnmaterialisableVertexIsReportedOnceByTheSurvivingPass() {
    createGraph(3);

    final RID victim = firstOf("Person");
    corruptRecordTypeByte(victim);

    final Result row = check("CHECK DATABASE TYPE Person");

    final Collection<String> warnings = row.getProperty("warnings");
    assertThat(warnings).as("the surviving pass must still report it: %s", row.toJSON())
        .anyMatch(w -> w.startsWith("vertex " + victim + " cannot be loaded (error:"));
    assertThat(warnings).as("a non-fixing check removes nothing: %s", row.toJSON())
        .noneMatch(w -> w.contains("removing it"));
    assertThat(longProperty(row, "totalWarnings")).as("reported ONCE, not once per pass: %s", row.toJSON())
        .isEqualTo(1L);
    assertThat(longProperty(row, "totalCorruptedRecords")).as("%s", row.toJSON()).isEqualTo(1L);
  }

  /**
   * The correctness half for a record that materialises but cannot be DECODED as a vertex (a truncated buffer):
   * the removed pass caught it in its own {@code asVertex(true)}, and the connectivity walk opens with the same
   * call, so the finding survives - with the same "(error: ...)" wording, which is the one the removed pass did
   * not use.
   */
  @Test
  void anUndecodableVertexIsReportedOnceByTheSurvivingPass() {
    createGraph(3);

    final RID victim = firstOf("Person");
    shrinkRecordBuffer(victim);

    final Result row = check("CHECK DATABASE TYPE Person");

    final Collection<String> warnings = row.getProperty("warnings");
    assertThat(warnings).as("%s", row.toJSON())
        .anyMatch(w -> w.startsWith("vertex " + victim + " cannot be loaded (error:"));
    assertThat(warnings).as("%s", row.toJSON()).noneMatch(w -> w.contains("removing it"));
    assertThat(longProperty(row, "totalWarnings")).as("%s", row.toJSON()).isEqualTo(1L);
    assertThat(longProperty(row, "totalCorruptedRecords")).as("%s", row.toJSON()).isEqualTo(1L);
  }

  /**
   * The dropped pass was labelled "CHECK RECORD IS OF THE RIGHT TYPE", so the shape it named explicitly gets its own
   * proof: a record sitting in a VERTEX bucket that materialises as something else entirely. Two variants, because
   * they fail in different places - a {@code Document} record-type byte builds an {@code ImmutableDocument} that the
   * connectivity walk's {@code asVertex(true)} rejects, and an {@code EdgeSegment} one builds a record that is not a
   * {@code Document} at all, so it fails in {@code scanType}'s own cast and lands in the error callback instead.
   */
  @Test
  void aRecordOfTheWrongTypeInAVertexBucketIsStillFlagged() {
    for (final byte recordType : new byte[] { Document.RECORD_TYPE, EdgeSegment.RECORD_TYPE }) {
      createGraph(3);
      try {
        final RID victim = firstOf("Person");
        setRecordTypeByte(victim, recordType);

        final Result row = check("CHECK DATABASE TYPE Person");
        assertThat((Collection<String>) row.getProperty("warnings"))
            .as("record type %d: %s", recordType, row.toJSON())
            .anyMatch(w -> w.startsWith("vertex " + victim + " cannot be loaded (error:"));
        assertThat(longProperty(row, "totalCorruptedRecords")).as("record type %d: %s", recordType, row.toJSON())
            .isEqualTo(1L);
      } finally {
        // Each iteration needs a pristine graph, and the corrupted record makes a drop the only clean way back.
        database.getSchema().dropType("WorksAt");
        database.getSchema().dropType("Person");
        database.getSchema().dropType("Company");
      }
    }
  }

  /** The edge arm of the same equivalence: identical code, so identical proof. */
  @Test
  void anUnmaterialisableEdgeIsReportedOnceByTheSurvivingPass() {
    createGraph(3);

    final RID victim = firstOf("WorksAt");
    corruptRecordTypeByte(victim);

    // TYPE-scoped to the edge type: a corrupt edge record is ALSO reachable from its endpoints' edge lists, so a
    // full-database run would report it a second time from the vertex arm and make this assertion say nothing.
    final Result row = check("CHECK DATABASE TYPE WorksAt");

    final Collection<String> warnings = row.getProperty("warnings");
    assertThat(warnings).as("%s", row.toJSON())
        .anyMatch(w -> w.startsWith("edge " + victim + " cannot be loaded (error:"));
    assertThat(warnings).as("%s", row.toJSON()).noneMatch(w -> w.contains("removing it"));
    assertThat(longProperty(row, "totalWarnings")).as("%s", row.toJSON()).isEqualTo(1L);
    assertThat(longProperty(row, "totalCorruptedRecords")).as("%s", row.toJSON()).isEqualTo(1L);
  }

  /**
   * A corrupted record is still DELETED by {@code FIX} once the pass that announced the removal is gone: the
   * removal was never that pass's doing (it only added the RID to {@code corruptedRecords}), and the surviving
   * pass adds the same RID.
   */
  @Test
  void fixStillDeletesTheCorruptedRecord() {
    createGraph(3);

    final RID victim = firstOf("Person");
    corruptRecordTypeByte(victim);

    final Result row = check("CHECK DATABASE TYPE Person FIX");
    assertThat(longProperty(row, "autoFix")).as("%s", row.toJSON()).isEqualTo(1L);

    database.transaction(() -> assertThat(database.existsRecord(victim)).as("FIX must still remove it").isFalse());
  }

  /**
   * #5773 item 4: past the retention cap, a RID that is STILL in the retained set must not be counted again.
   * {@code GraphDatabaseChecker.addCorrupted} incremented unconditionally there while its {@code DatabaseChecker}
   * twin already checked {@code contains} first, so the two disagreed on the same input.
   * <p>
   * Reached through the one caller that flags the same RID twice in one visit: an edge whose IN and OUT vertices
   * are both gone flags the edge on each side. With a cap of 1 the edge is the only retained RID, so the second
   * flag on it is exactly the case the {@code contains} check answers.
   */
  @Test
  void theCorruptedTotalDoesNotDoubleCountARetainedRecordPastTheCap() {
    createGraph(1);

    final RID edge = firstOf("WorksAt");
    final RID[] endpoints = new RID[2];
    database.transaction(() -> {
      final Edge e = edge.asEdge(true);
      endpoints[0] = e.getIn();
      endpoints[1] = e.getOut();
    });

    // Delete both endpoint VERTEX records underneath the edge, leaving the edge record itself intact.
    for (final RID endpoint : endpoints)
      database.transaction(() -> database.getSchema().getBucketById(endpoint.getBucketId()).deleteRecord(endpoint));

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkEdges("WorksAt", false, 0, 100, 1);

    // The retained slot holds the EDGE because checkEndpoints flags it before the missing endpoint it found: the
    // IN branch calls corrupt(edgeRID) first, then corrupt(edge.getIn()). Asserted rather than assumed, so a
    // future reorder of that branch fails here instead of quietly changing what this test is measuring.
    final Collection<RID> corrupted = (Collection<RID>) stats.get("corruptedRecords");
    assertThat(corrupted).as("precondition: the cap must bite, and the edge must be what it retained")
        .hasSize(1).containsExactly(edge);
    // Flagged: the edge (twice - once per missing endpoint) and the missing IN vertex. The edge's second flag is
    // the one that must not count, because the set still answers "already seen" for it.
    assertThat((Long) stats.get("totalCorruptedRecords"))
        .as("a retained RID flagged twice is one corrupted record: %s", stats).isEqualTo(2L);
  }

  /**
   * #5773 item 5: {@code totalWarnings} counts what the retained {@code warnings} collection keeps.
   * {@code GraphDatabaseChecker} retained its warnings in a list while {@code DatabaseChecker} published them as a
   * {@code Set}, so two findings rendering to the same message collapsed to one line but counted twice - the
   * totals exceeded the retained size on a run that was nowhere near its cap.
   * <p>
   * Reached through the scoped arm, whose {@code Collection<RID>} a caller can hand the same RID twice (the SQL
   * layer cannot - {@code DatabaseChecker.setRecords} takes a {@code Set}), so the identical message is produced
   * twice with no page surgery. The corrupted-record side of the same visit is de-duplicated too.
   */
  @Test
  void aFindingRenderedTwiceIsOneWarningOnBothSides() {
    createGraph(3);

    final RID victim = firstOf("Person");
    corruptRecordTypeByte(victim);

    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkVertices("Person", List.of(victim, victim), false, 0, 100, 100);

    final Collection<String> warnings = (Collection<String>) stats.get("warnings");
    assertThat(warnings).as("precondition: the corruption must be reported: %s", stats)
        .anyMatch(w -> w.startsWith("vertex " + victim + " cannot be loaded (error:"));
    assertThat(warnings).as("the same message is retained once: %s", stats).hasSize(1);
    assertThat((Long) stats.get("totalWarnings")).as("and counted once: %s", stats).isEqualTo(1L);
    assertThat((Long) stats.get("totalCorruptedRecords")).as("%s", stats).isEqualTo(1L);
  }

  /**
   * #5773: a warning the cap forced the checker to DROP is logged rather than lost - and both {@code addWarning}
   * arms honour {@code verboseLevel == 0} as "the caller asked for no logging", where the graph one used to log
   * regardless. The retained set and {@code totalWarnings} still report the drop either way, which is what makes
   * honouring the flag safe.
   */
  @Test
  void aDroppedWarningIsLoggedUnlessTheCallerAskedForSilence() {
    createGraph(3);

    final RID victim = firstOf("Person");
    corruptRecordTypeByte(victim);

    // maxWarnings 0: the finding is counted but never retained, which is the only path that reaches the log.
    assertThat(capturedWhileChecking(0)).as("verboseLevel 0 asked for no logging").isEmpty();
    assertThat(capturedWhileChecking(1)).as("otherwise the dropped message must still be audible")
        .anyMatch(m -> m.contains(victim.toString()));

    // Either way the caller can still see that something was dropped.
    final Map<String, Object> stats = new GraphDatabaseChecker((DatabaseInternal) database)
        .checkVertices("Person", null, false, 0, 0, 0);
    assertThat((Collection<String>) stats.get("warnings")).as("nothing retained at cap 0: %s", stats).isEmpty();
    assertThat((Long) stats.get("totalWarnings")).as("but counted: %s", stats).isEqualTo(1L);
  }

  /**
   * Runs a capped type-wide vertex check at {@code verboseLevel} and returns the WARNING messages it logged.
   * <p>
   * NOTE: {@code LogManager} is a SINGLETON, so this swap is process-wide for its duration. It is restored in the
   * finally and is safe only because surefire runs test classes sequentially within a fork - the same constraint
   * the four capture sites this PR touched carry.
   */
  private List<String> capturedWhileChecking(final int verboseLevel) {
    final List<String> captured = new CopyOnWriteArrayList<>();
    final Logger original = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(captured, original));
    try {
      new GraphDatabaseChecker((DatabaseInternal) database)
          .checkVertices("Person", null, false, verboseLevel, 0, 0);
    } finally {
      LogManager.instance().setLogger(original);
    }
    return captured;
  }

  private void assertBudgetIsOnePassPerRecord(final List<Emission> emissions, final String stepName,
      final long records) {
    assertThat(records).as("precondition: '%s' must have records to budget for", stepName).isGreaterThan(0L);

    final List<Emission> step = emissions.stream().filter(e -> e.stepName.equals(stepName)).toList();
    assertThat(step).as("emissions for step '%s'", stepName).isNotEmpty();
    assertThat(step.getFirst().total).as("step '%s' must budget ONE pass per record", stepName).isEqualTo(records);
    assertThat(step.getLast().done).as("step '%s' must reach its budget", stepName).isEqualTo(records);
  }

  private void createGraph(final int people) {
    database.transaction(() -> {
      database.getSchema().createVertexType("Person", 1);
      database.getSchema().createVertexType("Company", 1);
      database.getSchema().createEdgeType("WorksAt", 1);
    });

    database.transaction(() -> {
      final MutableVertex company = database.newVertex("Company").set("name", "Arcade").save();
      for (int i = 0; i < people; i++)
        database.newVertex("Person").set("i", i).save().newEdge("WorksAt", company);
    });
  }

  /** The RID of the first record of {@code typeName}, so the corruption helpers have a concrete victim. */
  private RID firstOf(final String typeName) {
    final RID[] holder = new RID[1];
    database.transaction(() -> database.scanType(typeName, false, record -> {
      holder[0] = record.getIdentity();
      return false;
    }));
    assertThat(holder[0]).as("no record of type '%s' to corrupt", typeName).isNotNull();
    return holder[0];
  }

  private Result check(final String command) {
    try (final ResultSet rs = database.command("sql", command)) {
      assertThat(rs.hasNext()).isTrue();
      return rs.next();
    }
  }

  /**
   * Overwrites the record-type byte with a value no {@code RecordFactory} branch knows, so the record still
   * occupies its slot but cannot be materialised at all - the shape the removed bucket scan met in
   * {@code newImmutableRecord}.
   */
  private void corruptRecordTypeByte(final RID rid) {
    setRecordTypeByte(rid, (byte) 99);
  }

  /** Rewrites the record-type byte of {@code rid} to {@code recordType}, leaving the rest of the record alone. */
  private void setRecordTypeByte(final RID rid, final byte recordType) {
    onRecordPage(rid, (page, recordOffset) -> {
      final long[] recordSize = page.readNumberAndSize(recordOffset);
      page.writeByte((int) (recordOffset + recordSize[1]), recordType);
    });
  }

  /**
   * Replaces the record-size varint with a single-byte varint encoding a size far below the fixed 25-byte vertex
   * prefix: the record materialises lazily but cannot be decoded - the shape the removed pass met in its own
   * {@code asVertex(true)}. zigzag(8) == 16.
   */
  private void shrinkRecordBuffer(final RID rid) {
    onRecordPage(rid, (page, recordOffset) -> page.writeByte(recordOffset, (byte) 16));
  }

  /** Runs {@code mutation} against the page holding {@code rid}, at the offset where its size varint starts. */
  private void onRecordPage(final RID rid, final BiConsumer<MutablePage, Integer> mutation) {
    final DatabaseInternal db = (DatabaseInternal) database;
    final int fileId = rid.getBucketId();
    final LocalBucket bucket = (LocalBucket) db.getSchema().getBucketById(fileId);
    final int pageSize = ((PaginatedComponentFile) db.getFileManager().getFile(fileId)).getPageSize();
    final int maxRecordsInPage = bucket.getMaxRecordsInPage();

    final int pageId = (int) (rid.getPosition() / maxRecordsInPage);
    final int positionInPage = (int) (rid.getPosition() % maxRecordsInPage);

    db.transaction(() -> {
      try {
        final MutablePage page = db.getTransaction().getPageToModify(new PageId(db, fileId, pageId), pageSize, false);
        final int slotOffset = Binary.SHORT_SERIALIZED_SIZE + (positionInPage * Binary.INT_SERIALIZED_SIZE);
        final int recordOffset = (int) page.readUnsignedInt(slotOffset);
        assertThat(recordOffset).as("the record must still occupy its slot").isGreaterThan(0);
        mutation.accept(page, recordOffset);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /** Captures WARNING-and-above messages while forwarding every record on, so test output still shows what fired. */
  private static final class CapturingLogger implements Logger {
    private final List<String> captured;
    private final Logger       delegate;

    CapturingLogger(final List<String> captured, final Logger delegate) {
      this.captured = captured;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message) {
      if (message != null && level.intValue() >= Level.WARNING.intValue())
        captured.add(message);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4, final Object arg5,
        final Object arg6, final Object arg7, final Object arg8, final Object arg9, final Object arg10, final Object arg11,
        final Object arg12, final Object arg13, final Object arg14, final Object arg15, final Object arg16,
        final Object arg17) {
      capture(level, message);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
          arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }

  /** Reads a numeric check-database property, failing loudly when the field does not exist. */
  private static long longProperty(final Result row, final String name) {
    final Object value = row.getProperty(name);
    assertThat(value).as("check database must report '%s': %s", name, row.toJSON()).isNotNull();
    return ((Number) value).longValue();
  }
}
