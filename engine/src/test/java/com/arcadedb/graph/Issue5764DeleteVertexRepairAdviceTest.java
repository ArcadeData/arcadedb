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
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.log.LogManager;
import com.arcadedb.log.Logger;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Level;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * #5764: what {@code GraphEngine.deleteVertex} tells an operator when it cannot walk an edge list, and what it
 * keeps of the failure that made it say so.
 * <p>
 * #5680 made the delete strict and #5710 added {@code CHECK DATABASE RECORD}, but they landed separately, so every
 * recovery hint the delete emitted still predated the scope it was written for: they all advised a whole-database
 * or whole-type run, costing two full passes over the vertex type plus an edge sweep, while the operator was
 * holding the one piece of information - the RID - that makes the repair cheap. And the retryable arm rethrew the
 * conflict bare, so what surfaced was {@code getEdgeHeadChunkForWrite}'s "concurrent commit in flight", which is
 * the right diagnosis for the transient case nobody ever sees and says nothing about the permanent one that
 * reaches a human.
 * <p>
 * These tests pin the advice by EXECUTING it: the command is parsed out of the message the delete raised and run
 * verbatim, and the delete must then go through. A message that named the wrong RID, or a command shape the SQL
 * parser does not accept, fails here rather than in production.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue5764DeleteVertexRepairAdviceTest extends TestHelper {

  /** These tests deliberately break an edge-list chain, so the blanket end-of-test check would always fire. */
  @Override
  protected boolean isCheckingDatabaseIntegrity() {
    return false;
  }

  private static final Pattern SCOPED_COMMAND = Pattern.compile("`(CHECK DATABASE RECORD [^`]+)`");

  /**
   * The case the {@code RECORD} scope exists for: the vertex's OWN list is unreadable, so the delete is REFUSED and
   * the vertex is still there to be repaired. The message must name the command aimed at that vertex, and running
   * exactly what it says must make the delete succeed and take every edge with it.
   */
  @Test
  void theRefusedDeleteNamesAScopedRepairCommandThatReallyRepairsIt() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 200);

    deleteRecord(inChunkChain(hubRID).get(0));

    final Throwable thrown = catchThrowable(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1));
    assertThat(thrown).isInstanceOf(ConcurrentModificationException.class);

    final String command = scopedRepairCommandIn(thrown.getMessage());
    assertThat(command).as("the delete must name the scoped repair: %s", thrown.getMessage())
        .isEqualTo("CHECK DATABASE RECORD " + hubRID + " FIX");
    // The original diagnosis is not replaced by the advice, only extended by it.
    assertThat(thrown.getMessage()).contains("concurrent commit in flight");

    // Not decoration: the exact string the operator was handed is what repairs the graph.
    try (final ResultSet rs = database.command("sql", command)) {
      assertThat(rs.hasNext()).isTrue();
    }

    database.transaction(() -> hubRID.asVertex().delete(), false, 1);

    database.transaction(() -> {
      assertThat(database.existsRecord(hubRID)).isFalse();
      for (final RID edge : edges)
        assertThat(database.existsRecord(edge)).as("edge " + edge + " outlived its vertex").isFalse();
    });
  }

  /**
   * The list that needs rebuilding is not always the one being deleted. When the conflict comes from disconnecting
   * a collected edge at its OTHER end, the broken list belongs to the NEIGHBOUR - so that is the RID the advice has
   * to name. Naming the vertex under delete would send the operator to repair a record that is perfectly healthy.
   */
  @Test
  void aConflictRaisedByTheNeighbourNamesTheNeighbourAndNotTheVertexBeingDeleted() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 20);
    final RID srcRID = outVertexOf(edges.get(edges.size() - 1));

    deleteRecord(inHeadChunk(hubRID));

    final Throwable thrown = catchThrowable(() -> database.transaction(() -> srcRID.asVertex().delete(), false, 1));
    assertThat(thrown).isInstanceOf(ConcurrentModificationException.class);

    final String command = scopedRepairCommandIn(thrown.getMessage());
    assertThat(command).as("%s", thrown.getMessage()).isEqualTo("CHECK DATABASE RECORD " + hubRID + " FIX");
    assertThat(command).as("the healthy vertex under delete must not be the repair target")
        .doesNotContain(srcRID.toString());
  }

  /**
   * The conflict keeps the failure that produced it. A retryable is normally absorbed and never seen, so the run
   * that does surface one is the retry-exhausted run - exactly the run whose stack trace has to be diagnosable, and
   * the one that used to arrive with the cause discarded because
   * {@code ConcurrentModificationException} had no {@code (message, cause)} constructor at all.
   */
  @Test
  void theConflictCarriesTheOriginalFailureAsItsCause() {
    createSchema();
    final RID hubRID = createHub();
    createEdges(hubRID, 200);

    deleteRecord(inChunkChain(hubRID).get(0));

    final Throwable thrown = catchThrowable(() -> database.transaction(() -> hubRID.asVertex().delete(), false, 1));
    assertThat(thrown).isInstanceOf(ConcurrentModificationException.class);
    assertThat(thrown.getCause()).as("the advice must WRAP the conflict, not replace it").isNotNull();

    final List<Throwable> chain = causeChainOf(thrown);
    assertThat(chain).as("cause chain: %s", chain)
        .anyMatch(RecordNotFoundException.class::isInstance);
  }

  /**
   * The other outcome, and the reason it gets DIFFERENT advice: under {@code force} the delete goes THROUGH, so by
   * the time the warning is logged the record a scoped check would be aimed at no longer exists. Pointing there
   * would be worse than the whole-database form it replaced - it names a repair that cannot run. The message must
   * therefore keep the whole-database command for the references left dangling, and say what the scoped form buys
   * when run BEFORE the delete.
   * <p>
   * NOTE on the capture: {@link LogManager} is a singleton, so swapping its logger is PROCESS-WIDE for the duration
   * of the forced delete below. Restored in a {@code finally}, and safe under the suite as it runs today (surefire
   * executes classes sequentially within a fork), but it is shared state: if class-level parallelism is ever
   * enabled in this module, a concurrent test's WARNING output would land in {@code warnings} here and this test's
   * delegate would be whatever that test installed. There is no per-invocation seam to capture through - the log
   * call sites go straight to the singleton - so the alternative is asserting on nothing, which is worse than a
   * documented window.
   */
  @Test
  void aForcedDeleteAdvisesTheWholeDatabaseFixBecauseTheScopedFormCannotHelpAfterwards() {
    createSchema();
    final RID hubRID = createHub();
    final List<RID> edges = createEdges(hubRID, 1);
    final RID srcRID = outVertexOf(edges.get(0));

    deleteRecord(outHeadChunk(srcRID));

    final List<String> warnings = new CopyOnWriteArrayList<>();
    // getLogger(), not reflection on the private field: LogManager exposes the accessor for exactly this - a
    // caller that temporarily swaps the logger and has to put the original back.
    final Logger originalLogger = LogManager.instance().getLogger();
    LogManager.instance().setLogger(new CapturingLogger(warnings, originalLogger));
    try {
      database.transaction(() -> graphEngine().deleteVertex((VertexInternal) srcRID.asVertex(), true));
    } finally {
      LogManager.instance().setLogger(originalLogger);
    }

    database.transaction(() -> assertThat(database.existsRecord(srcRID)).isFalse());

    final List<String> advice = warnings.stream().filter(w -> w.contains(srcRID.toString())).toList();
    assertThat(advice).as("the forced delete must say what it left behind (captured=%s)", warnings).isNotEmpty();
    assertThat(advice).allSatisfy(w -> {
      assertThat(w).as("%s", w).contains("CHECK DATABASE FIX");
      // The scoped form appears only as the placeholder shape describing what to do BEFORE a delete. It must never
      // be handed out with the RID substituted here: the record it would name is the one just deleted.
      assertThat(scopedRepairCommandIn(w)).as("%s", w).isEqualTo("CHECK DATABASE RECORD <vertex> FIX");
      assertThat(w).as("%s", w).doesNotContain("CHECK DATABASE RECORD " + srcRID);
    });
  }

  /** The scoped repair command inside a message, or {@code null} when it names none. */
  private static String scopedRepairCommandIn(final String message) {
    if (message == null)
      return null;
    final Matcher m = SCOPED_COMMAND.matcher(message);
    return m.find() ? m.group(1) : null;
  }

  private static List<Throwable> causeChainOf(final Throwable e) {
    final List<Throwable> chain = new ArrayList<>();
    for (Throwable current = e; current != null && !chain.contains(current); current = current.getCause())
      chain.add(current);
    return chain;
  }

  private GraphEngine graphEngine() {
    return ((DatabaseInternal) database).getGraphEngine();
  }

  private void createSchema() {
    database.transaction(() -> {
      database.getSchema().createVertexType("Hub", 1);
      database.getSchema().createVertexType("Src", 16);
      database.getSchema().createEdgeType("LINK", 16);
    });
  }

  private RID createHub() {
    final MutableVertex[] holder = new MutableVertex[1];
    database.transaction(() -> {
      holder[0] = database.newVertex("Hub");
      holder[0].save();
    });
    return holder[0].getIdentity();
  }

  /** One edge per transaction, so the hub's IN chain grows chunk by chunk exactly as it does in production. */
  private List<RID> createEdges(final RID hubRID, final int count) {
    final List<RID> edges = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      final RID[] holder = new RID[1];
      database.transaction(() -> {
        final MutableVertex src = database.newVertex("Src");
        src.save();
        holder[0] = src.newEdge("LINK", hubRID).getIdentity();
      });
      edges.add(holder[0]);
    }
    return edges;
  }

  private RID outVertexOf(final RID edgeRID) {
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = edgeRID.asEdge().getOut());
    return holder[0];
  }

  private RID outHeadChunk(final RID vertexRID) {
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getOutEdgesHeadChunk());
    assertThat(holder[0]).as("the vertex must have an outgoing edge list").isNotNull();
    return holder[0];
  }

  private RID inHeadChunk(final RID vertexRID) {
    final RID[] holder = new RID[1];
    database.transaction(() -> holder[0] = ((VertexInternal) vertexRID.asVertex()).getInEdgesHeadChunk());
    assertThat(holder[0]).as("the vertex must have an incoming edge list").isNotNull();
    return holder[0];
  }

  /** The hub's IN chunk chain, head first (newest chunk) to tail (the chunk created with the first edge). */
  private List<RID> inChunkChain(final RID hubRID) {
    final List<RID> chain = new ArrayList<>();
    database.transaction(() -> {
      RID rid = ((VertexInternal) hubRID.asVertex()).getInEdgesHeadChunk();
      while (rid != null) {
        chain.add(rid);
        rid = ((EdgeSegment) database.lookupByRID(rid, true)).getPreviousRID();
      }
    });
    return chain;
  }

  private void deleteRecord(final RID rid) {
    database.transaction(() -> database.getSchema().getBucketById(rid.getBucketId()).deleteRecord(rid));
    database.transaction(() -> assertThat(database.existsRecord(rid)).isFalse());
  }

  /**
   * Captures WARNING-and-above messages into a list while forwarding every record to the production logger, so the
   * test output still shows what fired.
   */
  private static final class CapturingLogger implements Logger {
    private final List<String> warnings;
    private final Logger       delegate;

    CapturingLogger(final List<String> warnings, final Logger delegate) {
      this.warnings = warnings;
      this.delegate = delegate;
    }

    private void capture(final Level level, final String message, final Object... args) {
      if (message == null || level.intValue() < Level.WARNING.intValue())
        return;
      String formatted = message;
      if (args != null && args.length > 0) {
        try {
          formatted = message.formatted(args);
        } catch (final Exception ignored) {
          // Fall back to the raw template - good enough for the substring matching above.
        }
      }
      warnings.add(formatted);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object arg1, final Object arg2, final Object arg3, final Object arg4,
        final Object arg5, final Object arg6, final Object arg7, final Object arg8, final Object arg9,
        final Object arg10, final Object arg11, final Object arg12, final Object arg13, final Object arg14,
        final Object arg15, final Object arg16, final Object arg17) {
      capture(level, message, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9, arg10, arg11, arg12, arg13, arg14,
          arg15, arg16, arg17);
      delegate.log(requester, level, message, exception, context, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9,
          arg10, arg11, arg12, arg13, arg14, arg15, arg16, arg17);
    }

    @Override
    public void log(final Object requester, final Level level, final String message, final Throwable exception,
        final String context, final Object... args) {
      capture(level, message, args);
      delegate.log(requester, level, message, exception, context, args);
    }

    @Override
    public void flush() {
      delegate.flush();
    }
  }
}
