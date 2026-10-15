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
import com.arcadedb.event.BeforeRecordCreateListener;
import com.arcadedb.exception.CauseChain;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.RecordVetoedException;
import com.arcadedb.query.sql.executor.ResultSet;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Issue #10085: a before-create listener that refuses an edge was answered with a raw {@link NullPointerException} out of the
 * adjacency append, or, when the source vertex's edge list was new in the transaction, with an edge that had no identity and
 * nothing stored. {@code newEdge} has to return an edge, so a refusal is now a typed {@link RecordVetoedException} that every
 * protocol classifies as the request being refused; {@link GraphEngine#newEdgeReportingVeto} answers it as {@code null} for a
 * caller that counts the records it wrote, and the batch form skips the refused edge instead of wiring it.
 *
 * @author Luca Garulli (l.garulli@arcadedata.com)
 */
class Issue10085EdgeCreateVetoTest extends TestHelper {
  private static final String V = "V10085";
  private static final String E = "E10085";
  private static final String LIGHT = "ELight10085";
  private static final String REFUSED = "refused";
  private static final String KEPT = "kept";

  private RID source;
  private RID target;
  private RID third;

  @Override
  protected void beginTest() {
    database.getSchema().createVertexType(V);
    database.getSchema().createEdgeType(E).getEvents().registerListener(refuseTag());
    database.getSchema().buildEdgeType().withName(LIGHT).withLightweight(true).create();

    database.transaction(() -> {
      source = database.newVertex(V).save().getIdentity();
      target = database.newVertex(V).save().getIdentity();
      third = database.newVertex(V).save().getIdentity();
    });
  }

  /** The reporter's ordinary case: the source already owns a committed edge segment, so the append dereferenced the null RID. */
  @Test
  void aRefusedEdgeOnASourceThatOwnsACommittedSegmentIsATypedRefusal() {
    database.transaction(() -> source().newEdge(E, target, "tag", KEPT));
    assertThat(edgeCount(E)).isEqualTo(1L);

    final Throwable failure = attempt(() -> source().newEdge(E, third, "tag", REFUSED));

    assertThat(failure).isInstanceOf(RecordVetoedException.class);
    assertThat(failure.getMessage()).contains(E).contains(source.toString()).contains(third.toString());
    assertNothingWasStored(1L);
  }

  /** The other branch: the source's edge list is new in the transaction, and no exception at all used to be raised. */
  @Test
  void aRefusedEdgeOnASourceCreatedInTheSameTransactionIsATypedRefusalToo() {
    final Throwable[] failure = new Throwable[1];
    database.transaction(() -> {
      final MutableVertex fresh = database.newVertex(V).save();
      failure[0] = capture(() -> fresh.newEdge(E, target, "tag", REFUSED));
    });

    assertThat(failure[0]).isInstanceOf(RecordVetoedException.class);
    assertThat(edgeCount(E)).isZero();
  }

  /** A kept edge between the same vertices still works, and comes back with the identity the engine gave it. */
  @Test
  void aKeptEdgeIsUnaffected() {
    final Edge[] kept = new Edge[1];
    database.transaction(() -> kept[0] = source().newEdge(E, target, "tag", KEPT));

    assertThat(kept[0].getIdentity()).isNotNull();
    assertThat(edgeCount(E)).isEqualTo(1L);
    assertThat(source().countEdges(Vertex.DIRECTION.OUT, E)).isEqualTo(1L);
    assertThat(target().countEdges(Vertex.DIRECTION.IN, E)).isEqualTo(1L);
  }

  /** The refusal must not leave the incoming half of a bidirectional edge on the destination. */
  @Test
  void aRefusedEdgeLeavesNeitherHalfOnEitherVertex() {
    database.transaction(() -> source().newEdge(E, target, "tag", KEPT));

    attempt(() -> source().newEdge(E, third, "tag", REFUSED));

    assertThat(source().countEdges(Vertex.DIRECTION.OUT, E)).isEqualTo(1L);
    assertThat(third().countEdges(Vertex.DIRECTION.IN, E)).isZero();
    assertThat(third().countEdges(Vertex.DIRECTION.BOTH, E)).isZero();
  }

  @Test
  void theReportingFormAnswersNullInsteadOfThrowing() {
    database.transaction(() -> source().newEdge(E, target, "tag", KEPT));

    final MutableEdge[] answers = new MutableEdge[3];
    database.transaction(() -> {
      final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
      answers[0] = engine.newEdgeReportingVeto(source(), E, third, "tag", REFUSED);
      answers[1] = engine.newEdgeReportingVeto(source(), E, third, "tag", KEPT);
      final MutableVertex fresh = database.newVertex(V).save();
      answers[2] = engine.newEdgeReportingVeto(fresh, E, third, "tag", REFUSED);
    });

    assertThat(answers[0]).isNull();
    assertThat(answers[1]).isNotNull();
    assertThat(answers[1].getIdentity()).isNotNull();
    assertThat(answers[2]).isNull();
    assertThat(edgeCount(E)).isEqualTo(2L);
    assertThat(third().countEdges(Vertex.DIRECTION.IN, E)).isEqualTo(1L);
  }

  /** The initializer sets the properties BEFORE the save, so the listener sees them and the veto refuses the whole create. */
  @Test
  void theInitializedFormSetsThePropertiesBeforeTheSaveSoTheListenerSeesThem() {
    database.transaction(() -> source().newEdge(E, target, "tag", KEPT));

    final MutableEdge[] answers = new MutableEdge[2];
    database.transaction(() -> {
      final GraphEngine engine = ((DatabaseInternal) database).getGraphEngine();
      answers[0] = engine.newEdgeInitializedReportingVeto(source(), E, third, edge -> edge.set("tag", REFUSED));
      answers[1] = engine.newEdgeInitializedReportingVeto(source(), E, third, edge -> edge.set("tag", KEPT));
    });

    assertThat(answers[0]).isNull();
    assertThat(answers[1]).isNotNull();
    assertThat(answers[1].getString("tag")).isEqualTo(KEPT);
    assertThat(edgeCount(E)).isEqualTo(2L);
    assertThat(third().countEdges(Vertex.DIRECTION.IN, E)).isEqualTo(1L);
  }

  /** An initializer that throws leaves nothing stored or wired, on a record-backed and on a lightweight edge type. */
  @ParameterizedTest
  @ValueSource(strings = { E, LIGHT })
  void anInitializerThatThrowsLeavesNothingStoredOrWired(final String edgeType) {
    final Throwable failure = attempt(() -> ((DatabaseInternal) database).getGraphEngine()
        .newEdgeInitializedReportingVeto(source(), edgeType, third, edge -> {
          throw new IllegalStateException("boom");
        }));

    assertThat(failure).isInstanceOf(IllegalStateException.class);
    assertThat(source().countEdges(Vertex.DIRECTION.OUT, edgeType)).isZero();
    assertThat(third().countEdges(Vertex.DIRECTION.IN, edgeType)).isZero();
    if (E.equals(edgeType))
      assertThat(edgeCount(E)).isZero();
  }

  /** The batch form must not wire a refused edge, and answers only the edges it created. */
  @Test
  void theBatchFormSkipsTheRefusedEdgesAndWiresTheOthers() {
    database.transaction(() -> source().newEdge(E, target, "tag", KEPT));

    final List<Edge> created = new ArrayList<>();
    database.transaction(() -> {
      final List<GraphEngine.CreateEdgeOperation> operations = new ArrayList<>();
      operations.add(new GraphEngine.CreateEdgeOperation(E, third, new Object[] { "tag", REFUSED }));
      operations.add(new GraphEngine.CreateEdgeOperation(E, target, new Object[] { "tag", KEPT }));
      created.addAll(((DatabaseInternal) database).getGraphEngine().newEdges((VertexInternal) source().getRecord(), operations, true));
    });

    assertThat(created).hasSize(1);
    assertThat(created.get(0).getIdentity()).isNotNull();
    assertThat(edgeCount(E)).isEqualTo(2L);
    assertThat(source().countEdges(Vertex.DIRECTION.OUT, E)).isEqualTo(2L);
    assertThat(third().countEdges(Vertex.DIRECTION.IN, E)).isZero();
    assertThat(target().countEdges(Vertex.DIRECTION.IN, E)).isEqualTo(2L);
  }

  /** An asynchronous create reports no success for a refused edge, as it does for a refused document (issue #9974). */
  @Test
  void anAsynchronousEdgeCreateDoesNotReportASuccessForARefusedEdge() {
    database.transaction(() -> source().newEdge(E, target, "tag", KEPT));

    final AtomicInteger succeeded = new AtomicInteger();
    final AtomicInteger errored = new AtomicInteger();
    database.async().onError(e -> errored.incrementAndGet());
    database.async().newEdge(source(), E, third, false, (edge, createdSource, createdDestination) -> succeeded.incrementAndGet(), "tag",
        REFUSED);
    database.async().newEdge(source(), E, third, false, (edge, createdSource, createdDestination) -> succeeded.incrementAndGet(), "tag",
        KEPT);
    database.async().waitCompletion();

    assertThat(succeeded.get()).isEqualTo(1);
    assertThat(errored.get()).isZero();
    assertThat(edgeCount(E)).isEqualTo(2L);
  }

  /** SQL {@code CREATE EDGE} already skipped the refused edge: pinned, so the family stays consistent. */
  @Test
  void sqlCreateEdgeOfARefusedEdgeCreatesNothingAndRaisesNothing() {
    database.transaction(() -> source().newEdge(E, target, "tag", KEPT));

    database.transaction(() -> {
      try (final ResultSet rs = database.command("sql", "CREATE EDGE " + E + " FROM " + source + " TO " + third + " SET tag = '" + REFUSED + "'")) {
        assertThat(rs.hasNext()).isFalse();
      }
    });

    assertNothingWasStored(1L);
  }

  /** openCypher {@code CREATE} reaches {@code newEdge} too: the statement fails with the refusal, classified as the caller's. */
  @Test
  void cypherCreateOfARefusedEdgeFailsWithTheRefusalAndStoresNothing() {
    database.transaction(() -> source().newEdge(E, target, "tag", KEPT));

    final Throwable failure = attempt(() -> {
      try (final ResultSet rs = database.command("opencypher",
          "MATCH (a), (b) WHERE id(a) = '" + source + "' AND id(b) = '" + third + "' CREATE (a)-[:" + E + " {tag: '" + REFUSED + "'}]->(b)")) {
        while (rs.hasNext())
          rs.next();
      }
    });

    assertThat(failure).isNotNull();
    assertThat(CauseChain.contains(failure, RecordVetoedException.class)).isTrue();
    assertThat(ErrorCategory.of(failure)).isEqualTo(ErrorCategory.VALIDATION);
    assertNothingWasStored(1L);
  }

  /** A refusing listener on a record kind that can report it never raises an unnamed exception: the family table of the issue. */
  @ParameterizedTest
  @ValueSource(strings = { "document", "vertex", "edge" })
  void everyRecordKindAnswersTheRefusalWithoutAnUnnamedException(final String kind) {
    database.getSchema().createDocumentType("Doc10085").getEvents().registerListener((BeforeRecordCreateListener) record -> false);
    database.getSchema().createVertexType("Refusing10085").getEvents().registerListener((BeforeRecordCreateListener) record -> false);
    database.getSchema().createEdgeType("RefusingEdge10085").getEvents().registerListener((BeforeRecordCreateListener) record -> false);

    final Object[] answer = new Object[1];
    final Throwable[] failure = new Throwable[1];
    database.transaction(() -> {
      switch (kind) {
      case "document" -> answer[0] = database.newDocument("Doc10085").saveReportingVeto();
      case "vertex" -> answer[0] = database.newVertex("Refusing10085").saveReportingVeto();
      default -> {
        failure[0] = capture(() -> source().newEdge("RefusingEdge10085", third));
        answer[0] = ((DatabaseInternal) database).getGraphEngine().newEdgeReportingVeto(source(), "RefusingEdge10085", third) == null;
      }
      }
    });

    if ("edge".equals(kind)) {
      assertThat(failure[0]).isInstanceOf(RecordVetoedException.class);
      assertThat(answer[0]).isEqualTo(true);
      assertThat(edgeCount("RefusingEdge10085")).isZero();
    } else {
      assertThat(failure[0]).isNull();
      assertThat(answer[0]).isEqualTo(false);
      assertThat(count(kind.equals("document") ? "Doc10085" : "Refusing10085")).isZero();
    }
  }

  /** A lightweight edge owns no record, so there is nothing a before-create listener could refuse: it is always created. */
  @Test
  void aLightweightEdgeIsNeverRefusedByARecordListener() {
    database.getSchema().getType(LIGHT).getEvents().registerListener((BeforeRecordCreateListener) record -> false);

    final Edge[] created = new Edge[2];
    database.transaction(() -> {
      created[0] = source().newEdge(LIGHT, target);
      created[1] = ((DatabaseInternal) database).getGraphEngine().newEdgeReportingVeto(source(), LIGHT, third);
    });

    assertThat(created[0]).isNotNull();
    assertThat(created[1]).isNotNull();
    assertThat(source().countEdges(Vertex.DIRECTION.OUT, LIGHT)).isEqualTo(2L);
  }

  @Test
  void theRefusalIsClassifiedAsTheRequestBeingRefusedAndNotAsAServerFault() {
    assertThat(ErrorCategory.of(new RecordVetoedException("refused"))).isEqualTo(ErrorCategory.VALIDATION);
  }

  // --- helpers ---

  private static BeforeRecordCreateListener refuseTag() {
    return record -> !REFUSED.equals(record.asDocument().getString("tag"));
  }

  private MutableVertex source() {
    return source.asVertex().modify();
  }

  private MutableVertex target() {
    return target.asVertex().modify();
  }

  private MutableVertex third() {
    return third.asVertex().modify();
  }

  private void assertNothingWasStored(final long expectedEdges) {
    assertThat(edgeCount(E)).isEqualTo(expectedEdges);
    assertThat(source().countEdges(Vertex.DIRECTION.OUT, E)).isEqualTo(expectedEdges);
    assertThat(third().countEdges(Vertex.DIRECTION.IN, E)).isZero();
  }

  /** Runs the callback in its own transaction and answers what it threw, which the transaction then rolls back. */
  private Throwable attempt(final Runnable callback) {
    final Throwable[] failure = new Throwable[1];
    database.transaction(() -> failure[0] = capture(callback));
    return failure[0];
  }

  private static Throwable capture(final Runnable callback) {
    try {
      callback.run();
      return null;
    } catch (final Throwable t) {
      return t;
    }
  }

  private long edgeCount(final String type) {
    try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
      return ((Number) rs.next().getProperty("c")).longValue();
    }
  }

  private long count(final String type) {
    return edgeCount(type);
  }
}
