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
import com.arcadedb.exception.ConcurrentModificationException;
import com.arcadedb.exception.DuplicatedKeyException;
import com.arcadedb.exception.ErrorCategory;
import com.arcadedb.exception.NeedRetryException;
import com.arcadedb.exception.RecordNotFoundException;
import com.arcadedb.exception.RecordVetoedException;
import com.arcadedb.exception.VertexNotFoundException;
import com.arcadedb.query.sql.executor.ResultSet;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Regression tests, one nested class per fixed issue.
 */
class EdgeCreationRegressionTest {

  /**
   * Regression for #10085: a before-create listener that refuses an edge was answered with a raw {@link NullPointerException} out of the
   * adjacency append, or, when the source vertex's edge list was new in the transaction, with an edge that had no identity and
   * nothing stored. {@code newEdge} has to return an edge, so a refusal is now a typed {@link RecordVetoedException} that every
   * protocol classifies as the request being refused; {@link GraphEngine#newEdgeReportingVeto} answers it as {@code null} for a
   * caller that counts the records it wrote, and the batch form skips the refused edge instead of wiring it.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeCreateVetoTest extends TestHelper {
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

  /**
   * Regression for #10136: {@link GraphEngine#newEdges} built a record-backed {@link MutableEdge} for every connection, so on a
   * LIGHTWEIGHT edge type it stored a record in the type's bucket and accepted properties, where the single-edge path
   * creates a light edge and refuses them. The storage shape of an edge is a property of its type, not of the call.
   */
  @Nested
  class LightweightBatchEdgesTest extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().buildVertexType().withName("V").create();
        database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
        database.getSchema().buildEdgeType().withName("UL").withBidirectional(false).withLightweight(true).create();
        database.getSchema().buildEdgeType().withName("E").create();
        database.getSchema().buildEdgeType().withName("U").withLightweight(true).withUnique(true).create();
      });
    }

    @Test
    void aBatchOfLightweightEdgesStoresNoRecord() {
      final RID[] v = newVertices(3);

      final List<Edge> created = newEdges(v[0],
          new GraphEngine.CreateEdgeOperation("L", v[1], new Object[0]),
          new GraphEngine.CreateEdgeOperation("L", v[2], new Object[0]));

      assertThat(created).hasSize(2);
      assertThat(created).allMatch(edge -> edge instanceof LightEdge);
      assertThat(database.countType("L", true)).isZero();
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isEqualTo(2L);
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isEqualTo(1L);
      assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isEqualTo(1L);
    }

    /** The incoming side follows the type, not the {@code bidirectional} argument of the batch, as on the single-edge path. */
    @Test
    void aBatchOfUnidirectionalLightweightEdgesDoesNotWireIncoming() {
      final RID[] v = newVertices(2);

      final List<Edge> created = newEdges(v[0], new GraphEngine.CreateEdgeOperation("UL", v[1], new Object[0]));

      assertThat(created).hasSize(1);
      assertThat(created).allMatch(edge -> edge instanceof LightEdge);
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "UL")).isEqualTo(1L);
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "UL")).isZero();
    }

    @Test
    void aBatchAgreesWithTheSingleEdgePath() {
      final RID[] v = newVertices(3);
      database.transaction(() -> v[0].asVertex().newEdge("L", v[1]));

      newEdges(v[0], new GraphEngine.CreateEdgeOperation("L", v[2], new Object[0]));

      assertThat(database.countType("L", true)).isZero();
      final List<Edge> edges = new ArrayList<>();
      v[0].asVertex().getEdges(Vertex.DIRECTION.OUT, "L").forEach(edges::add);
      assertThat(edges).hasSize(2);
      assertThat(edges).allMatch(edge -> edge instanceof LightEdge);
    }

    @Test
    void aLightweightEdgeInABatchRefusesProperties() {
      final RID[] v = newVertices(2);

      assertThatThrownBy(() -> newEdges(v[0], new GraphEngine.CreateEdgeOperation("L", v[1], new Object[] { "weight", 1 })))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining("LIGHTWEIGHT");

      assertThat(database.countType("L", true)).isZero();
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isZero();
    }

    @Test
    void aMixedBatchKeepsTheShapeOfEachType() {
      final RID[] v = newVertices(3);

      final List<Edge> created = newEdges(v[0],
          new GraphEngine.CreateEdgeOperation("E", v[1], new Object[] { "weight", 1 }),
          new GraphEngine.CreateEdgeOperation("L", v[2], new Object[0]));

      assertThat(created).hasSize(2);
      assertThat(database.countType("E", true)).isEqualTo(1L);
      assertThat(database.countType("L", true)).isZero();
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "E")).isEqualTo(1L);
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isEqualTo(1L);
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isEqualTo(1L);
      assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isEqualTo(1L);
    }

    /**
     * The first light edge of a batch creates the source's edge list on the transaction's copy of the vertex: the uniqueness
     * check of the next connection has to see it, or a unique type accepts the same pair twice in one batch.
     */
    @Test
    void aUniqueLightweightTypeRefusesTheSamePairTwiceInOneBatch() {
      final RID[] v = newVertices(2);

      assertThatThrownBy(() -> newEdges(v[0],
          new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0]),
          new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0])))
          .isInstanceOf(DuplicatedKeyException.class);

      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "U")).isZero();
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "U")).isZero();
    }

    private RID[] newVertices(final int count) {
      final RID[] rids = new RID[count];
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          rids[i] = database.newVertex("V").save().getIdentity();
      });
      return rids;
    }

    private List<Edge> newEdges(final RID source, final GraphEngine.CreateEdgeOperation... operations) {
      final List<Edge> created = new ArrayList<>();
      database.transaction(() -> created.addAll(((DatabaseInternal) database).getGraphEngine()
          .newEdges((VertexInternal) source.asVertex(), List.of(operations), true)));
      return created;
    }
  }

  /**
   * Regression for #10196: engine side: an edge whose endpoint this same transaction had already deleted was created and committed. The delete
   * wins over the later write to the vertex record (#7149), so the adjacency update was dropped while the edge record was stored,
   * leaving an edge whose vertex no longer exists; when the deleted vertex had edges, the append met its freed head chunk and was
   * refused as a retryable "concurrent commit in flight".
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeOnDeletedVertexTest extends TestHelper {
    private RID v1;
    private RID v2;

    @BeforeEach
    void seed() {
      database.getSchema().createVertexType("N");
      database.getSchema().createEdgeType("E");
      database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
      database.transaction(() -> {
        v1 = database.newVertex("N").set("uid", 1).save().getIdentity();
        v2 = database.newVertex("N").set("uid", 2).save().getIdentity();
      });
    }

    private long edges() {
      // count(), NOT count(*): THE CACHED COUNTER IS NOT GROUND TRUTH
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM E")) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    @Test
    void edgeFromADeletedVertexIsRefused() {
      database.begin();
      final Vertex source = v1.asVertex();
      source.delete();
      assertThatThrownBy(() -> source.newEdge("E", v2)).isInstanceOf(VertexNotFoundException.class)
          .hasMessageContaining("source vertex " + v1).hasMessageContaining("deleted in this transaction");
      database.commit();

      assertThat(edges()).isZero();
      assertThat(database.existsRecord(v1)).isFalse();
    }

    @Test
    void edgeToADeletedVertexIsRefused() {
      database.begin();
      final Vertex target = v2.asVertex();
      target.delete();
      assertThatThrownBy(() -> v1.asVertex().newEdge("E", target)).isInstanceOf(VertexNotFoundException.class)
          .hasMessageContaining("destination vertex " + v2);
      database.commit();

      assertThat(edges()).isZero();
    }

    @Test
    void edgeFromADeletedVertexWithEdgesIsRefusedAsNotFoundNotAsAConflict() {
      database.transaction(() -> v1.asVertex().newEdge("E", v2));

      database.begin();
      final Vertex source = v1.asVertex();
      source.delete();
      assertThatThrownBy(() -> source.newEdge("E", v2)).isInstanceOf(VertexNotFoundException.class);
      database.commit();

      assertThat(edges()).isZero();
    }

    @Test
    void lightweightEdgeOnADeletedVertexIsRefused() {
      database.begin();
      final Vertex source = v1.asVertex();
      source.delete();
      assertThatThrownBy(() -> source.newEdge("L", v2)).isInstanceOf(VertexNotFoundException.class);
      database.commit();

      assertThat(v2.asVertex().countEdges(Vertex.DIRECTION.IN, "L")).isZero();
    }

    @Test
    void sqlCreateEdgeFromAVertexDeletedEarlierInTheTransactionIsRefused() {
      // THE LOOKUP OF THE ENDPOINT BY RID ALREADY FINDS NOTHING
      database.begin();
      database.command("sql", "DELETE FROM " + v1).close();
      assertThatThrownBy(() -> database.command("sql", "CREATE EDGE E FROM " + v1 + " TO " + v2).close())
          .isInstanceOf(RecordNotFoundException.class);
      database.rollback();

      assertThat(edges()).isZero();
      assertThat(database.existsRecord(v1)).isTrue();
    }

    @Test
    void cypherCreateFromANodeDeletedEarlierInTheStatementIsRefused() {
      // NEO4J REFUSES IT TOO: THE NODE WAS DELETED IN THIS TRANSACTION
      assertThatThrownBy(() -> database.transaction(() -> database.command("opencypher",
          "MATCH (a:N {uid: 1}), (b:N {uid: 2}) DETACH DELETE a CREATE (a)-[:E]->(b)").close()))
          .hasMessageContaining("node `a` was deleted in this transaction");

      assertThat(edges()).isZero();
      assertThat(database.existsRecord(v1)).isTrue();
    }

    @Test
    void anEdgeCreatedBeforeTheDeleteIsStillTakenWithIt() {
      database.transaction(() -> {
        final Vertex source = v1.asVertex();
        source.newEdge("E", v2);
        source.asVertex().delete();
      });

      assertThat(edges()).isZero();
      assertThat(v2.asVertex().countEdges(Vertex.DIRECTION.IN, "E")).isZero();
    }
  }

  /**
   * Regression for #10268: {@link GraphEngine#newEdges} wired the incoming half of every record-backed edge from its {@code bidirectional}
   * argument instead of from the edge type. A unidirectional type got an IN entry the type says cannot exist, and a bidirectional
   * type got a half-wired edge. The direction belongs to the type, as on {@link GraphEngine#newEdge} and the other flag-taking APIs:
   * a unidirectional type ignores {@code bidirectional=true}, and a bidirectional type refuses {@code bidirectional=false}.
   */
  @Nested
  class NewEdgesDirectionFromTypeTest extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().buildVertexType().withName("V").create();
        database.getSchema().buildEdgeType().withName("B").create();
        database.getSchema().buildEdgeType().withName("U").withBidirectional(false).create();
        database.getSchema().buildEdgeType().withName("UL").withBidirectional(false).withLightweight(true).create();
        database.getSchema().buildEdgeType().withName("L").withLightweight(true).create();
      });
    }

    /** The first row of the reproduction in #10136: an IN entry on a unidirectional type. */
    @Test
    void aUnidirectionalTypeGetsNoIncomingSideFromABidirectionalBatch() {
      final RID[] v = newVertices(2);

      final List<Edge> created = newEdges(v[0], true, new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0]));

      assertThat(created).hasSize(1);
      assertThat(database.countType("U", true)).isEqualTo(1L);
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "U")).isEqualTo(1L);
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "U")).isZero();
    }

    /** The second row of the reproduction in #10136: a half-wired edge on a bidirectional type is refused, not stored. */
    @Test
    void aBidirectionalTypeRefusesAUnidirectionalBatch() {
      final RID[] v = newVertices(2);

      assertThatThrownBy(() -> newEdges(v[0], false, new GraphEngine.CreateEdgeOperation("B", v[1], new Object[0])))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(GraphEngine.unidirectionalEdgeOnBidirectionalTypeMessage("B"));

      assertThat(database.countType("B", true)).isZero();
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "B")).isZero();
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "B")).isZero();
    }

    /**
     * The refusal comes before anything is stored: a connection on a unidirectional type earlier in the batch is not created
     * either, even when the caller's own transaction catches the exception and commits.
     */
    @Test
    void theRefusalStoresNothingFromTheRestOfTheBatch() {
      final RID[] v = newVertices(3);

      database.transaction(() -> assertThatThrownBy(() -> graphEngine().newEdges((VertexInternal) v[0].asVertex(),
          List.of(new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0]),
              new GraphEngine.CreateEdgeOperation("B", v[2], new Object[0])), false))
          .isInstanceOf(IllegalArgumentException.class));

      assertThat(database.countType("U", true)).isZero();
      assertThat(database.countType("B", true)).isZero();
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT)).isZero();
    }

    @Test
    void aMixedBatchWiresTheIncomingSideOfEachTypeAsTheTypeSays() {
      final RID[] v = newVertices(4);

      final List<Edge> created = newEdges(v[0], true,
          new GraphEngine.CreateEdgeOperation("B", v[1], new Object[] { "weight", 1 }),
          new GraphEngine.CreateEdgeOperation("U", v[2], new Object[] { "weight", 2 }),
          new GraphEngine.CreateEdgeOperation("B", v[3], new Object[0]));

      assertThat(created).hasSize(3);
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT)).isEqualTo(3L);
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "B")).isEqualTo(1L);
      assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.IN, "U")).isZero();
      assertThat(v[3].asVertex().countEdges(Vertex.DIRECTION.IN, "B")).isEqualTo(1L);

      // THE IN ENTRY NAMES THE EDGE AND THE SOURCE VERTEX, IN THAT ORDER
      final Edge incoming = v[1].asVertex().getEdges(Vertex.DIRECTION.IN, "B").iterator().next();
      assertThat(incoming.getIdentity()).isEqualTo(created.get(0).getIdentity());
      assertThat(incoming.getOut()).isEqualTo(v[0]);
    }

    /** A unidirectional batch is what a unidirectional type is: accepted, for record-backed and lightweight types alike. */
    @Test
    void aUnidirectionalBatchOnUnidirectionalTypesIsAccepted() {
      final RID[] v = newVertices(3);

      final List<Edge> created = newEdges(v[0], false,
          new GraphEngine.CreateEdgeOperation("U", v[1], new Object[0]),
          new GraphEngine.CreateEdgeOperation("UL", v[2], new Object[0]));

      assertThat(created).hasSize(2);
      assertThat(created.get(1)).isInstanceOf(LightEdge.class);
      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT)).isEqualTo(2L);
      assertThat(v[1].asVertex().countEdges(Vertex.DIRECTION.IN, "U")).isZero();
      assertThat(v[2].asVertex().countEdges(Vertex.DIRECTION.IN, "UL")).isZero();
    }

    /** The type check runs on a lightweight type too, where the single-edge path would otherwise ignore the flag. */
    @Test
    void aBidirectionalLightweightTypeRefusesAUnidirectionalBatch() {
      final RID[] v = newVertices(2);

      assertThatThrownBy(() -> newEdges(v[0], false, new GraphEngine.CreateEdgeOperation("L", v[1], new Object[0])))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessage(GraphEngine.unidirectionalEdgeOnBidirectionalTypeMessage("L"));

      assertThat(v[0].asVertex().countEdges(Vertex.DIRECTION.OUT, "L")).isZero();
    }

    private GraphEngine graphEngine() {
      return ((DatabaseInternal) database).getGraphEngine();
    }

    private RID[] newVertices(final int count) {
      final RID[] rids = new RID[count];
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          rids[i] = database.newVertex("V").save().getIdentity();
      });
      return rids;
    }

    private List<Edge> newEdges(final RID source, final boolean bidirectional, final GraphEngine.CreateEdgeOperation... operations) {
      final List<Edge> created = new ArrayList<>();
      database.transaction(
          () -> created.addAll(graphEngine().newEdges((VertexInternal) source.asVertex(), List.of(operations), bidirectional)));
      return created;
    }
  }

  /**
   * Regression for #10325: an edge of a unidirectional type whose destination vertex was deleted by an earlier, committed transaction was
   * refused only at commit, as a retryable {@link ConcurrentModificationException} ("deleted by a concurrent transaction. Please
   * retry"), which no retry can ever satisfy. A destination already gone when the edge is created is a not-found, as SQL
   * {@code CREATE EDGE} and every bidirectional type answer it; the retryable refusal stays for a target deleted concurrently.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class UnidirectionalEdgeToMissingVertexTest extends TestHelper {
    private RID source;
    private RID gone;

    @BeforeEach
    void seed() {
      database.command("sql", "CREATE VERTEX TYPE V").close();
      database.command("sql", "CREATE EDGE TYPE U UNIDIRECTIONAL").close();
      database.getSchema().buildEdgeType().withName("UL").withLightweight(true).withBidirectional(false).create();
      database.transaction(() -> {
        source = database.newVertex("V").save().getIdentity();
        gone = database.newVertex("V").save().getIdentity();
      });
      database.transaction(() -> gone.asVertex().delete());
    }

    private long edges(final String type) {
      // count(), NOT count(*): THE CACHED COUNTER IS NOT GROUND TRUTH
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
        return rs.next().<Number>getProperty("c").longValue();
      }
    }

    private void assertRefusedAsNotFound(final Runnable creation) {
      database.begin();
      try {
        assertThatThrownBy(creation::run).isInstanceOf(RecordNotFoundException.class).isNotInstanceOf(NeedRetryException.class)
            .hasMessageContaining(gone.toString());
      } finally {
        database.rollback();
      }
      assertThat(edges("U")).isZero();
    }

    @Test
    void vertexNewEdgeByRidIsRefusedAsNotFound() {
      assertRefusedAsNotFound(() -> source.asVertex().newEdge("U", gone));
    }

    @Test
    void newEdgesUnidirectionalIsRefusedAsNotFound() {
      assertRefusedAsNotFound(() -> ((DatabaseInternal) database).getGraphEngine()
          .newEdges((VertexInternal) source.asVertex(), List.of(new GraphEngine.CreateEdgeOperation("U", gone, new Object[0])), false));
    }

    @Test
    void newEdgesBidirectionalFlagOnAUnidirectionalTypeIsRefusedAsNotFound() {
      assertRefusedAsNotFound(() -> ((DatabaseInternal) database).getGraphEngine()
          .newEdges((VertexInternal) source.asVertex(), List.of(new GraphEngine.CreateEdgeOperation("U", gone, new Object[0])), true));
    }

    @Test
    void aBatchWithAMissingDestinationLeavesNoEdgeOfItBehind() {
      // THE SOURCE ITSELF AS THE DESTINATION THAT EXISTS: A VERTEX CREATED HERE WOULD TAKE THE FREED SLOT OF THE DELETED ONE
      final RID[] alive = { source };

      // THE REFUSAL IS CAUGHT AND THE TRANSACTION COMMITTED: THE EDGE BEFORE THE MISSING DESTINATION MUST NOT HAVE BEEN SAVED, OR IT
      // WOULD BE COMMITTED WITHOUT ITS ENTRY IN THE SOURCE'S OUT LIST
      database.begin();
      assertThatThrownBy(() -> ((DatabaseInternal) database).getGraphEngine().newEdges((VertexInternal) source.asVertex(),
          List.of(new GraphEngine.CreateEdgeOperation("U", alive[0], new Object[0]),
              new GraphEngine.CreateEdgeOperation("U", gone, new Object[0])), false)).isInstanceOf(RecordNotFoundException.class);
      database.commit();

      assertThat(edges("U")).isZero();
      assertThat(source.asVertex().countEdges(Vertex.DIRECTION.OUT, "U")).isZero();
    }

    @Test
    void lightweightUnidirectionalEdgeIsRefusedAsNotFound() {
      database.begin();
      try {
        assertThatThrownBy(() -> source.asVertex().newEdge("UL", gone)).isInstanceOf(RecordNotFoundException.class)
            .isNotInstanceOf(NeedRetryException.class);
      } finally {
        database.rollback();
      }
      assertThat(source.asVertex().countEdges(Vertex.DIRECTION.OUT, "UL")).isZero();
    }

    @Test
    void sqlCreateEdgeStillRefusesAsNotFound() {
      assertRefusedAsNotFound(() -> database.command("sql", "CREATE EDGE U FROM " + source + " TO " + gone).close());
    }

    @Test
    void aDestinationInABucketThatDoesNotExistIsRefusedAsNotFound() {
      final RID dangling = new RID(999_999, 0);
      database.begin();
      try {
        assertThatThrownBy(() -> source.asVertex().newEdge("U", dangling)).isInstanceOf(RecordNotFoundException.class)
            .isNotInstanceOf(NeedRetryException.class);
      } finally {
        database.rollback();
      }
      assertThat(edges("U")).isZero();
    }

    @Test
    void aDestinationCreatedInTheSameTransactionIsAccepted() {
      database.transaction(() -> {
        final Vertex fresh = database.newVertex("V").save();
        source.asVertex().newEdge("U", fresh.getIdentity());
      });
      assertThat(edges("U")).isEqualTo(1);
    }

    @Test
    void aTargetDeletedConcurrentlyAfterTheEdgeWasCreatedIsStillARetryableConflict() throws Exception {
      final RID[] target = new RID[1];
      database.transaction(() -> target[0] = database.newVertex("V").save().getIdentity());

      database.begin();
      source.asVertex().newEdge("U", target[0]);

      // ANOTHER THREAD DELETES THE TARGET AND COMMITS WHILE THIS TRANSACTION IS STILL OPEN
      final CountDownLatch deleted = new CountDownLatch(1);
      final AtomicReference<Throwable> failure = new AtomicReference<>();
      final Thread deleter = new Thread(() -> {
        try {
          database.transaction(() -> target[0].asVertex().delete());
        } catch (final Throwable t) {
          failure.set(t);
        } finally {
          deleted.countDown();
        }
      });
      deleter.start();
      deleted.await();
      deleter.join();
      assertThat(failure.get()).isNull();

      assertThatThrownBy(() -> database.commit()).isInstanceOf(ConcurrentModificationException.class);
      assertThat(edges("U")).isZero();
    }
  }
}
