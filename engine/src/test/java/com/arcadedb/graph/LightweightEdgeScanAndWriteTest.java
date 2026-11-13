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

import com.arcadedb.GlobalConfiguration;
import com.arcadedb.TestHelper;
import com.arcadedb.database.Database;
import com.arcadedb.database.DatabaseContext;
import com.arcadedb.database.DatabaseFactory;
import com.arcadedb.database.DatabaseInternal;
import com.arcadedb.database.RID;
import com.arcadedb.engine.Bucket;
import com.arcadedb.engine.DatabaseChecker;
import com.arcadedb.engine.LocalBucket;
import com.arcadedb.event.AfterRecordDeleteListener;
import com.arcadedb.event.BeforeRecordDeleteListener;
import com.arcadedb.exception.CommandExecutionException;
import com.arcadedb.query.sql.executor.BasicCommandContext;
import com.arcadedb.query.sql.executor.Result;
import com.arcadedb.query.sql.executor.ResultSet;
import com.arcadedb.query.sql.parser.LocalResultSet;
import com.arcadedb.query.sql.parser.SelectStatement;
import com.arcadedb.schema.DocumentType;
import com.arcadedb.schema.Property;
import com.arcadedb.security.SecurityDatabaseUser;
import com.arcadedb.serializer.json.JSONObject;
import com.arcadedb.utility.Pair;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Regression tests of the LightweightEdgeScanAndWrite area, one nested class per former stand-alone regression class.
 */
class LightweightEdgeScanAndWriteTest {
  /**
   * Issue #7477, reported as discussion #7473: a LIGHTWEIGHT edge type stores its edges inside the two vertices and
   * allocates no record, so the bucket a type scan reads is empty by construction. Every query that named the type
   * therefore answered zero - {@code SELECT FROM CITE}, {@code SELECT count(*) FROM CITE} and Studio's record count
   * alike - on a graph that held 75 million edges, which is what the bulk load in the report looked like when it had
   * in fact worked.
   * <p>
   * The scan now walks the vertices that hold those edges. This pins what it returns, that the count agrees with it,
   * and that neither a regular edge type nor the cheap {@code @out}/{@code @in} rewrite changed on the way.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class LightweightEdgeScanTest extends TestHelper {

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().buildVertexType().withName("Work").create();
        database.getSchema().buildEdgeType().withName("Cite").withLightweight(true).create();
        database.getSchema().buildEdgeType().withName("Wrote").create();
      });
    }

    @Test
    void aLightweightEdgeTypeIsScannedThroughTheVerticesThatHoldIt() {
      final RID[] works = newWorks(3);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[0], works[2]);
      connect("Cite", works[1], works[2]);

      final List<Result> edges = query("select from Cite");
      assertThat(edges).hasSize(3);
      for (final Result edge : edges) {
        assertThat(edge.isEdge()).isTrue();
        assertThat(edge.getEdge().get().getTypeName()).isEqualTo("Cite");
      }

      assertThat(pairs("select from Cite")).containsExactlyInAnyOrder(
          works[0] + "->" + works[1], works[0] + "->" + works[2], works[1] + "->" + works[2]);
    }

    @Test
    void theCountAgreesWithTheScan() {
      final RID[] works = newWorks(3);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[0], works[2]);

      // The count push-down reads countType(), which counts records: it has to decline on a type that keeps none, or
      // it answers 0 for a scan that returns 2.
      assertThat(query("select count(*) as c from Cite").getFirst().<Long>getProperty("c")).isEqualTo(2L);
      assertThat(query("select from Cite")).hasSize(2);
      // ...while the record count of the type itself is still, correctly, zero
      assertThat(database.countType("Cite", true)).isZero();
    }

    /**
     * The target of a count can be a context variable, resolved only at execution time - so the check cannot live in
     * the planner, where the name is still {@code $t}. It sits in {@link CountFromTypeStep} instead, and this pins
     * that the indirection does not reopen the issue one step away from the case above (issue #7477).
     */
    @Test
    void theCountAgreesWithTheScanThroughAVariableTargetToo() {
      final RID[] works = newWorks(3);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[1], works[2]);

      assertThat(varTargetQuery("SELECT count(*) as c FROM $t").getFirst().<Long>getProperty("c")).isEqualTo(2L);
      assertThat(varTargetQuery("SELECT FROM $t")).hasSize(2);

      // ...and a regular edge type reached the same way is unaffected
      connect("Wrote", works[0], works[2]);
      assertThat(varTargetQuery("SELECT count(*) as c FROM $t", "Wrote").getFirst().<Long>getProperty("c")).isEqualTo(1L);
    }

    /** SKIP/LIMIT and the page boundary of the step's own {@code nRecords} batching. */
    @Test
    void theScanPaginates() {
      final RID[] works = newWorks(20);
      for (int i = 1; i < works.length; i++)
        connect("Cite", works[0], works[i]);

      assertThat(query("select from Cite")).hasSize(19);
      assertThat(query("select from Cite limit 5")).hasSize(5);
      assertThat(query("select from Cite skip 15")).hasSize(4);
    }

    /** A regular edge type keeps the bucket scan: its edges are records, and there is no vertex walk to pay for. */
    @Test
    void aRegularEdgeTypeIsUnchanged() {
      final RID[] works = newWorks(2);
      connect("Wrote", works[0], works[1]);

      assertThat(query("select from Wrote")).hasSize(1);
      assertThat(query("select count(*) as c from Wrote").getFirst().<Long>getProperty("c")).isEqualTo(1L);
      assertThat(explain("select from Wrote")).contains("FETCH FROM TYPE Wrote");
    }

    /**
     * A supertype that holds records with a lightweight subtype under it: the scan must return both shapes, and each
     * edge exactly once. The record edges come from the bucket, the lightweight ones from the vertex walk, and the
     * walk tells them apart by the storage shape of the entry rather than by what its type declares.
     */
    @Test
    void aMixedHierarchyReturnsEachEdgeOnce() {
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Mentions").create());
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Quotes").withLightweight(true)
          .withSuperType("Mentions").create());

      final RID[] works = newWorks(3);
      connect("Mentions", works[0], works[1]);
      connect("Quotes", works[0], works[2]);
      connect("Quotes", works[1], works[2]);

      assertThat(pairs("select from Mentions")).containsExactlyInAnyOrder(
          works[0] + "->" + works[1], works[0] + "->" + works[2], works[1] + "->" + works[2]);
      assertThat(query("select count(*) as c from Mentions").getFirst().<Long>getProperty("c")).isEqualTo(3L);

      assertThat(pairs("select from Quotes")).containsExactlyInAnyOrder(
          works[0] + "->" + works[2], works[1] + "->" + works[2]);
    }

    /**
     * A {@code @rid} filter can only ever name a record-backed edge - a lightweight one has no addressable identity -
     * so the RID short-circuit is correct on such a type and must keep winning over the walk. It matters on a mixed
     * hierarchy, where the record half is the only half a RID can reach and the walk would otherwise be paid in full
     * to answer a single-row lookup (issue #7477, and the RID short-circuit of #5824).
     */
    @Test
    void theRidShortCircuitStillWinsOnAMixedHierarchy() {
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Mentions").create());
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Quotes").withLightweight(true)
          .withSuperType("Mentions").create());

      final RID[] works = newWorks(3);
      connect("Mentions", works[0], works[1]);
      connect("Quotes", works[0], works[2]);

      final RID recordEdge = query("select from Mentions").stream()
          .map(r -> r.getEdge().get().getIdentity())
          .filter(rid -> rid.getPosition() >= 0)
          .findFirst().orElseThrow();

      assertThat(explain("select from Mentions where @rid = " + recordEdge))
          .as("the walk must not be paid to answer a single-row RID lookup")
          .doesNotContain("FETCH LIGHTWEIGHT EDGES OF TYPE");
      assertThat(query("select from Mentions where @rid = " + recordEdge)).hasSize(1);

      // ...and the unfiltered scan of the same type still returns both shapes
      assertThat(query("select from Mentions")).hasSize(2);
    }

    /**
     * The rewrite that reaches the edges of ONE vertex is both cheaper and already correct for a lightweight type, so
     * it must keep winning over the whole-graph walk.
     */
    @Test
    void theVertexRidRewriteStillWins() {
      final RID[] works = newWorks(3);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[0], works[2]);
      connect("Cite", works[1], works[2]);

      assertThat(explain("select from Cite where @out = " + works[0])).contains("FETCH EDGES FROM VERTEX");
      assertThat(query("select from Cite where @out = " + works[0])).hasSize(2);
      assertThat(query("select from Cite where @in = " + works[2])).hasSize(2);
    }

    /**
     * The whole design rests on the outgoing entry always being written while the incoming one is the optional half,
     * so walking OUT alone is complete and yields each edge exactly once. A non-bidirectional LIGHTWEIGHT type is the
     * combination that would break if that ever stopped being true, and it is the one no other test constructs
     * (issue #7477).
     */
    @Test
    void aNonBidirectionalLightweightTypeIsWalkedCompletelyAndOnce() {
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Follows").withLightweight(true)
          .withBidirectional(false).create());

      final RID[] works = newWorks(3);
      connect("Follows", works[0], works[1]);
      connect("Follows", works[0], works[2]);
      connect("Follows", works[1], works[2]);

      database.transaction(() -> assertThat(database.lookupByRID(works[2], true).asVertex()
          .countEdges(Vertex.DIRECTION.IN, "Follows"))
          .as("precondition: nothing was written to the incoming side, so only the OUT walk can find these")
          .isZero());

      assertThat(pairs("select from Follows")).containsExactlyInAnyOrder(
          works[0] + "->" + works[1], works[0] + "->" + works[2], works[1] + "->" + works[2]);
      assertThat(query("select count(*) as c from Follows").getFirst().<Long>getProperty("c")).isEqualTo(3L);
    }

    /**
     * Issue #7480: {@code TraverseExecutionPlanner} now routes a LIGHTWEIGHT edge type target to the same vertex walk
     * SELECT uses (issue #7477/#7478), and {@code AbstractTraverseStep}'s dedup no longer collapses every lightweight
     * edge of a type into one. Before the dedup fix, routing the planner alone reproduced exactly this: a graph
     * holding 3 {@code Cite} edges answered 1 row instead of 3, because {@code RidHashSet} keys on
     * {@code (bucketId, position)} - the edge type's bucket and the placeholder position -1 - which every lightweight
     * edge of the type shares.
     */
    @Test
    void traverseOnALightweightEdgeTypeReturnsEveryEdgeNotJustOne() {
      final RID[] works = newWorks(3);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[0], works[2]);
      connect("Cite", works[1], works[2]);

      assertThat(query("select from Cite")).as("precondition: the SELECT side is fixed").hasSize(3);

      assertThat(query("traverse in, out from Cite while $depth < 1"))
          .as("every lightweight edge of the type must be its own row, not collapsed into one")
          .hasSize(3);

      // a regular edge type is unaffected
      connect("Wrote", works[0], works[2]);
      assertThat(query("traverse in, out from Wrote while $depth < 1")).hasSize(1);
    }

    /** The dedup fix lives in the shared base step, so BREADTH_FIRST must be just as unaffected as DEPTH_FIRST. */
    @Test
    void traverseBreadthFirstOnALightweightEdgeTypeReturnsEveryEdgeNotJustOne() {
      final RID[] works = newWorks(3);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[0], works[2]);
      connect("Cite", works[1], works[2]);

      assertThat(query("traverse in, out from Cite while $depth < 1 strategy breadth_first")).hasSize(3);
    }

    /**
     * {@code DepthFirstTraverseStep} has its own second dedup set ({@code emitted}, gating final emission in
     * {@code fetchNextResults}) in addition to the shared {@code traversed} one ({@code fetchNextEntryPoints}) -
     * both used to be a plain {@code RidHashSet} with the same collapsing defect, and DEPTH_FIRST is the default
     * strategy this whole test class already exercises, so both are pinned together: a vertex whose OUT list holds
     * more than one lightweight edge to peer vertices is exactly the case where every one of those edges shares the
     * same (bucketId, offset) pair - distinct {@code LightEdgeRID}s that a plain {@code RidHashSet} would collapse.
     */
    @Test
    void traverseDepthFirstEmissionDedupDoesNotCollapseLightweightSiblingEdges() {
      final RID[] works = newWorks(4);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[0], works[2]);
      connect("Cite", works[0], works[3]);

      assertThat(query("traverse in, out from Cite while $depth < 1 strategy depth_first")).hasSize(3);
    }

    /**
     * The walk opens every vertex type in the schema, not only the ones the edge type connects - nothing records
     * which vertex types an edge type has endpoints in. That is the documented cost, and this pins the correctness
     * half of it: an unrelated vertex type, including one carrying edges of a different type, changes neither what
     * the scan returns nor what the count says.
     */
    @Test
    void anUnrelatedVertexTypeIsWalkedWithoutDisturbingTheResult() {
      database.transaction(() -> {
        database.getSchema().buildVertexType().withName("Author").create();
        database.getSchema().buildEdgeType().withName("Knows").withLightweight(true).create();
      });

      final RID[] works = newWorks(2);
      connect("Cite", works[0], works[1]);

      final RID[] authors = new RID[2];
      database.transaction(() -> {
        for (int i = 0; i < authors.length; i++)
          authors[i] = database.newVertex("Author").set("id", i).save().getIdentity();
      });
      connect("Knows", authors[0], authors[1]);

      assertThat(pairs("select from Cite")).containsExactly(works[0] + "->" + works[1]);
      assertThat(query("select count(*) as c from Cite").getFirst().<Long>getProperty("c")).isEqualTo(1L);

      // ...and symmetrically, from the other type's point of view
      assertThat(pairs("select from Knows")).containsExactly(authors[0] + "->" + authors[1]);
      assertThat(query("select count(*) as c from Knows").getFirst().<Long>getProperty("c")).isEqualTo(1L);
    }

    /**
     * UPDATE over a MIXED hierarchy reaches both shapes, so it modifies the record-backed edges and then refuses at
     * the first lightweight one. The statement is therefore not atomic on its own - it is the transaction that makes
     * it so, which is the ordinary ArcadeDB contract and is what this pins: the refusal names the reason, and the
     * rollback leaves the record half as it was rather than half-updated.
     */
    @Test
    void updateOverAMixedHierarchyRefusesAndRollsBackWholly() {
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Mentions").create());
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Quotes").withLightweight(true)
          .withSuperType("Mentions").create());

      final RID[] works = newWorks(3);
      connect("Mentions", works[0], works[1]);
      connect("Quotes", works[0], works[2]);

      assertThatThrownBy(() -> database.transaction(
          () -> database.command("sql", "update Mentions set since = 2020").close()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Lightweight edges cannot be modified");

      // the record-backed half was modified before the refusal, and the transaction took it back with everything else
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "select since from Mentions")) {
          while (rs.hasNext())
            assertThat(rs.next().<Object>getProperty("since"))
                .as("no edge of a mixed hierarchy may survive a refused UPDATE half-written").isNull();
        }
      });

      // ...and the scan itself is unharmed
      assertThat(query("select from Mentions")).hasSize(2);
    }

    /**
     * TRUNCATE used to delete what {@code scanType()} finds, and that reads the type's buckets directly rather than
     * going through the planner - so the walk never reached it and the statement reported success having deleted
     * nothing. It now delegates to the same removal-safe path {@code DELETE FROM <type>} already takes correctly
     * (issue #7481): a lightweight type has no indexes to drop, so the index-drop/rebuild speed-up
     * {@code truncateInOwnTransaction} exists for buys nothing here.
     */
    @Test
    void truncateOnALightweightEdgeTypeActuallyDeletesTheEdges() {
      final RID[] works = newWorks(3);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[0], works[2]);
      connect("Cite", works[1], works[2]);

      database.command("sql", "truncate type Cite unsafe").close();

      assertThat(query("select from Cite")).as("TRUNCATE must actually reach the edges now").isEmpty();
      database.transaction(() -> {
        assertThat(database.lookupByRID(works[0], true).asVertex().countEdges(Vertex.DIRECTION.OUT, "Cite")).isZero();
        assertThat(database.lookupByRID(works[1], true).asVertex().countEdges(Vertex.DIRECTION.OUT, "Cite")).isZero();
        assertThat(database.lookupByRID(works[1], true).asVertex().countEdges(Vertex.DIRECTION.IN, "Cite")).isZero();
        assertThat(database.lookupByRID(works[2], true).asVertex().countEdges(Vertex.DIRECTION.IN, "Cite")).isZero();
      });
    }

    /** The fast (own-transaction) path and the caller-transaction path must both actually delete. */
    @Test
    void truncateOnALightweightEdgeTypeWorksInsideACallerTransactionToo() {
      final RID[] works = newWorks(2);
      connect("Cite", works[0], works[1]);

      database.transaction(() -> database.command("sql", "truncate type Cite unsafe").close());

      assertThat(query("select from Cite")).isEmpty();
    }

    /** A ROLLBACK inside a caller transaction must put the lightweight edges back, exactly as DELETE FROM does. */
    @Test
    void truncateOnALightweightEdgeTypeInsideACallerTransactionRollsBack() {
      final RID[] works = newWorks(2);
      connect("Cite", works[0], works[1]);

      database.begin();
      database.command("sql", "truncate type Cite unsafe").close();
      assertThat(query("select from Cite")).isEmpty();
      database.rollback();

      assertThat(query("select from Cite")).as("the rollback must put the edge back").hasSize(1);
    }

    /**
     * A hierarchy where the named type itself is not lightweight but a subtype below it is.
     * <p>
     * This used to be REFUSED without POLYMORPHIC, on the grounds that there is no bucket-only truncation that also
     * reaches a subtype's lightweight edges. Reaching them is precisely what "not POLYMORPHIC" asks it NOT to do,
     * though, and {@code Mentions} is record-backed: it has a bucket of its own, and clearing exactly that bucket is
     * what a non-polymorphic TRUNCATE means for any type with subtypes, lightweight or not. The refusal survived
     * #7668 only because its guard still read the conflated {@code holdsLightweightEdges()} - true for a
     * record-backed root that merely has a lightweight type below it - while #7668's own execution split had already
     * made this shape well defined (issue #7919).
     */
    @Test
    void truncateOnAMixedHierarchyScopesToTheRootWithoutPolymorphicAndDeletesBothShapesWithIt() {
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Mentions").create());
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Quotes").withLightweight(true)
          .withSuperType("Mentions").create());

      final RID[] works = newWorks(3);
      connect("Mentions", works[0], works[1]);
      connect("Quotes", works[0], works[2]);

      database.command("sql", "truncate type Mentions unsafe").close();
      assertThat(query("select from Mentions"))
          .as("only the root's own bucket is cleared; the lightweight subtype's edge was not in scope")
          .hasSize(1);

      database.command("sql", "truncate type Mentions polymorphic unsafe").close();
      assertThat(query("select from Mentions")).isEmpty();
    }

    /**
     * The shape the structural refusal is actually for: a LIGHTWEIGHT ROOT with a subtype. That one has no bucket to
     * scope a non-polymorphic TRUNCATE to, so the only way to serve it is the unconditionally-polymorphic
     * {@code DELETE FROM}, which would reach into the subtype the caller did not ask for (issue #7481/#7919).
     */
    @Test
    void truncateOnALightweightRootWithASubtypeRefusesWithoutPolymorphic() {
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Hints").withLightweight(true).create());
      database.transaction(() -> database.getSchema().buildEdgeType().withName("StrongHints").withSuperType("Hints").create());

      final RID[] works = newWorks(2);
      connect("Hints", works[0], works[1]);

      assertThatThrownBy(() -> database.command("sql", "truncate type Hints").close())
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("POLYMORPHIC");
      assertThat(query("select from Hints")).as("a refused TRUNCATE must not have deleted anything").hasSize(1);
    }

    /** A lightweight LEAF type (no subtypes of its own) truncates the same way with or without POLYMORPHIC. */
    @Test
    void truncateOnALightweightLeafTypeIgnoresThePolymorphicFlag() {
      final RID[] works = newWorks(2);
      connect("Cite", works[0], works[1]);

      database.command("sql", "truncate type Cite polymorphic unsafe").close();

      assertThat(query("select from Cite")).isEmpty();
    }

    /** UNSAFE keeps meaning "the type is not empty", not "force past a structural refusal". */
    @Test
    void truncateOnALightweightRootWithASubtypeStillRefusesWithoutPolymorphicEvenWithUnsafe() {
      database.transaction(() -> database.getSchema().buildEdgeType().withName("Hints").withLightweight(true).create());
      database.transaction(() -> database.getSchema().buildEdgeType().withName("StrongHints").withSuperType("Hints").create());

      final RID[] works = newWorks(2);
      connect("Hints", works[0], works[1]);

      assertThatThrownBy(() -> database.command("sql", "truncate type Hints unsafe").close())
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("POLYMORPHIC");
    }

    /**
     * TRUNCATE rebuilds {@code DELETE FROM <type>} as SQL text, so a type name that needs back-tick quoting (a
     * reserved word, here) has to come back out through {@code Identifier.toString()} rather than the plain,
     * unescaped {@code getStringValue()} - otherwise the rebuilt text fails to parse instead of deleting anything.
     */
    @Test
    void truncateOnALightweightEdgeTypeWithAReservedWordNameStillDeletes() {
      database.transaction(() -> database.command("sql", "CREATE EDGE TYPE `SELECT` LIGHTWEIGHT").close());

      final RID[] works = newWorks(2);
      connect("SELECT", works[0], works[1]);

      assertThat(query("select from `SELECT`")).hasSize(1);

      database.command("sql", "truncate type `SELECT` unsafe").close();

      assertThat(query("select from `SELECT`")).isEmpty();
    }

    /**
     * The own-transaction path now carries the same {@code BATCH} commit-and-reopen cycle
     * {@link com.arcadedb.query.sql.executor.BatchStep} gives the record-backed path, so a lightweight type with more
     * edges than the configured batch size must still delete every one of them across several committed transactions,
     * not just the first batch.
     */
    @Test
    void truncateOnALightweightEdgeTypeDeletesEverythingAcrossSeveralBatches() {
      final int batchSize = 4;
      database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE, batchSize);
      try {
        final RID[] works = newWorks(1 + 3 * batchSize);
        for (int i = 1; i < works.length; i++)
          connect("Cite", works[0], works[i]);

        assertThat(query("select from Cite")).hasSize(works.length - 1);

        database.command("sql", "truncate type Cite unsafe").close();

        assertThat(query("select from Cite")).isEmpty();
        database.transaction(() -> assertThat(database.lookupByRID(works[0], true).asVertex()
            .countEdges(Vertex.DIRECTION.OUT, "Cite")).isZero());
      } finally {
        database.getConfiguration().setValue(GlobalConfiguration.TRUNCATE_BATCH_SIZE,
            GlobalConfiguration.TRUNCATE_BATCH_SIZE.getDefValue());
      }
    }

    /**
     * {@code Database.countType()} sums bucket record counts, and a LIGHTWEIGHT edge allocates none - so it answers 0
     * for such a type no matter how many edges it holds ({@link #theCountAgreesWithTheScan} pins exactly this). The
     * "not empty, needs UNSAFE" guard in {@code TruncateTypeStatement} reads that same count, so a LIGHTWEIGHT edge
     * type must not be able to bypass it just because the count it relies on lies for this storage shape (review
     * finding on #7481).
     * <p>
     * The {@code E} super type this fixture builds is a leftover from when the guard asked
     * {@code isSubTypeOf("E")} by name and this hierarchy was the only way to reach it at all. Every other truncate
     * in this class needs UNSAFE now, which is the same finding seen from the other side (issue #8042). The super
     * type is kept because it costs nothing and pins that the explicit hierarchy does not CHANGE the answer either.
     */
    @Test
    void truncateOnALightweightEdgeTypeUnderARealEHierarchyStillRequiresUnsafeWhenNotEmpty() {
      database.transaction(() -> {
        database.getSchema().createEdgeType("E");
        database.getSchema().buildEdgeType().withName("CiteUnderE").withLightweight(true).withSuperType("E").create();
      });

      final RID[] works = newWorks(2);
      connect("CiteUnderE", works[0], works[1]);

      assertThatThrownBy(() -> database.command("sql", "truncate type CiteUnderE").close())
          .isInstanceOf(CommandExecutionException.class)
          .hasMessageContaining("not empty")
          .hasMessageContaining("UNSAFE");
      assertThat(query("select from CiteUnderE")).as("a refused TRUNCATE must not have deleted anything").hasSize(1);

      database.command("sql", "truncate type CiteUnderE unsafe").close();
      assertThat(query("select from CiteUnderE")).isEmpty();
    }

    /** A regular edge type truncates as it always did. */
    @Test
    void truncateOnARegularEdgeTypeIsUnchanged() {
      final RID[] works = newWorks(2);
      connect("Wrote", works[0], works[1]);

      database.command("sql", "truncate type Wrote unsafe").close();

      assertThat(query("select from Wrote")).isEmpty();
    }

    /** The walk is the plan, so EXPLAIN has to name it: it is O(V + E) where a bucket scan reads O(E). */
    @Test
    void explainNamesTheVertexWalk() {
      assertThat(explain("select from Cite")).contains("FETCH LIGHTWEIGHT EDGES OF TYPE Cite");
    }

    @Test
    void anEmptyGraphStillAnswersEmpty() {
      assertThat(query("select from Cite")).isEmpty();
      assertThat(query("select count(*) as c from Cite").getFirst().<Long>getProperty("c")).isZero();
    }

    /** A filter on the endpoints is applied on top of the walk, not lost by it. */
    @Test
    void aFilterOverTheWalkIsApplied() {
      final RID[] works = newWorks(3);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[1], works[2]);

      assertThat(pairs("select from Cite where @in = " + works[2] + " or @in = " + works[1]))
          .containsExactlyInAnyOrder(works[0] + "->" + works[1], works[1] + "->" + works[2]);
    }

    /**
     * DELETE and UPDATE resolve their target through the same planner, so both used to address an empty bucket and
     * silently do nothing on a lightweight type. Both now reach the edges: the delete removes them from BOTH vertices'
     * edge lists, and the update refuses with the sentence that says why rather than reporting a no-op as a success.
     */
    @Test
    void deleteReachesTheEdgesAndUpdateSaysWhyItCannot() {
      final RID[] works = newWorks(3);
      connect("Cite", works[0], works[1]);
      connect("Cite", works[1], works[2]);

      assertThat(query("delete from Cite where @in = " + works[2]).getFirst().<Long>getProperty("count")).isEqualTo(1L);

      database.transaction(() -> {
        assertThat(database.lookupByRID(works[1], true).asVertex().countEdges(Vertex.DIRECTION.OUT, "Cite"))
            .as("the deleted edge must be gone from the source vertex's edge list").isZero();
        assertThat(database.lookupByRID(works[2], true).asVertex().countEdges(Vertex.DIRECTION.IN, "Cite"))
            .as("...and from the destination's").isZero();
      });

      assertThat(query("select from Cite")).hasSize(1);

      assertThatThrownBy(() -> database.transaction(() -> database.command("sql", "update Cite set since = 2020").close()))
          .isInstanceOf(IllegalStateException.class)
          .hasMessageContaining("Lightweight edges cannot be modified");
    }

    private RID[] newWorks(final int count) {
      final RID[] rids = new RID[count];
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          rids[i] = database.newVertex("Work").set("id", i).save().getIdentity();
      });
      return rids;
    }

    private void connect(final String edgeType, final RID from, final RID to) {
      database.transaction(() -> database.lookupByRID(from, true).asVertex().modify().newEdge(edgeType, to));
    }

    private List<Result> query(final String sql) {
      final List<Result> results = new ArrayList<>();
      database.transaction(() -> {
        try (final ResultSet rs = database.command("sql", sql)) {
          while (rs.hasNext())
            results.add(rs.next());
        }
      });
      return results;
    }

    private List<String> pairs(final String sql) {
      final List<String> pairs = new ArrayList<>();
      for (final Result r : query(sql))
        pairs.add(r.getEdge().get().getOut() + "->" + r.getEdge().get().getIn());
      return pairs;
    }

    private List<Result> varTargetQuery(final String sql) {
      return varTargetQuery(sql, "Cite");
    }

    /** Runs {@code sql} with {@code $t} bound to {@code typeName}, the way {@code SELECT FROM $t} is driven. */
    private List<Result> varTargetQuery(final String sql, final String typeName) {
      final List<Result> results = new ArrayList<>();
      database.transaction(() -> {
        final SelectStatement statement = (SelectStatement) ((DatabaseInternal) database).getStatementCache().get(sql);
        final BasicCommandContext context = new BasicCommandContext();
        context.setDatabase(database);
        context.setVariable("$t", typeName);
        try (final ResultSet rs = new LocalResultSet(statement.createExecutionPlan(context))) {
          while (rs.hasNext())
            results.add(rs.next());
        }
      });
      return results;
    }

    private String explain(final String sql) {
      final StringBuilder plan = new StringBuilder();
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "explain " + sql)) {
          while (rs.hasNext())
            plan.append(rs.next().toJSON());
        }
      });
      return plan.toString();
    }
  }

  /**
   * Issue #9619: a LIGHTWEIGHT edge allocates no record, so {@code LocalBucket.createRecord}/{@code deleteRecord} - where
   * {@code CREATE_RECORD} and {@code DELETE_RECORD} are enforced for every other record - never run for it, and both grants
   * were inert on such a type. The user below resolves permissions exactly as the server does: the grant map is keyed on the
   * type's INVOLVED buckets only, so the vertex type stays fully writable and only the edge type is refused.
   */
  @Nested
  class LightweightEdgeWriteGrantsTest {
    private static final String PATH = "target/databases/LightweightEdgeScanAndWriteTest.LightweightEdgeWriteGrantsTest";

    private DatabaseFactory factory;
    private Database        database;
    private RID             a;
    private RID             b;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE LW LIGHTWEIGHT");
      database.command("sql", "CREATE EDGE TYPE HE");

      database.transaction(() -> {
        a = database.newVertex("V").set("id", "a").save().getIdentity();
        b = database.newVertex("V").set("id", "b").save().getIdentity();
      });
    }

    @AfterEach
    void tearDown() {
      unbindUser();
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      factory.close();
    }

    // ---------------------------------------------------------------- DELETE

    @Test
    void sqlDeleteOfALightweightEdgeIsRefused() {
      connectLW();
      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

      database.begin();
      assertThat(catchThrowable(() -> database.command("sql", "DELETE FROM LW"))).isInstanceOf(SecurityException.class);
      database.commit();

      unbindUser();
      assertLWConnected(1);
    }

    @Test
    void apiDeleteOfALightweightEdgeIsRefused() {
      connectLW();
      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

      database.begin();
      final Edge edge = a.asVertex().getEdges(Vertex.DIRECTION.OUT, "LW").iterator().next();
      assertThat(edge).isInstanceOf(LightEdge.class);
      assertThat(catchThrowable(edge::delete)).isInstanceOf(SecurityException.class);
      database.commit();

      unbindUser();
      assertLWConnected(1);
    }

    @Test
    void cypherDeleteOfALightweightEdgeIsRefused() {
      connectLW();
      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

      database.begin();
      assertThat(catchThrowable(() -> database.command("opencypher", "MATCH (:V)-[r:LW]->(:V) DELETE r"))).isInstanceOf(
          SecurityException.class);
      if (database.isTransactionActive())
        database.rollback();

      unbindUser();
      assertLWConnected(1);
    }

    /** The cascade from a vertex delete reaches the lightweight edge the same way it reaches a record-backed one. */
    @Test
    void vertexDeleteCascadingToARefusedLightweightEdgeIsRefused() {
      connectLW();
      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

      database.begin();
      assertThat(catchThrowable(() -> database.deleteRecord(a.asVertex()))).isInstanceOf(SecurityException.class);
      if (database.isTransactionActive())
        database.rollback();

      unbindUser();
      assertThat(database.countType("V", false)).isEqualTo(2);
      assertLWConnected(1);
    }

    /** The record-backed control in the same shape, to pin that both edge kinds now answer identically. */
    @Test
    void sqlDeleteOfARecordBackedEdgeIsRefusedToo() {
      database.transaction(() -> a.asVertex().newEdge("HE", b));
      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "HE");

      database.begin();
      assertThat(catchThrowable(() -> database.command("sql", "DELETE FROM HE"))).isInstanceOf(SecurityException.class);
      database.commit();

      unbindUser();
      assertThat(database.countType("HE", false)).isEqualTo(1);
    }

    @Test
    void grantedLightweightDeleteStillWorks() {
      connectLW();
      // refused on another type only: LW stays deletable
      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "HE");

      database.transaction(() -> database.command("sql", "DELETE FROM LW"));

      unbindUser();
      assertLWConnected(0);
    }

    // ---------------------------------------------------------------- CREATE

    @Test
    void sqlCreateOfALightweightEdgeIsRefused() {
      bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");

      database.begin();
      assertThat(catchThrowable(() -> database.command("sql", "CREATE EDGE LW FROM " + a + " TO " + b))).isInstanceOf(
          SecurityException.class);
      database.commit();

      unbindUser();
      assertLWConnected(0);
    }

    @Test
    void apiNewEdgeOnALightweightTypeIsRefused() {
      bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");

      database.begin();
      assertThat(catchThrowable(() -> a.asVertex().modify().newEdge("LW", b))).isInstanceOf(SecurityException.class);
      database.commit();

      unbindUser();
      assertLWConnected(0);
    }

    /** The deprecated per-call light edge, on a type that does not declare LIGHTWEIGHT: still gated by the type's grant. */
    @Test
    @SuppressWarnings("deprecation")
    void deprecatedNewLightEdgeIsRefused() {
      bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "HE");

      database.begin();
      assertThat(catchThrowable(() -> a.asVertex().modify().newLightEdge("HE", b))).isInstanceOf(SecurityException.class);
      database.commit();

      unbindUser();
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "HE")).isZero();
    }

    @Test
    void cypherCreateOfALightweightEdgeIsRefused() {
      bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");

      database.begin();
      assertThat(catchThrowable(() -> database.command("opencypher",
          "MATCH (x:V {id:'a'}), (y:V {id:'b'}) CREATE (x)-[:LW]->(y)"))).isInstanceOf(SecurityException.class);
      if (database.isTransactionActive())
        database.rollback();

      unbindUser();
      assertLWConnected(0);
    }

    @Test
    void graphBatchLightweightEdgeIsRefused() {
      bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");

      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(10).build()) {
        assertThat(catchThrowable(() -> batch.newEdge(a, "LW", b))).isInstanceOf(SecurityException.class);
      }

      unbindUser();
      assertLWConnected(0);
    }

    /** A grant checked for one user must not carry over to the next user bound to the same batch's thread. */
    @Test
    void graphBatchRechecksTheGrantWhenTheUserChanges() {
      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(10).build()) {
        batch.newEdge(a, "LW", b);

        bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW");
        assertThat(catchThrowable(() -> batch.newEdge(b, "LW", a))).isInstanceOf(SecurityException.class);
        unbindUser();
      }

      assertThat(countLW()).isEqualTo(1);
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW")).isEqualTo(1);
      assertThat(b.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW")).isZero();
    }

    @Test
    void grantedLightweightCreateStillWorks() {
      bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "HE");

      database.transaction(() -> database.command("sql", "CREATE EDGE LW FROM " + a + " TO " + b));
      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(10).build()) {
        batch.newEdge(b, "LW", a);
      }

      unbindUser();
      assertThat(countLW()).isEqualTo(2);
    }

    // ---------------------------------------------------------------- MULTI-BUCKET

    /**
     * A lightweight type with several buckets, created after an indexed type so that index files sit between bucket files
     * and the type's buckets are not the dense low ids of a fresh database. Pins the two assumptions the checks rest on: the
     * RID's bucket id is the file id the grant is keyed on, and the type's first bucket answers for all of them.
     */
    @Test
    void multiBucketLightweightTypeIsGatedOnCreateAndDelete() {
      database.command("sql", "CREATE VERTEX TYPE Indexed");
      database.command("sql", "CREATE PROPERTY Indexed.k STRING");
      database.command("sql", "CREATE INDEX ON Indexed (k) UNIQUE");
      database.command("sql", "CREATE EDGE TYPE LW4 LIGHTWEIGHT BUCKETS 4");

      final DocumentType lw4 = database.getSchema().getType("LW4");
      assertThat(lw4.getBuckets(false)).hasSize(4);
      for (final Bucket bucket : lw4.getBuckets(false))
        assertThat(database.getSchema().getBucketById(bucket.getFileId()).getFileId()).isEqualTo(bucket.getFileId());

      bindUserRefusing(SecurityDatabaseUser.ACCESS.CREATE_RECORD, "LW4");
      database.begin();
      assertThat(catchThrowable(() -> database.command("sql", "CREATE EDGE LW4 FROM " + a + " TO " + b))).isInstanceOf(
          SecurityException.class);
      database.commit();
      try (final GraphBatch batch = GraphBatch.builder(database).withBatchSize(10).build()) {
        assertThat(catchThrowable(() -> batch.newEdge(a, "LW4", b))).isInstanceOf(SecurityException.class);
      }
      unbindUser();
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW4")).isZero();

      database.transaction(() -> database.command("sql", "CREATE EDGE LW4 FROM " + a + " TO " + b));
      final Edge edge = a.asVertex().getEdges(Vertex.DIRECTION.OUT, "LW4").iterator().next();
      assertThat(edge).isInstanceOf(LightEdge.class);
      assertThat(edge.getIdentity().getBucketId()).isEqualTo(lw4.getFirstBucketId());

      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW4");
      database.begin();
      assertThat(catchThrowable(() -> database.command("sql", "DELETE FROM LW4"))).isInstanceOf(SecurityException.class);
      database.commit();
      unbindUser();
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW4")).isEqualTo(1);
    }

    // ---------------------------------------------------------------- helpers

    private void connectLW() {
      database.transaction(() -> database.command("sql", "CREATE EDGE LW FROM " + a + " TO " + b));
      assertLWConnected(1);
    }

    private void assertLWConnected(final long expected) {
      assertThat(countLW()).isEqualTo(expected);
      assertThat(a.asVertex().countEdges(Vertex.DIRECTION.OUT, "LW")).isEqualTo(expected);
      assertThat(b.asVertex().countEdges(Vertex.DIRECTION.IN, "LW")).isEqualTo(expected);
    }

    private long countLW() {
      try (final ResultSet rs = database.query("sql", "SELECT count(*) AS c FROM LW")) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private void unbindUser() {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    }

    /** Same resolution as ServerSecurityDatabaseUser: only the type's involved bucket ids are listed, the rest is default-allow. */
    private void bindUserRefusing(final SecurityDatabaseUser.ACCESS refused, final String typeName) {
      final DocumentType type = database.getSchema().getType(typeName);
      final Set<Integer> gated = new HashSet<>();
      for (final Bucket bucket : type.getInvolvedBuckets())
        gated.add(bucket.getFileId());
      assertThat(gated).isNotEmpty();

      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "restricted";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return true;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return access != refused || !gated.contains(fileId);
        }

        @Override
        public boolean requestAccessOnType(final String name, final ACCESS access) {
          return access != refused || !typeName.equals(name);
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }
  }

  /**
   * Issue #9779: {@code Iterator.remove()} on the iterators a vertex hands out over its edge lists.
   * <ul>
   * <li>the untyped edge and entry iterators deleted the edge inside a {@code catch (Exception)} that logged a refused
   * delete and then unlinked the entry anyway: a LIGHTWEIGHT edge the user may not delete was gone from the source and
   * still listed on the target, a record-backed one survived reachable from one endpoint only;</li>
   * <li>the TYPED iterators ({@code getEdges(OUT, "T")}, {@code getVertices(OUT, "T")}) did not delete anything: they
   * unlinked the entry from the walked list only, with no {@code DELETE_RECORD} check at all, leaving the edge record and
   * the far endpoint's entry behind;</li>
   * <li>a second {@code remove()} on the untyped iterators dropped a SIBLING edge: the delete removed the entry from the
   * walked list by RID and the iterator then removed, by position, the entry that had moved into its place.</li>
   * </ul>
   * The user below resolves permissions as {@code ServerSecurityDatabaseUser} does (see
   * {@code LightweightEdgeScanAndWriteTest.LightweightEdgeWriteGrantsTest}): only the edge type's buckets are refused, the vertex type stays writable.
   *
   * @author Luca Garulli (l.garulli@arcadedata.com)
   */
  @Nested
  class EdgeIteratorRemoveTest {
    private static final String PATH = "target/databases/LightweightEdgeScanAndWriteTest.EdgeIteratorRemoveTest";

    /** Every iterator over one vertex's OUT list that supports {@code remove()}. */
    enum Walk {
      EDGES((v, type) -> v.getEdges(Vertex.DIRECTION.OUT).iterator()),
      TYPED_EDGES((v, type) -> v.getEdges(Vertex.DIRECTION.OUT, type).iterator()),
      VERTICES((v, type) -> v.getVertices(Vertex.DIRECTION.OUT).iterator()),
      TYPED_VERTICES((v, type) -> v.getVertices(Vertex.DIRECTION.OUT, type).iterator()),
      ENTRIES((v, type) -> outList(v).entryIterator()),
      TYPED_ENTRIES((v, type) -> outList(v).entryIterator(type));

      private final BiFunction<Vertex, String, Iterator<?>> open;

      Walk(final BiFunction<Vertex, String, Iterator<?>> open) {
        this.open = open;
      }

      Iterator<?> open(final Vertex vertex, final String type) {
        return open.apply(vertex, type);
      }

      private static EdgeLinkedList outList(final Vertex v) {
        return ((DatabaseInternal) v.getDatabase()).getGraphEngine().getEdgeHeadChunk((VertexInternal) v, Vertex.DIRECTION.OUT);
      }
    }

    private DatabaseFactory factory;
    private Database        database;
    private RID             a;
    private RID[]           targets;

    @BeforeEach
    void setUp() {
      factory = new DatabaseFactory(PATH).setSecurity(db -> {
      });
      if (factory.exists())
        factory.open().drop();
      database = factory.create();

      database.command("sql", "CREATE VERTEX TYPE V");
      database.command("sql", "CREATE EDGE TYPE LW LIGHTWEIGHT");
      database.command("sql", "CREATE EDGE TYPE HE");

      database.transaction(() -> {
        a = database.newVertex("V").set("id", "a").save().getIdentity();
        targets = new RID[3];
        for (int i = 0; i < targets.length; i++)
          targets[i] = database.newVertex("V").set("id", "t" + i).save().getIdentity();
      });
    }

    @AfterEach
    void tearDown() {
      unbindUser();
      if (database.isTransactionActive())
        database.rollback();
      database.drop();
      factory.close();
    }

    // ---------------------------------------------------------------- A REFUSED DELETE CHANGES NOTHING

    @ParameterizedTest
    @EnumSource(Walk.class)
    void refusedRemoveOfALightweightEdgeReachesTheCallerAndKeepsBothSides(final Walk walk) {
      connect("LW", 1);
      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");

      database.begin();
      final Iterator<?> it = walk.open(vertex(a), "LW");
      it.next();
      assertThat(catchThrowable(it::remove)).isInstanceOf(SecurityException.class);
      database.commit();

      unbindUser();
      assertConnected("LW", 1);
    }

    @ParameterizedTest
    @EnumSource(Walk.class)
    void refusedRemoveOfARecordBackedEdgeReachesTheCallerAndKeepsBothSides(final Walk walk) {
      connect("HE", 1);
      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "HE");

      database.begin();
      final Iterator<?> it = walk.open(vertex(a), "HE");
      it.next();
      assertThat(catchThrowable(it::remove)).isInstanceOf(SecurityException.class);
      database.commit();

      unbindUser();
      assertConnected("HE", 1);
    }

    /** The issue's own repro: the state survives a close and reopen. */
    @Test
    void refusedRemoveLeavesNothingBehindAcrossAReopen() {
      connect("LW", 1);
      connect("HE", 1);
      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "LW");
      database.begin();
      final Iterator<Edge> lw = vertex(a).getEdges(Vertex.DIRECTION.OUT, "LW").iterator();
      lw.next();
      assertThat(catchThrowable(lw::remove)).isInstanceOf(SecurityException.class);
      database.commit();
      unbindUser();

      bindUserRefusing(SecurityDatabaseUser.ACCESS.DELETE_RECORD, "HE");
      database.begin();
      final Iterator<Edge> he = vertex(a).getEdges(Vertex.DIRECTION.OUT, "HE").iterator();
      he.next();
      assertThat(catchThrowable(he::remove)).isInstanceOf(SecurityException.class);
      database.commit();
      unbindUser();

      database.close();
      database = factory.open();

      assertConnected("LW", 1);
      assertConnected("HE", 1);
    }

    // ---------------------------------------------------------------- A GRANTED REMOVE DELETES EXACTLY THE EDGE RETURNED

    @ParameterizedTest
    @EnumSource(Walk.class)
    void twoConsecutiveRemovesOfLightweightEdgesDeleteExactlyThoseTwo(final Walk walk) {
      twoConsecutiveRemovesDeleteExactlyThoseTwo(walk, "LW");
    }

    @ParameterizedTest
    @EnumSource(Walk.class)
    void twoConsecutiveRemovesOfRecordBackedEdgesDeleteExactlyThoseTwo(final Walk walk) {
      twoConsecutiveRemovesDeleteExactlyThoseTwo(walk, "HE");
    }

    /** {@code Iterator.remove()} is about the element {@code next()} returned, even after a {@code hasNext()} peeked the following one. */
    @ParameterizedTest
    @EnumSource(Walk.class)
    void removeAfterHasNextDeletesTheReturnedEdgeNotThePeekedOne(final Walk walk) {
      connect("HE", 3);

      final Set<RID> removed = new HashSet<>();
      final Set<RID> visited = new HashSet<>();
      database.transaction(() -> {
        final Iterator<?> it = walk.open(vertex(a), "HE");
        final RID first = farEnd(it.next());
        visited.add(first);
        assertThat(it.hasNext()).isTrue();
        it.remove();
        removed.add(first);
        while (it.hasNext())
          visited.add(farEnd(it.next()));
      });

      // every edge was visited once, and the one removed is the one returned first
      assertThat(visited).hasSize(3);
      assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "HE")).isEqualTo(2);
      assertThat(countType("HE")).isEqualTo(2);
      for (final RID target : targets)
        assertThat(vertex(target).countEdges(Vertex.DIRECTION.IN, "HE")).isEqualTo(removed.contains(target) ? 0 : 1);
      assertCheckIsClean();
    }

    /**
     * The same over a list of many segments, removing every other element after a {@code hasNext()}: the peek lands in the
     * next segment at each segment's last entry, where the cursor fix-up must not apply, and in the same segment everywhere
     * else, where it must.
     */
    @ParameterizedTest
    @EnumSource(Walk.class)
    void peekThenRemoveAcrossSegmentBoundariesDeletesExactlyTheReturnedEdges(final Walk walk) {
      final int degree = 300;
      final RID[] far = new RID[degree];
      database.transaction(() -> {
        for (int i = 0; i < degree; i++) {
          far[i] = database.newVertex("V").set("id", "f" + i).save().getIdentity();
          vertex(a).modify().newEdge("LW", far[i]);
        }
      });
      final EdgeSegment head = (EdgeSegment) database.lookupByRID(((VertexInternal) vertex(a)).getOutEdgesHeadChunk(), true);
      assertThat(head.getPrevious()).as("the list spans several segments").isNotNull();

      final Set<RID> removed = new HashSet<>();
      final Set<RID> visited = new HashSet<>();
      database.transaction(() -> {
        final Iterator<?> it = walk.open(vertex(a), "LW");
        boolean remove = true;
        while (it.hasNext()) {
          final RID target = farEnd(it.next());
          assertThat(visited.add(target)).as("each edge is visited once").isTrue();
          if (remove) {
            it.hasNext();
            it.remove();
            removed.add(target);
          }
          remove = !remove;
        }
      });

      assertThat(visited).hasSize(degree);
      assertThat(removed).hasSize(degree / 2);
      assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "LW")).isEqualTo(degree / 2);
      for (final RID target : far)
        assertThat(vertex(target).countEdges(Vertex.DIRECTION.IN, "LW")).isEqualTo(removed.contains(target) ? 0 : 1);
      assertCheckIsClean();
    }

    /** A before-delete listener that vetoes the delete leaves the edge in place on both sides, as with {@code edge.delete()}. */
    @ParameterizedTest
    @EnumSource(Walk.class)
    void aVetoedDeleteKeepsTheEntry(final Walk walk) {
      connect("HE", 2);
      final BeforeRecordDeleteListener veto = record -> !(record instanceof Edge);
      database.getEvents().registerListener(veto);
      try {
        database.transaction(() -> {
          final Iterator<?> it = walk.open(vertex(a), "HE");
          it.next();
          it.remove();
          assertThat(catchThrowable(it::remove)).isInstanceOf(IllegalStateException.class);
        });
      } finally {
        database.getEvents().unregisterListener(veto);
      }
      assertConnected("HE", 2);
      assertCheckIsClean();
    }

    /**
     * A delete listener that writes to the walked list while {@code remove()} deletes - a cascade delete of a sibling edge,
     * a new edge from the same vertex - must not make the positional removal take out the wrong entry or overwrite the
     * listener's write. The graph stays consistent; an iterator whose view of the list went stale refuses to go on with a
     * {@link ConcurrentModificationException}, as java.util collections do.
     */
    @ParameterizedTest
    @EnumSource(Walk.class)
    void aDeleteListenerWritingToTheWalkedListKeepsTheGraphConsistent(final Walk walk) {
      for (final String type : new String[] { "HE", "LW" })
        for (final boolean cascade : new boolean[] { true, false }) {
          connect(type, 3);
          final boolean[] inListener = { false };
          final AfterRecordDeleteListener listener = record -> {
            if (inListener[0] || !(record instanceof Edge))
              return;
            inListener[0] = true;
            try {
              if (cascade) {
                // DELETE THE EDGE TO targets[2], WHICHEVER EDGE THE ITERATOR REMOVED
                for (final Edge edge : vertex(a).getEdges(Vertex.DIRECTION.OUT, type))
                  if (edge.getIn().equals(targets[2])) {
                    edge.delete();
                    break;
                  }
              } else
                vertex(a).modify().newEdge(type, targets[2]);
            } finally {
              inListener[0] = false;
            }
          };
          database.getEvents().registerListener(listener);
          final RID[] removed = new RID[1];
          try {
            database.transaction(() -> {
              final Iterator<?> it = walk.open(vertex(a), type);
              RID target = farEnd(it.next());
              if (target.equals(targets[2]))
                target = farEnd(it.next());
              it.remove();
              removed[0] = target;
              // THE LISTENER CHANGED THE ONE SEGMENT THE ITERATOR STANDS ON: ITS VIEW IS STALE, AND IT REFUSES TO GO ON
              assertThat(catchThrowable(it::hasNext)).isInstanceOf(ConcurrentModificationException.class);
              assertThat(catchThrowable(it::next)).isInstanceOf(ConcurrentModificationException.class);
              assertThat(catchThrowable(it::remove)).isInstanceOf(ConcurrentModificationException.class);
            });
          } finally {
            database.getEvents().unregisterListener(listener);
          }

          // 3 edges, one removed by the iterator, then one more removed (cascade) or added (create) by the listener
          final long expected = cascade ? 1 : 3;
          assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, type)).as(type + " cascade=" + cascade).isEqualTo(expected);
          assertThat(countType(type)).isEqualTo(expected);
          assertThat(vertex(removed[0]).countEdges(Vertex.DIRECTION.IN, type)).isZero();
          assertThat(vertex(targets[2]).countEdges(Vertex.DIRECTION.IN, type)).isEqualTo(cascade ? 0 : 2);
          assertCheckIsClean();

          // CLEAN UP FOR THE NEXT ROUND
          database.transaction(() -> database.command("sql", "DELETE FROM " + type));
        }
    }

    /** A self-loop walked on its OUT side is disconnected from its IN side too: only the walked list is skipped. */
    @Test
    void removingASelfLoopClearsBothListsOfTheVertex() {
      for (final String type : new String[] { "LW", "HE" }) {
        database.transaction(() -> vertex(a).modify().newEdge(type, a));

        database.transaction(() -> {
          final Iterator<Edge> it = vertex(a).getEdges(Vertex.DIRECTION.OUT, type).iterator();
          it.next();
          it.remove();
        });

        assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, type)).isZero();
        assertThat(vertex(a).countEdges(Vertex.DIRECTION.IN, type)).isZero();
        assertThat(countType(type)).isZero();
      }
      assertCheckIsClean();
    }

    @ParameterizedTest
    @EnumSource(Walk.class)
    void aSecondRemoveWithoutNextIsRefusedByTheIteratorContract(final Walk walk) {
      connect("HE", 2);

      database.begin();
      final Iterator<?> it = walk.open(vertex(a), "HE");
      it.next();
      it.remove();
      assertThat(catchThrowable(it::remove)).isInstanceOf(IllegalStateException.class);
      database.commit();

      assertConnected("HE", 1);
    }

    /** An entry whose edge record is gone names no edge to delete: removing the entry is the repair, and nothing else is touched. */
    @Test
    void anEntryWhoseEdgeRecordIsGoneIsUnlinked() {
      connect("HE", 2);
      final RID[] ghost = new RID[1];
      database.transaction(() -> {
        ghost[0] = vertex(a).getEdges(Vertex.DIRECTION.OUT, "HE").iterator().next().getIdentity();
        // THE RECORD ONLY, BYPASSING THE GRAPH: THE TWO ENTRIES POINTING AT IT ARE LEFT DANGLING
        ((LocalBucket) database.getSchema().getBucketById(ghost[0].getBucketId())).deleteRecord(ghost[0]);
      });

      database.transaction(() -> {
        final Iterator<Pair<RID, RID>> it = Walk.outList(vertex(a)).entryIterator();
        while (it.hasNext())
          if (it.next().getFirst().equals(ghost[0]))
            it.remove();
      });

      assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, "HE")).isEqualTo(1);
      assertThat(countType("HE")).isEqualTo(1);
    }

    // ---------------------------------------------------------------- helpers

    private void twoConsecutiveRemovesDeleteExactlyThoseTwo(final Walk walk, final String type) {
      connect(type, 3);

      final Set<RID> removed = new HashSet<>();
      database.transaction(() -> {
        final Iterator<?> it = walk.open(vertex(a), type);
        removed.add(farEnd(it.next()));
        it.remove();
        removed.add(farEnd(it.next()));
        it.remove();
      });

      assertThat(removed).hasSize(2);
      assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, type)).isEqualTo(1);
      assertThat(countType(type)).isEqualTo(1);
      for (final RID target : targets)
        assertThat(vertex(target).countEdges(Vertex.DIRECTION.IN, type)).isEqualTo(removed.contains(target) ? 0 : 1);
      assertCheckIsClean();
    }

    /** Through the CURRENT database handle, so a reopen does not leave the test reading through the closed one. */
    private Vertex vertex(final RID rid) {
      return database.lookupByRID(rid, false).asVertex();
    }

    /** The vertex at the far end of whatever an iterator hands out. */
    private RID farEnd(final Object element) {
      if (element instanceof Edge edge)
        return edge.getIn();
      if (element instanceof Vertex vertex)
        return vertex.getIdentity();
      return ((Pair<?, RID>) element).getSecond();
    }

    private void connect(final String type, final int count) {
      database.transaction(() -> {
        for (int i = 0; i < count; i++)
          vertex(a).modify().newEdge(type, targets[i]);
      });
      assertConnected(type, count);
    }

    private void assertConnected(final String type, final long expected) {
      assertThat(countType(type)).isEqualTo(expected);
      assertThat(vertex(a).countEdges(Vertex.DIRECTION.OUT, type)).isEqualTo(expected);
      long in = 0;
      for (final RID target : targets)
        in += vertex(target).countEdges(Vertex.DIRECTION.IN, type);
      assertThat(in).isEqualTo(expected);
    }

    private long countType(final String type) {
      try (final ResultSet rs = database.query("sql", "SELECT count() AS c FROM " + type)) {
        return ((Number) rs.next().getProperty("c")).longValue();
      }
    }

    private void assertCheckIsClean() {
      final Map<String, Object> result = new DatabaseChecker(database).setVerboseLevel(0).check();
      assertThat((Long) result.get("totalWarnings")).isZero();
      assertThat((Long) result.get("totalCorruptedRecords")).isZero();
    }

    private void unbindUser() {
      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(null);
    }

    /** Same resolution as ServerSecurityDatabaseUser: only the type's involved bucket ids are listed, the rest is default-allow. */
    private void bindUserRefusing(final SecurityDatabaseUser.ACCESS refused, final String typeName) {
      final DocumentType type = database.getSchema().getType(typeName);
      final Set<Integer> gated = new HashSet<>();
      for (final Bucket bucket : type.getInvolvedBuckets())
        gated.add(bucket.getFileId());
      assertThat(gated).isNotEmpty();

      DatabaseContext.INSTANCE.getContext(database.getDatabasePath()).setCurrentUser(new SecurityDatabaseUser() {
        @Override
        public String getName() {
          return "restricted";
        }

        @Override
        public boolean requestAccessOnDatabase(final DATABASE_ACCESS access) {
          return true;
        }

        @Override
        public boolean requestAccessOnFile(final int fileId, final ACCESS access) {
          return access != refused || !gated.contains(fileId);
        }

        @Override
        public boolean requestAccessOnType(final String name, final ACCESS access) {
          return access != refused || !typeName.equals(name);
        }

        @Override
        public long getResultSetLimit() {
          return -1L;
        }

        @Override
        public long getReadTimeout() {
          return -1L;
        }
      });
    }
  }

  /**
   * Issue #9839: a lightweight edge read back from the graph ({@link ImmutableLightEdge}) answered {@code toMap(true)} with
   * {@code {}}, while its write-path twin ({@link MutableLightEdge}) and every record-backed edge returned the metadata. The
   * two classes also disagreed on {@code toJSON}: the write-path one ignored the flag and omitted {@code @rid}. Every map and
   * JSON form of a lightweight edge must now carry {@code @cat}, {@code @type}, {@code @rid}, {@code @in} and {@code @out}
   * exactly when metadata is asked for, whichever class answers.
   */
  @Nested
  class LightEdgeMetadataTest extends TestHelper {
    private static final Set<String> METADATA = Set.of(Property.CAT_PROPERTY, Property.TYPE_PROPERTY, Property.RID_PROPERTY,
        Property.IN_PROPERTY, Property.OUT_PROPERTY);

    private RID v1;
    private RID v2;

    @Override
    protected void beginTest() {
      database.transaction(() -> {
        database.getSchema().buildVertexType().withName("V1").create();
        database.getSchema().buildEdgeType().withName("L1").withLightweight(true).create();
        database.getSchema().buildEdgeType().withName("R1").create();
      });
      database.transaction(() -> {
        final MutableVertex a = database.newVertex("V1").set("n", 1).save();
        final MutableVertex b = database.newVertex("V1").set("n", 2).save();
        a.newEdge("L1", b);
        a.newEdge("R1", b).set("w", 7).save();
        v1 = a.getIdentity();
        v2 = b.getIdentity();
      });
    }

    @Test
    void aLightEdgeReadFromTheGraphMapsItsMetadata() {
      database.transaction(() -> {
        final Edge edge = readLightEdge();
        assertThat(edge).isInstanceOf(ImmutableLightEdge.class);
        assertMetadataMap(edge, edge.toMap(true));
        assertMetadataMap(edge, edge.toMap());
        assertThat(edge.toMap(false)).isEmpty();
      });
    }

    @Test
    void aLightEdgeOnTheWritePathMapsTheSameMetadata() {
      database.transaction(() -> {
        final Edge edge = database.lookupByRID(v1, true).asVertex().modify().newEdge("L1", v2);
        assertThat(edge).isInstanceOf(MutableLightEdge.class);
        assertMetadataMap(edge, edge.toMap(true));
        assertThat(edge.toMap(false)).isEmpty();
      });
    }

    @Test
    void bothLightEdgeClassesRenderTheSameJson() {
      database.transaction(() -> {
        final Edge read = readLightEdge();
        final Edge written = database.lookupByRID(v1, true).asVertex().modify().newEdge("L1", v2);
        assertThat(written).isInstanceOf(MutableLightEdge.class);

        for (final Edge edge : List.of(read, written)) {
          final JSONObject json = edge.toJSON(true);
          assertThat(json.keySet()).as(edge.getClass().getSimpleName() + ".toJSON(true)").containsExactlyInAnyOrderElementsOf(METADATA);
          assertThat(json.getString(Property.CAT_PROPERTY)).isEqualTo("e");
          assertThat(json.getString(Property.TYPE_PROPERTY)).isEqualTo("L1");
          assertThat(json.getString(Property.RID_PROPERTY)).isEqualTo(edge.getIdentity().toString());
          assertThat(json.getString(Property.OUT_PROPERTY)).isEqualTo(v1.toString());
          assertThat(json.getString(Property.IN_PROPERTY)).isEqualTo(v2.toString());

          assertThat(edge.toJSON(false).keySet()).as(edge.getClass().getSimpleName() + ".toJSON(false)").isEmpty();
        }
      });
    }

    @Test
    void aLightEdgeMapsTheSameMetadataKeysAsARecordBackedEdge() {
      database.transaction(() -> {
        final Edge regular = database.lookupByRID(v1, true).asVertex().getEdges(Vertex.DIRECTION.OUT, "R1").iterator().next();
        final Map<String, Object> regularMap = regular.toMap(true);
        regularMap.remove("w");
        assertThat(readLightEdge().toMap(true).keySet()).containsExactlyInAnyOrderElementsOf(regularMap.keySet());
      });
    }

    @Test
    void sqlIncludeReadsTheMetadataOfALightEdge() {
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql",
            "select outE('L1').include('@rid','@type','@cat','@in','@out') as e from V1 where n = 1")) {
          final List<Map<String, Object>> rows = rs.next().getProperty("e");
          assertThat(rows).hasSize(1);
          final Map<String, Object> row = rows.getFirst();
          assertThat(row.get(Property.RID_PROPERTY)).isEqualTo(readLightEdge().getIdentity().toString());
          assertThat(row.get(Property.TYPE_PROPERTY)).isEqualTo("L1");
          assertThat(row.get(Property.CAT_PROPERTY)).isEqualTo("e");
          assertThat(row.get(Property.OUT_PROPERTY)).hasToString(v1.toString());
          assertThat(row.get(Property.IN_PROPERTY)).hasToString(v2.toString());
        }
      });
    }

    @Test
    void sqlExcludeKeepsTheOtherMetadataOfALightEdge() {
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "select outE('L1').exclude('@rid') as e from V1 where n = 1")) {
          final List<Map<String, Object>> rows = rs.next().getProperty("e");
          assertThat(rows).hasSize(1);
          assertThat(rows.getFirst().keySet()).containsExactlyInAnyOrder(Property.CAT_PROPERTY, Property.TYPE_PROPERTY,
              Property.IN_PROPERTY, Property.OUT_PROPERTY);
        }
      });
    }

    @Test
    void aResultHoldingALightEdgeMapsItsMetadata() {
      database.transaction(() -> {
        try (final ResultSet rs = database.query("sql", "select expand(outE('L1')) from V1 where n = 1")) {
          final Result result = rs.next();
          assertThat(result.isElement()).isTrue();
          assertThat(result.toMap().keySet()).containsExactlyInAnyOrderElementsOf(METADATA);
          assertThat(result.toMap().get(Property.RID_PROPERTY)).isEqualTo(readLightEdge().getIdentity().toString());
        }
      });
    }

    private Edge readLightEdge() {
      return database.lookupByRID(v1, true).asVertex().getEdges(Vertex.DIRECTION.OUT, "L1").iterator().next();
    }

    private void assertMetadataMap(final Edge edge, final Map<String, Object> map) {
      assertThat(map.keySet()).as(edge.getClass().getSimpleName()).containsExactlyInAnyOrderElementsOf(METADATA);
      assertThat(map.get(Property.CAT_PROPERTY)).isEqualTo("e");
      assertThat(map.get(Property.TYPE_PROPERTY)).isEqualTo("L1");
      assertThat(map.get(Property.RID_PROPERTY)).isEqualTo(edge.getIdentity().toString());
      assertThat(map.get(Property.OUT_PROPERTY)).isEqualTo(v1);
      assertThat(map.get(Property.IN_PROPERTY)).isEqualTo(v2);
    }
  }
}
